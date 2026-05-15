import os
import json
import re
from urllib.parse import quote_plus

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# ==================================================
# 1. .env 불러오기
# ==================================================
load_dotenv()


def get_required_env(key: str) -> str:
    value = os.getenv(key)

    if value is None or value.strip() == "":
        raise ValueError(f".env 파일에 {key} 값이 없습니다.")

    return value


DB_USER = get_required_env("DB_USER")
DB_PASSWORD = quote_plus(get_required_env("DB_PASSWORD"))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = get_required_env("DB_NAME")


engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
)


# ==================================================
# 2. 설정
# ==================================================
TARGET_HOUR = 6

RAIN_WINDOWS = [1, 3, 6, 12, 24, 48, 72]
INFLOW_LAGS = [1, 3, 6, 12, 24]
INFLOW_ROLLING_WINDOWS = [3, 6, 12, 24]

# 댐별 유입량 상한 기준
# 0.999 = 상위 0.1% 이상을 상한 처리
INFLOW_CAP_QUANTILE = 0.999

OUTPUT_DIR = "model_results_v3_cleaned_by_dam"

METRIC_PATH = os.path.join(
    OUTPUT_DIR,
    f"inflow_after_{TARGET_HOUR}h_metrics_by_dam_v3.csv"
)

PREDICTION_SAMPLE_PATH = os.path.join(
    OUTPUT_DIR,
    f"inflow_after_{TARGET_HOUR}h_prediction_sample_v3.csv"
)

CAP_INFO_PATH = os.path.join(
    OUTPUT_DIR,
    f"inflow_cap_info_v3.csv"
)

SUMMARY_PATH = os.path.join(
    OUTPUT_DIR,
    f"inflow_after_{TARGET_HOUR}h_summary_v3.json"
)


# ==================================================
# 3. 유틸 함수
# ==================================================
def safe_filename(name: str) -> str:
    name = str(name)
    name = re.sub(r"[\\/:*?\"<>|()\s]+", "_", name)
    return name.strip("_")


def calculate_rmse(y_true, y_pred):
    try:
        return mean_squared_error(y_true, y_pred, squared=False)
    except TypeError:
        return np.sqrt(mean_squared_error(y_true, y_pred))


# ==================================================
# 4. DB에서 데이터 불러오기
# ==================================================
def load_data() -> pd.DataFrame:
    query = """
        SELECT
            dam_code,
            dam_name,
            observed_at,

            inflow,
            water_level,
            hydrology_rainfall,
            storage_amount,
            storage_rate,
            discharge,

            temperature,
            kma_rainfall,
            snow
        FROM final_historical_data
        ORDER BY dam_name, observed_at
    """

    print("[LOAD] final_historical_data 조회 시작")

    df = pd.read_sql(query, engine)

    print(f"[LOAD] 조회 완료: {len(df):,}건")
    print(f"[LOAD] 댐 개수: {df['dam_name'].nunique()}개")
    print(f"[LOAD] 기간: {df['observed_at'].min()} ~ {df['observed_at'].max()}")

    return df


# ==================================================
# 5. 기본 전처리
# ==================================================
def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    print("[PREPROCESS] 전처리 시작")

    df = df.copy()

    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")

    numeric_cols = [
        "inflow",
        "water_level",
        "hydrology_rainfall",
        "storage_amount",
        "storage_rate",
        "discharge",
        "temperature",
        "kma_rainfall",
        "snow",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["dam_name", "observed_at"])

    df["hydrology_rainfall"] = df["hydrology_rainfall"].fillna(0)
    df["kma_rainfall"] = df["kma_rainfall"].fillna(0)
    df["snow"] = df["snow"].fillna(0)

    df = df.sort_values(["dam_name", "observed_at"]).reset_index(drop=True)

    print(f"[PREPROCESS] 완료: {len(df):,}건")

    return df


# ==================================================
# 6. 댐별 inflow 이상치 상한값 계산
# ==================================================
def calculate_inflow_caps(df: pd.DataFrame) -> pd.DataFrame:
    cap_rows = []

    for dam_name, dam_df in df.groupby("dam_name"):
        inflow = dam_df["inflow"].dropna()

        if inflow.empty:
            cap_value = np.nan
            max_value = np.nan
            p99 = np.nan
            p999 = np.nan
        else:
            cap_value = inflow.quantile(INFLOW_CAP_QUANTILE)
            max_value = inflow.max()
            p99 = inflow.quantile(0.99)
            p999 = inflow.quantile(0.999)

        cap_rows.append({
            "dam_name": dam_name,
            "cap_quantile": INFLOW_CAP_QUANTILE,
            "inflow_cap": cap_value,
            "inflow_max": max_value,
            "inflow_p99": p99,
            "inflow_p999": p999,
            "max_to_cap_ratio": max_value / cap_value if cap_value and cap_value > 0 else np.nan,
        })

    cap_df = pd.DataFrame(cap_rows)

    return cap_df


# ==================================================
# 7. 댐별 데이터에 clipping 적용
# ==================================================
def apply_inflow_cap(dam_df: pd.DataFrame, cap_value: float) -> pd.DataFrame:
    df = dam_df.copy()

    df["inflow_original"] = df["inflow"]

    if pd.notna(cap_value) and cap_value > 0:
        df["inflow_cleaned"] = df["inflow"].clip(lower=0, upper=cap_value)
    else:
        df["inflow_cleaned"] = df["inflow"].clip(lower=0)

    return df


# ==================================================
# 8. 댐별 feature 생성
# ==================================================
def create_features_for_one_dam(dam_df: pd.DataFrame) -> pd.DataFrame:
    df = dam_df.copy()
    df = df.sort_values("observed_at").reset_index(drop=True)

    # 시간 특징
    df["hour"] = df["observed_at"].dt.hour
    df["month"] = df["observed_at"].dt.month
    df["dayofyear"] = df["observed_at"].dt.dayofyear

    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    # 강수 누적
    for rain_col in ["hydrology_rainfall", "kma_rainfall"]:
        for window in RAIN_WINDOWS:
            df[f"{rain_col}_{window}h_sum"] = (
                df[rain_col]
                .rolling(window=window, min_periods=1)
                .sum()
            )

    # inflow는 cleaned 기준으로 lag 생성
    for lag in INFLOW_LAGS:
        df[f"inflow_lag_{lag}h"] = df["inflow_cleaned"].shift(lag)

    for window in INFLOW_ROLLING_WINDOWS:
        df[f"inflow_{window}h_mean"] = (
            df["inflow_cleaned"]
            .rolling(window=window, min_periods=1)
            .mean()
        )

        df[f"inflow_{window}h_max"] = (
            df["inflow_cleaned"]
            .rolling(window=window, min_periods=1)
            .max()
        )

    # 변화량
    df["inflow_diff_1h"] = df["inflow_cleaned"].diff(1)
    df["water_level_diff_1h"] = df["water_level"].diff(1)
    df["storage_rate_diff_1h"] = df["storage_rate"].diff(1)
    df["discharge_diff_1h"] = df["discharge"].diff(1)

    # target도 cleaned 기준
    target_col = f"inflow_after_{TARGET_HOUR}h"
    df[target_col] = df["inflow_cleaned"].shift(-TARGET_HOUR)

    # 원본 target도 비교용으로 저장
    df[f"inflow_original_after_{TARGET_HOUR}h"] = df["inflow_original"].shift(-TARGET_HOUR)

    return df


# ==================================================
# 9. feature 컬럼 목록
# ==================================================
def get_feature_columns() -> list:
    feature_cols = [
        "inflow_cleaned",
        "water_level",
        "hydrology_rainfall",
        "storage_amount",
        "storage_rate",
        "discharge",
        "temperature",
        "kma_rainfall",
        "snow",

        "hour",
        "month",
        "dayofyear",
        "hour_sin",
        "hour_cos",
        "month_sin",
        "month_cos",

        "inflow_diff_1h",
        "water_level_diff_1h",
        "storage_rate_diff_1h",
        "discharge_diff_1h",
    ]

    for rain_col in ["hydrology_rainfall", "kma_rainfall"]:
        for window in RAIN_WINDOWS:
            feature_cols.append(f"{rain_col}_{window}h_sum")

    for lag in INFLOW_LAGS:
        feature_cols.append(f"inflow_lag_{lag}h")

    for window in INFLOW_ROLLING_WINDOWS:
        feature_cols.append(f"inflow_{window}h_mean")
        feature_cols.append(f"inflow_{window}h_max")

    return feature_cols


# ==================================================
# 10. 댐 하나에 대한 모델 학습
# ==================================================
def train_one_dam_model(dam_name: str, dam_df: pd.DataFrame, cap_value: float):
    print("\n" + "=" * 80)
    print(f"[DAM START] {dam_name}")
    print("=" * 80)

    dam_df = apply_inflow_cap(dam_df, cap_value)

    df = create_features_for_one_dam(dam_df)

    target_col = f"inflow_after_{TARGET_HOUR}h"
    original_target_col = f"inflow_original_after_{TARGET_HOUR}h"

    feature_cols = get_feature_columns()

    df = df.dropna(subset=[target_col])
    df = df.dropna(subset=["inflow_cleaned"])

    if len(df) < 1000:
        print(f"[SKIP] {dam_name}: 데이터가 너무 적습니다. {len(df)}건")
        return None, None, None

    unique_times = sorted(df["observed_at"].dropna().unique())

    split_index = int(len(unique_times) * 0.8)
    split_time = unique_times[split_index]

    train_df = df[df["observed_at"] < split_time].copy()
    test_df = df[df["observed_at"] >= split_time].copy()

    X_train = train_df[feature_cols]
    y_train_raw = train_df[target_col]

    X_test = test_df[feature_cols]
    y_test_raw = test_df[target_col]

    original_y_test = test_df[original_target_col]

    medians = X_train.median(numeric_only=True)

    X_train = X_train.fillna(medians)
    X_test = X_test.fillna(medians)

    y_train = np.log1p(y_train_raw.clip(lower=0))

    model = HistGradientBoostingRegressor(
        max_iter=600,
        learning_rate=0.035,
        max_leaf_nodes=31,
        l2_regularization=0.08,
        random_state=42
    )

    model.fit(X_train, y_train)

    y_pred_log = model.predict(X_test)
    y_pred = np.expm1(y_pred_log)
    y_pred = np.clip(y_pred, 0, None)

    y_test = y_test_raw.values

    mae = mean_absolute_error(y_test, y_pred)
    rmse = calculate_rmse(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # 원본 target 기준 MAE도 참고용 계산
    original_mae = mean_absolute_error(original_y_test, y_pred)
    original_rmse = calculate_rmse(original_y_test, y_pred)

    peak_threshold = np.nanpercentile(y_test, 90)
    peak_mask = y_test >= peak_threshold

    if peak_mask.sum() > 0:
        peak_mae = mean_absolute_error(y_test[peak_mask], y_pred[peak_mask])
        peak_rmse = calculate_rmse(y_test[peak_mask], y_pred[peak_mask])
        peak_count = int(peak_mask.sum())
    else:
        peak_mae = np.nan
        peak_rmse = np.nan
        peak_count = 0

    metric = {
        "dam_name": dam_name,
        "target_hour": TARGET_HOUR,
        "cap_quantile": INFLOW_CAP_QUANTILE,
        "inflow_cap": cap_value,
        "train_count": len(train_df),
        "test_count": len(test_df),
        "split_time": str(split_time),

        "mae_cleaned_target": mae,
        "rmse_cleaned_target": rmse,
        "r2_cleaned_target": r2,

        "mae_original_target": original_mae,
        "rmse_original_target": original_rmse,

        "actual_mean_cleaned": float(np.nanmean(y_test)),
        "predicted_mean": float(np.nanmean(y_pred)),
        "actual_max_cleaned": float(np.nanmax(y_test)),
        "actual_max_original": float(np.nanmax(original_y_test)),
        "predicted_max": float(np.nanmax(y_pred)),

        "peak_threshold_90p": float(peak_threshold),
        "peak_count": peak_count,
        "peak_mae": peak_mae,
        "peak_rmse": peak_rmse,
    }

    prediction_df = test_df[["dam_name", "observed_at"]].copy()
    prediction_df["actual_inflow_cleaned"] = y_test
    prediction_df["actual_inflow_original"] = original_y_test.values
    prediction_df["predicted_inflow"] = y_pred
    prediction_df["error_cleaned"] = prediction_df["predicted_inflow"] - prediction_df["actual_inflow_cleaned"]
    prediction_df["abs_error_cleaned"] = prediction_df["error_cleaned"].abs()

    package = {
        "model": model,
        "dam_name": dam_name,
        "feature_cols": feature_cols,
        "medians": medians,
        "target_hour": TARGET_HOUR,
        "split_time": str(split_time),
        "rain_windows": RAIN_WINDOWS,
        "inflow_lags": INFLOW_LAGS,
        "inflow_rolling_windows": INFLOW_ROLLING_WINDOWS,
        "uses_log_target": True,
        "uses_inflow_cap": True,
        "inflow_cap_quantile": INFLOW_CAP_QUANTILE,
        "inflow_cap": cap_value,
    }

    print(f"[DAM RESULT] {dam_name}")
    print(f"CAP: {cap_value:.4f}")
    print(f"MAE cleaned: {mae:.4f}")
    print(f"RMSE cleaned: {rmse:.4f}")
    print(f"R2 cleaned: {r2:.4f}")
    print(f"MAE original: {original_mae:.4f}")
    print(f"RMSE original: {original_rmse:.4f}")
    print(f"Peak MAE: {peak_mae:.4f}")
    print(f"Actual max cleaned: {metric['actual_max_cleaned']:.4f}")
    print(f"Actual max original: {metric['actual_max_original']:.4f}")
    print(f"Pred max: {metric['predicted_max']:.4f}")

    return package, metric, prediction_df


# ==================================================
# 11. 결과 저장
# ==================================================
def save_one_dam_model(package: dict):
    dam_name = package["dam_name"]

    file_name = safe_filename(dam_name)

    model_path = os.path.join(
        OUTPUT_DIR,
        f"{file_name}_inflow_after_{TARGET_HOUR}h_model_v3.pkl"
    )

    joblib.dump(package, model_path)

    print(f"[SAVE MODEL] {dam_name}: {model_path}")


def save_all_results(metrics: list, prediction_samples: list, cap_df: pd.DataFrame):
    metrics_df = pd.DataFrame(metrics)

    if not metrics_df.empty:
        metrics_df = metrics_df.sort_values("mae_cleaned_target").reset_index(drop=True)

    metrics_df.to_csv(METRIC_PATH, index=False, encoding="utf-8-sig")

    cap_df.to_csv(CAP_INFO_PATH, index=False, encoding="utf-8-sig")

    if prediction_samples:
        prediction_df = pd.concat(prediction_samples, ignore_index=True)
        prediction_df.to_csv(PREDICTION_SAMPLE_PATH, index=False, encoding="utf-8-sig")

    summary = {
        "target_hour": TARGET_HOUR,
        "dam_count": len(metrics_df),
        "inflow_cap_quantile": INFLOW_CAP_QUANTILE,
        "mean_mae_cleaned": float(metrics_df["mae_cleaned_target"].mean()) if not metrics_df.empty else None,
        "mean_rmse_cleaned": float(metrics_df["rmse_cleaned_target"].mean()) if not metrics_df.empty else None,
        "mean_r2_cleaned": float(metrics_df["r2_cleaned_target"].mean()) if not metrics_df.empty else None,
        "median_mae_cleaned": float(metrics_df["mae_cleaned_target"].median()) if not metrics_df.empty else None,
        "median_r2_cleaned": float(metrics_df["r2_cleaned_target"].median()) if not metrics_df.empty else None,
    }

    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 100)
    print("[V3 전체 요약]")
    print("=" * 100)

    if not metrics_df.empty:
        print(metrics_df[[
            "dam_name",
            "test_count",
            "inflow_cap",
            "mae_cleaned_target",
            "rmse_cleaned_target",
            "r2_cleaned_target",
            "mae_original_target",
            "actual_max_cleaned",
            "actual_max_original",
            "predicted_max",
        ]].to_string(index=False))

    print("\n[SAVE] 댐별 성능:", METRIC_PATH)
    print("[SAVE] 예측 샘플:", PREDICTION_SAMPLE_PATH)
    print("[SAVE] 상한값 정보:", CAP_INFO_PATH)
    print("[SAVE] 요약:", SUMMARY_PATH)


# ==================================================
# 12. 전체 실행
# ==================================================
def main():
    print("[START] V3 이상치 상한 처리 기반 댐별 유입량 예측 모델 학습 시작")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = load_data()
    df = preprocess(df)

    cap_df = calculate_inflow_caps(df)

    print("\n[CAP INFO]")
    print(cap_df.sort_values("max_to_cap_ratio", ascending=False).to_string(index=False))

    metrics = []
    prediction_samples = []

    dam_names = sorted(df["dam_name"].dropna().unique())

    for dam_name in dam_names:
        dam_df = df[df["dam_name"] == dam_name].copy()

        cap_row = cap_df[cap_df["dam_name"] == dam_name]

        if cap_row.empty:
            cap_value = np.nan
        else:
            cap_value = cap_row["inflow_cap"].iloc[0]

        package, metric, prediction_df = train_one_dam_model(dam_name, dam_df, cap_value)

        if package is None:
            continue

        save_one_dam_model(package)

        metrics.append(metric)

        prediction_samples.append(prediction_df.head(300))

    save_all_results(metrics, prediction_samples, cap_df)

    print("[END] V3 이상치 상한 처리 기반 댐별 유입량 예측 모델 학습 종료")


if __name__ == "__main__":
    main()