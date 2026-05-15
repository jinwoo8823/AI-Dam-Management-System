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

OUTPUT_DIR = "model_results_v2_by_dam"

METRIC_PATH = os.path.join(OUTPUT_DIR, f"inflow_after_{TARGET_HOUR}h_metrics_by_dam_v2.csv")
PREDICTION_SAMPLE_PATH = os.path.join(OUTPUT_DIR, f"inflow_after_{TARGET_HOUR}h_prediction_sample_v2.csv")
SUMMARY_PATH = os.path.join(OUTPUT_DIR, f"inflow_after_{TARGET_HOUR}h_summary_v2.json")


# ==================================================
# 3. 유틸 함수
# ==================================================
def safe_filename(name: str) -> str:
    """
    파일명으로 쓰기 어려운 문자를 제거한다.
    """
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

    # 강수량 결측치는 0으로 처리
    df["hydrology_rainfall"] = df["hydrology_rainfall"].fillna(0)
    df["kma_rainfall"] = df["kma_rainfall"].fillna(0)
    df["snow"] = df["snow"].fillna(0)

    df = df.sort_values(["dam_name", "observed_at"]).reset_index(drop=True)

    print(f"[PREPROCESS] 완료: {len(df):,}건")

    return df


# ==================================================
# 6. 댐별 feature 생성
# ==================================================
def create_features_for_one_dam(dam_df: pd.DataFrame) -> pd.DataFrame:
    df = dam_df.copy()
    df = df.sort_values("observed_at").reset_index(drop=True)

    # 시간 특징
    df["hour"] = df["observed_at"].dt.hour
    df["month"] = df["observed_at"].dt.month
    df["dayofyear"] = df["observed_at"].dt.dayofyear

    # 계절성 sin/cos
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

    # 유입량 lag
    for lag in INFLOW_LAGS:
        df[f"inflow_lag_{lag}h"] = df["inflow"].shift(lag)

    # 유입량 rolling mean / max
    for window in INFLOW_ROLLING_WINDOWS:
        df[f"inflow_{window}h_mean"] = (
            df["inflow"]
            .rolling(window=window, min_periods=1)
            .mean()
        )

        df[f"inflow_{window}h_max"] = (
            df["inflow"]
            .rolling(window=window, min_periods=1)
            .max()
        )

    # 변화량
    df["inflow_diff_1h"] = df["inflow"].diff(1)
    df["water_level_diff_1h"] = df["water_level"].diff(1)
    df["storage_rate_diff_1h"] = df["storage_rate"].diff(1)
    df["discharge_diff_1h"] = df["discharge"].diff(1)

    # target
    target_col = f"inflow_after_{TARGET_HOUR}h"
    df[target_col] = df["inflow"].shift(-TARGET_HOUR)

    return df


# ==================================================
# 7. feature 컬럼 목록
# ==================================================
def get_feature_columns() -> list:
    feature_cols = [
        "inflow",
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
# 8. 댐 하나에 대한 모델 학습
# ==================================================
def train_one_dam_model(dam_name: str, dam_df: pd.DataFrame):
    print("\n" + "=" * 80)
    print(f"[DAM START] {dam_name}")
    print("=" * 80)

    df = create_features_for_one_dam(dam_df)

    target_col = f"inflow_after_{TARGET_HOUR}h"
    feature_cols = get_feature_columns()

    # target 없는 마지막 구간 제거
    df = df.dropna(subset=[target_col])

    # 현재 inflow가 없는 행 제거
    df = df.dropna(subset=["inflow"])

    if len(df) < 1000:
        print(f"[SKIP] {dam_name}: 데이터가 너무 적습니다. {len(df)}건")
        return None, None, None

    # 시간 기준 train/test 분리
    unique_times = sorted(df["observed_at"].dropna().unique())
    split_index = int(len(unique_times) * 0.8)
    split_time = unique_times[split_index]

    train_df = df[df["observed_at"] < split_time].copy()
    test_df = df[df["observed_at"] >= split_time].copy()

    X_train = train_df[feature_cols]
    y_train_raw = train_df[target_col]

    X_test = test_df[feature_cols]
    y_test_raw = test_df[target_col]

    # 결측치 처리
    medians = X_train.median(numeric_only=True)

    X_train = X_train.fillna(medians)
    X_test = X_test.fillna(medians)

    # 유입량은 음수가 나오면 모델 안정성을 위해 0으로 클리핑 후 log1p
    y_train = np.log1p(y_train_raw.clip(lower=0))

    model = HistGradientBoostingRegressor(
        max_iter=500,
        learning_rate=0.04,
        max_leaf_nodes=31,
        l2_regularization=0.05,
        random_state=42
    )

    model.fit(X_train, y_train)

    # 예측값 원복
    y_pred_log = model.predict(X_test)
    y_pred = np.expm1(y_pred_log)
    y_pred = np.clip(y_pred, 0, None)

    y_test = y_test_raw.values

    mae = mean_absolute_error(y_test, y_pred)
    rmse = calculate_rmse(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # 피크 구간 평가: 실제 유입량 상위 10%
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
        "train_count": len(train_df),
        "test_count": len(test_df),
        "split_time": str(split_time),
        "mae": mae,
        "rmse": rmse,
        "r2": r2,
        "actual_mean": float(np.nanmean(y_test)),
        "predicted_mean": float(np.nanmean(y_pred)),
        "actual_max": float(np.nanmax(y_test)),
        "predicted_max": float(np.nanmax(y_pred)),
        "peak_threshold_90p": float(peak_threshold),
        "peak_count": peak_count,
        "peak_mae": peak_mae,
        "peak_rmse": peak_rmse,
    }

    prediction_df = test_df[["dam_name", "observed_at"]].copy()
    prediction_df["actual_inflow"] = y_test
    prediction_df["predicted_inflow"] = y_pred
    prediction_df["error"] = prediction_df["predicted_inflow"] - prediction_df["actual_inflow"]
    prediction_df["abs_error"] = prediction_df["error"].abs()

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
    }

    print(f"[DAM RESULT] {dam_name}")
    print(f"MAE: {mae:.4f}")
    print(f"RMSE: {rmse:.4f}")
    print(f"R2: {r2:.4f}")
    print(f"Peak MAE: {peak_mae:.4f}")
    print(f"Actual mean: {metric['actual_mean']:.4f}")
    print(f"Pred mean: {metric['predicted_mean']:.4f}")
    print(f"Actual max: {metric['actual_max']:.4f}")
    print(f"Pred max: {metric['predicted_max']:.4f}")

    return package, metric, prediction_df


# ==================================================
# 9. 결과 저장
# ==================================================
def save_one_dam_model(package: dict):
    dam_name = package["dam_name"]
    file_name = safe_filename(dam_name)
    model_path = os.path.join(OUTPUT_DIR, f"{file_name}_inflow_after_{TARGET_HOUR}h_model.pkl")

    joblib.dump(package, model_path)

    print(f"[SAVE MODEL] {dam_name}: {model_path}")


def save_all_results(metrics: list, prediction_samples: list):
    metrics_df = pd.DataFrame(metrics)

    metrics_df = metrics_df.sort_values("mae").reset_index(drop=True)

    metrics_df.to_csv(METRIC_PATH, index=False, encoding="utf-8-sig")

    if prediction_samples:
        prediction_df = pd.concat(prediction_samples, ignore_index=True)
        prediction_df.to_csv(PREDICTION_SAMPLE_PATH, index=False, encoding="utf-8-sig")

    summary = {
        "target_hour": TARGET_HOUR,
        "dam_count": len(metrics_df),
        "mean_mae": float(metrics_df["mae"].mean()) if not metrics_df.empty else None,
        "mean_rmse": float(metrics_df["rmse"].mean()) if not metrics_df.empty else None,
        "mean_r2": float(metrics_df["r2"].mean()) if not metrics_df.empty else None,
        "median_mae": float(metrics_df["mae"].median()) if not metrics_df.empty else None,
        "median_r2": float(metrics_df["r2"].median()) if not metrics_df.empty else None,
    }

    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 80)
    print("[V2 전체 요약]")
    print("=" * 80)
    print(metrics_df[[
        "dam_name",
        "test_count",
        "mae",
        "rmse",
        "r2",
        "peak_mae",
        "actual_mean",
        "predicted_mean",
        "actual_max",
        "predicted_max",
    ]].to_string(index=False))

    print("\n[SAVE] 댐별 성능:", METRIC_PATH)
    print("[SAVE] 예측 샘플:", PREDICTION_SAMPLE_PATH)
    print("[SAVE] 요약:", SUMMARY_PATH)


# ==================================================
# 10. 전체 실행
# ==================================================
def main():
    print("[START] V2 댐별 유입량 예측 모델 학습 시작")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = load_data()
    df = preprocess(df)

    metrics = []
    prediction_samples = []

    dam_names = sorted(df["dam_name"].dropna().unique())

    for dam_name in dam_names:
        dam_df = df[df["dam_name"] == dam_name].copy()

        package, metric, prediction_df = train_one_dam_model(dam_name, dam_df)

        if package is None:
            continue

        save_one_dam_model(package)

        metrics.append(metric)

        # 전체 예측을 다 저장하면 커질 수 있으므로 댐별 앞 300건만 샘플 저장
        prediction_samples.append(prediction_df.head(300))

    save_all_results(metrics, prediction_samples)

    print("[END] V2 댐별 유입량 예측 모델 학습 종료")


if __name__ == "__main__":
    main()