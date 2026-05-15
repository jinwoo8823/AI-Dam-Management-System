import os
import json
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

# 몇 시간 뒤 유입량을 예측할지
TARGET_HOUR = 6

# 누적 강수량 기준
RAIN_WINDOWS = [1, 3, 6, 12, 24, 48, 72]

# 결과 저장 폴더
OUTPUT_DIR = "model_results"

MODEL_PATH = os.path.join(OUTPUT_DIR, f"inflow_after_{TARGET_HOUR}h_model.pkl")
METRIC_PATH = os.path.join(OUTPUT_DIR, f"inflow_after_{TARGET_HOUR}h_metrics.csv")
DAM_METRIC_PATH = os.path.join(OUTPUT_DIR, f"inflow_after_{TARGET_HOUR}h_metrics_by_dam.csv")
FEATURE_INFO_PATH = os.path.join(OUTPUT_DIR, f"inflow_after_{TARGET_HOUR}h_feature_info.json")


# ==================================================
# 3. DB에서 데이터 불러오기
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
# 4. 전처리
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
# 5. 특징값 생성
# ==================================================
def create_features(df: pd.DataFrame) -> pd.DataFrame:
    print("[FEATURE] 특징값 생성 시작")

    df = df.copy()
    df = df.sort_values(["dam_name", "observed_at"]).reset_index(drop=True)

    # 시간 특징
    df["hour"] = df["observed_at"].dt.hour
    df["month"] = df["observed_at"].dt.month
    df["dayofyear"] = df["observed_at"].dt.dayofyear

    # 누적 강수량 특징
    rain_cols = ["hydrology_rainfall", "kma_rainfall"]

    for rain_col in rain_cols:
        for window in RAIN_WINDOWS:
            new_col = f"{rain_col}_{window}h_sum"

            df[new_col] = (
                df.groupby("dam_name")[rain_col]
                .transform(lambda x: x.rolling(window=window, min_periods=1).sum())
            )

            print(f"[FEATURE] 생성: {new_col}")

    # 최근 변화량 특징
    df["inflow_diff_1h"] = df.groupby("dam_name")["inflow"].diff(1)
    df["water_level_diff_1h"] = df.groupby("dam_name")["water_level"].diff(1)
    df["storage_rate_diff_1h"] = df.groupby("dam_name")["storage_rate"].diff(1)
    df["discharge_diff_1h"] = df.groupby("dam_name")["discharge"].diff(1)

    # 미래 유입량 target
    target_col = f"inflow_after_{TARGET_HOUR}h"

    df[target_col] = (
        df.groupby("dam_name")["inflow"]
        .shift(-TARGET_HOUR)
    )

    print(f"[TARGET] 생성: {target_col}")

    print("[FEATURE] 특징값 생성 완료")

    return df


# ==================================================
# 6. 학습 데이터 만들기
# ==================================================
def build_train_test_data(df: pd.DataFrame):
    print("[DATASET] 학습 데이터 생성 시작")

    target_col = f"inflow_after_{TARGET_HOUR}h"

    base_feature_cols = [
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

        "inflow_diff_1h",
        "water_level_diff_1h",
        "storage_rate_diff_1h",
        "discharge_diff_1h",
    ]

    rainfall_feature_cols = []

    for rain_col in ["hydrology_rainfall", "kma_rainfall"]:
        for window in RAIN_WINDOWS:
            rainfall_feature_cols.append(f"{rain_col}_{window}h_sum")

    feature_cols = base_feature_cols + rainfall_feature_cols

    model_df = df.copy()

    # target이 없는 마지막 구간 제거
    model_df = model_df.dropna(subset=[target_col])

    # 현재 유입량 자체가 없는 행 제거
    model_df = model_df.dropna(subset=["inflow"])

    # 댐 이름 one-hot encoding
    dam_dummies = pd.get_dummies(model_df["dam_name"], prefix="dam")

    model_df = pd.concat([model_df, dam_dummies], axis=1)

    dam_dummy_cols = list(dam_dummies.columns)

    feature_cols = feature_cols + dam_dummy_cols

    # 시간 기준 분리
    unique_times = sorted(model_df["observed_at"].dropna().unique())

    split_index = int(len(unique_times) * 0.8)
    split_time = unique_times[split_index]

    train_df = model_df[model_df["observed_at"] < split_time].copy()
    test_df = model_df[model_df["observed_at"] >= split_time].copy()

    X_train = train_df[feature_cols]
    y_train = train_df[target_col]

    X_test = test_df[feature_cols]
    y_test = test_df[target_col]

    # 결측치 처리: 학습 데이터 중앙값으로 채움
    medians = X_train.median(numeric_only=True)

    X_train = X_train.fillna(medians)
    X_test = X_test.fillna(medians)

    print(f"[DATASET] target: {target_col}")
    print(f"[DATASET] split_time: {split_time}")
    print(f"[DATASET] train: {len(X_train):,}건")
    print(f"[DATASET] test: {len(X_test):,}건")
    print(f"[DATASET] feature 개수: {len(feature_cols)}개")

    return X_train, X_test, y_train, y_test, train_df, test_df, feature_cols, medians, split_time


# ==================================================
# 7. 모델 학습
# ==================================================
def train_model(X_train, y_train):
    print("[TRAIN] 모델 학습 시작")

    model = HistGradientBoostingRegressor(
        max_iter=300,
        learning_rate=0.06,
        max_leaf_nodes=31,
        l2_regularization=0.1,
        random_state=42
    )

    model.fit(X_train, y_train)

    print("[TRAIN] 모델 학습 완료")

    return model


# ==================================================
# 8. 성능 평가
# ==================================================
def calculate_rmse(y_true, y_pred):
    try:
        return mean_squared_error(y_true, y_pred, squared=False)
    except TypeError:
        return np.sqrt(mean_squared_error(y_true, y_pred))


def evaluate_model(model, X_test, y_test, test_df: pd.DataFrame):
    print("[EVALUATE] 모델 평가 시작")

    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = calculate_rmse(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    overall_metrics = pd.DataFrame([{
        "target_hour": TARGET_HOUR,
        "test_count": len(y_test),
        "mae": mae,
        "rmse": rmse,
        "r2": r2,
    }])

    result_df = test_df[["dam_name", "observed_at"]].copy()
    result_df["actual_inflow"] = y_test.values
    result_df["predicted_inflow"] = y_pred
    result_df["error"] = result_df["predicted_inflow"] - result_df["actual_inflow"]
    result_df["abs_error"] = result_df["error"].abs()

    dam_metrics = (
        result_df
        .groupby("dam_name")
        .agg(
            test_count=("actual_inflow", "count"),
            mae=("abs_error", "mean"),
            actual_mean=("actual_inflow", "mean"),
            predicted_mean=("predicted_inflow", "mean"),
        )
        .reset_index()
        .sort_values("mae")
    )

    print("[EVALUATE] 전체 성능")
    print(overall_metrics.to_string(index=False))

    print("\n[EVALUATE] 댐별 성능")
    print(dam_metrics.to_string(index=False))

    return overall_metrics, dam_metrics, result_df


# ==================================================
# 9. 결과 저장
# ==================================================
def save_outputs(model, feature_cols, medians, split_time, overall_metrics, dam_metrics):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    package = {
        "model": model,
        "feature_cols": feature_cols,
        "medians": medians,
        "target_hour": TARGET_HOUR,
        "split_time": str(split_time),
        "rain_windows": RAIN_WINDOWS,
    }

    joblib.dump(package, MODEL_PATH)

    overall_metrics.to_csv(METRIC_PATH, index=False, encoding="utf-8-sig")
    dam_metrics.to_csv(DAM_METRIC_PATH, index=False, encoding="utf-8-sig")

    feature_info = {
        "target_hour": TARGET_HOUR,
        "split_time": str(split_time),
        "feature_count": len(feature_cols),
        "features": feature_cols,
    }

    with open(FEATURE_INFO_PATH, "w", encoding="utf-8") as f:
        json.dump(feature_info, f, ensure_ascii=False, indent=2)

    print("[SAVE] 모델 저장:", MODEL_PATH)
    print("[SAVE] 전체 성능 저장:", METRIC_PATH)
    print("[SAVE] 댐별 성능 저장:", DAM_METRIC_PATH)
    print("[SAVE] feature 정보 저장:", FEATURE_INFO_PATH)


# ==================================================
# 10. 전체 실행
# ==================================================
def main():
    print("[START] 유입량 예측 모델 학습 시작")

    df = load_data()
    df = preprocess(df)
    df = create_features(df)

    (
        X_train,
        X_test,
        y_train,
        y_test,
        train_df,
        test_df,
        feature_cols,
        medians,
        split_time,
    ) = build_train_test_data(df)

    model = train_model(X_train, y_train)

    overall_metrics, dam_metrics, prediction_result_df = evaluate_model(
        model,
        X_test,
        y_test,
        test_df
    )

    save_outputs(
        model,
        feature_cols,
        medians,
        split_time,
        overall_metrics,
        dam_metrics
    )

    print("[END] 유입량 예측 모델 학습 종료")


if __name__ == "__main__":
    main()