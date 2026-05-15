import os
import glob
import re
from urllib.parse import quote_plus

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

try:
    from target_dams import get_standard_dam_name
except Exception:
    get_standard_dam_name = None


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

SOURCE_TABLE = "final_historical_data+dam_observation+sluice_observation"
HISTORICAL_SOURCE_TABLE = "final_historical_data"
MODEL_VERSION = "V3_CLEANED_BY_DAM_6H"
MODEL_DIR = "model_results_v3_cleaned_by_dam"

RAIN_WINDOWS = [1, 3, 6, 12, 24, 48, 72]
INFLOW_LAGS = [1, 3, 6, 12, 24]
INFLOW_ROLLING_WINDOWS = [3, 6, 12, 24]

# 위험도 산정 최대 점수
MAX_INFLOW_SCORE = 35
MAX_STORAGE_SCORE = 25
MAX_RAINFALL_SCORE = 20
MAX_DISCHARGE_BALANCE_SCORE = 10
MAX_WATER_LEVEL_TREND_SCORE = 10


# ==================================================
# 3. 유틸 함수
# ==================================================
def safe_float(value):
    if value is None:
        return None

    if pd.isna(value):
        return None

    try:
        return float(value)
    except Exception:
        return None


def safe_filename(name: str) -> str:
    name = str(name)
    name = re.sub(r"[\\/:*?\"<>|()\s]+", "_", name)
    return name.strip("_")


def standardize_dam_name(name):
    if name is None:
        return None

    raw_name = str(name).strip()

    if get_standard_dam_name is not None:
        try:
            standardized = get_standard_dam_name(raw_name)
            if standardized:
                return standardized
        except Exception:
            pass

    return raw_name


def get_risk_level(score: float) -> str:
    if score >= 80:
        return "위험"
    elif score >= 60:
        return "경계"
    elif score >= 30:
        return "주의"
    else:
        return "낮음"


def get_risk_message(level: str) -> str:
    if level == "위험":
        return "예측 유입량과 현재 저수 상태를 고려할 때 즉시 방류 검토가 필요합니다."
    elif level == "경계":
        return "유입량 증가 가능성이 높아 사전 방류 및 하류 상황 확인이 필요합니다."
    elif level == "주의":
        return "일부 위험 요인이 있어 지속적인 모니터링이 필요합니다."
    else:
        return "현재 기준으로 위험도는 낮은 상태입니다."


# ==================================================
# 4. dam_risk_score 테이블 생성
# ==================================================
def create_risk_table():
    sql = text("""
        CREATE TABLE IF NOT EXISTS dam_risk_score (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,

            dam_code VARCHAR(20),
            dam_name VARCHAR(100) NOT NULL,
            observed_at DATETIME NOT NULL,

            current_inflow DECIMAL(12,3),
            predicted_inflow_6h DECIMAL(12,3),
            current_discharge DECIMAL(12,3),
            water_level DECIMAL(10,3),
            water_level_diff_6h DECIMAL(10,3),
            storage_rate DECIMAL(7,3),

            hydrology_rainfall_24h DECIMAL(10,3),
            kma_rainfall_24h DECIMAL(10,3),

            inflow_p70 DECIMAL(12,3),
            inflow_p90 DECIMAL(12,3),
            inflow_p95 DECIMAL(12,3),
            inflow_p99 DECIMAL(12,3),

            inflow_score DECIMAL(6,2),
            storage_score DECIMAL(6,2),
            rainfall_score DECIMAL(6,2),
            discharge_balance_score DECIMAL(6,2),
            water_level_trend_score DECIMAL(6,2),

            risk_score DECIMAL(6,2),
            risk_level VARCHAR(20),
            risk_message VARCHAR(255),

            model_version VARCHAR(50),
            source_table VARCHAR(100),

            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

            UNIQUE KEY uq_dam_risk_score (
                dam_name,
                observed_at,
                model_version
            )
        );
    """)

    with engine.begin() as conn:
        conn.execute(sql)

    print("[DB] dam_risk_score 테이블 준비 완료")


# ==================================================
# 5. V3 모델 로드
# ==================================================
def load_v3_models():
    pattern = os.path.join(MODEL_DIR, f"*_inflow_after_{TARGET_HOUR}h_model_v3.pkl")
    model_paths = glob.glob(pattern)

    if not model_paths:
        raise FileNotFoundError(
            f"{MODEL_DIR} 폴더에서 V3 모델 파일을 찾지 못했습니다. "
            f"먼저 train_inflow_prediction_model_v3_cleaned_by_dam.py를 실행하세요."
        )

    models = {}

    for path in model_paths:
        package = joblib.load(path)
        dam_name = package.get("dam_name")

        if dam_name is None:
            continue

        models[dam_name] = package

    print(f"[MODEL] 로드된 V3 모델 수: {len(models)}개")

    return models


# ==================================================
# 6. 과거 학습 데이터 및 실시간 관측 데이터 조회
# ==================================================
def load_final_data():
    query = f"""
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
        FROM {HISTORICAL_SOURCE_TABLE}
        ORDER BY dam_name, observed_at
    """

    print("[LOAD] final_historical_data 조회 시작")

    df = pd.read_sql(query, engine)

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

    df["hydrology_rainfall"] = df["hydrology_rainfall"].fillna(0)
    df["kma_rainfall"] = df["kma_rainfall"].fillna(0)
    df["snow"] = df["snow"].fillna(0)

    df = df.sort_values(["dam_name", "observed_at"]).reset_index(drop=True)

    print(f"[LOAD] 조회 완료: {len(df):,}건")
    print(f"[LOAD] 댐 개수: {df['dam_name'].nunique()}개")
    print(f"[LOAD] 기간: {df['observed_at'].min()} ~ {df['observed_at'].max()}")

    return df


def normalize_observation_df(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    if df.empty:
        return df

    result = df.copy()
    result["dam_name"] = result["dam_name"].apply(standardize_dam_name)
    result = result[result["dam_name"].notna()].copy()
    result["observed_at"] = pd.to_datetime(result["observed_at"], errors="coerce")

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
        if col not in result.columns:
            result[col] = np.nan
        result[col] = pd.to_numeric(result[col], errors="coerce")

    if "dam_code" not in result.columns:
        result["dam_code"] = None

    result["source_table"] = source_name

    return result[
        [
            "dam_code",
            "dam_name",
            "observed_at",
            "inflow",
            "water_level",
            "hydrology_rainfall",
            "storage_amount",
            "storage_rate",
            "discharge",
            "temperature",
            "kma_rainfall",
            "snow",
            "source_table",
        ]
    ]


def load_latest_observation_data() -> pd.DataFrame:
    """
    dam_observation과 sluice_observation에서 관측 데이터를 가져온다.

    같은 댐/시각에 두 테이블 데이터가 모두 있으면 dam_observation을 우선 사용하고,
    dam_observation에 없는 시각은 sluice_observation으로 보강한다.
    """
    dam_query = """
        SELECT
            NULL AS dam_code,
            dam_name,
            observed_at,
            inflow,
            water_level,
            rainfall AS hydrology_rainfall,
            storage_amount,
            storage_rate,
            discharge,
            NULL AS temperature,
            0 AS kma_rainfall,
            0 AS snow
        FROM dam_observation
        ORDER BY dam_name, observed_at;
    """

    sluice_query = """
        SELECT
            dam_code,
            dam_name,
            observed_at,
            inflow,
            water_level,
            rainfall AS hydrology_rainfall,
            storage_amount,
            storage_rate,
            discharge,
            NULL AS temperature,
            0 AS kma_rainfall,
            0 AS snow
        FROM sluice_observation
        ORDER BY dam_name, observed_at;
    """

    frames = []

    try:
        dam_df = pd.read_sql(dam_query, engine)
        dam_df = normalize_observation_df(dam_df, "dam_observation")
        if not dam_df.empty:
            frames.append(dam_df)
            print(f"[LOAD] dam_observation 조회 완료: {len(dam_df):,}건")
    except Exception as exc:
        print("[WARNING] dam_observation 조회 실패:", exc)

    try:
        sluice_df = pd.read_sql(sluice_query, engine)
        sluice_df = normalize_observation_df(sluice_df, "sluice_observation")
        if not sluice_df.empty:
            frames.append(sluice_df)
            print(f"[LOAD] sluice_observation 조회 완료: {len(sluice_df):,}건")
    except Exception as exc:
        print("[WARNING] sluice_observation 조회 실패:", exc)

    if not frames:
        print("[WARNING] 사용 가능한 실시간 관측 데이터가 없습니다.")
        return pd.DataFrame()

    observation_df = pd.concat(frames, ignore_index=True)
    observation_df = observation_df.dropna(subset=["dam_name", "observed_at"])

    source_priority = {
        "dam_observation": 1,
        "sluice_observation": 2,
    }

    observation_df["_source_priority"] = (
        observation_df["source_table"]
        .map(source_priority)
        .fillna(99)
        .astype(int)
    )

    observation_df = (
        observation_df
        .sort_values(["dam_name", "observed_at", "_source_priority"])
        .drop_duplicates(["dam_name", "observed_at"], keep="first")
        .drop(columns=["_source_priority"])
        .reset_index(drop=True)
    )

    print(f"[LOAD] 통합 실시간 관측 데이터: {len(observation_df):,}건")
    print(f"[LOAD] 통합 관측 기간: {observation_df['observed_at'].min()} ~ {observation_df['observed_at'].max()}")

    return observation_df


def append_latest_observations(
    historical_df: pd.DataFrame,
    observation_df: pd.DataFrame
) -> pd.DataFrame:
    """
    과거 시계열 뒤에 최신 관측 데이터를 붙인다.

    모델 feature는 lag/rolling 값이 필요하므로 과거 데이터는 유지하고,
    각 댐별 final_historical_data 이후의 관측 행만 추가한다.
    """
    if observation_df.empty:
        print("[WARNING] 실시간 관측 데이터가 없어 final_historical_data만 사용합니다.")
        return historical_df.copy()

    latest_historical_time_by_dam = (
        historical_df
        .groupby("dam_name")["observed_at"]
        .max()
        .to_dict()
    )

    dam_code_by_name = (
        historical_df
        .dropna(subset=["dam_code"])
        .sort_values("observed_at")
        .groupby("dam_name")["dam_code"]
        .last()
        .to_dict()
    )

    realtime_df = observation_df.copy()
    realtime_df["latest_historical_time"] = realtime_df["dam_name"].map(
        latest_historical_time_by_dam
    )

    realtime_df = realtime_df[
        realtime_df["observed_at"] > realtime_df["latest_historical_time"]
    ].copy()

    if realtime_df.empty:
        print("[WARNING] final_historical_data 이후의 신규 관측 데이터가 없습니다.")
        return historical_df.copy()

    realtime_df["dam_code"] = realtime_df.apply(
        lambda row: (
            row["dam_code"]
            if pd.notna(row["dam_code"]) and str(row["dam_code"]).strip() != ""
            else dam_code_by_name.get(row["dam_name"])
        ),
        axis=1
    )

    realtime_df = realtime_df.drop(columns=["latest_historical_time"])

    combined_df = pd.concat([historical_df, realtime_df], ignore_index=True)
    combined_df = (
        combined_df
        .sort_values(["dam_name", "observed_at"])
        .drop_duplicates(["dam_name", "observed_at"], keep="last")
        .reset_index(drop=True)
    )

    latest_combined = (
        combined_df
        .sort_values("observed_at")
        .groupby("dam_name")
        .tail(1)
    )

    print(f"[LOAD] 신규 실시간 관측 추가: {len(realtime_df):,}건")
    print(f"[LOAD] 통합 데이터 총 행 수: {len(combined_df):,}건")
    print(f"[LOAD] 통합 데이터 최신 시각: {combined_df['observed_at'].max()}")
    print("[LOAD] 댐별 최신 관측 시각:")
    print(
        latest_combined[["dam_name", "observed_at"]]
        .sort_values("dam_name")
        .to_string(index=False)
    )

    return combined_df


# ==================================================
# 7. 댐별 유입량 분위수 계산
# ==================================================
def calculate_inflow_quantiles(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for dam_name, dam_df in df.groupby("dam_name"):
        inflow = pd.to_numeric(dam_df["inflow"], errors="coerce").dropna()

        if inflow.empty:
            continue

        rows.append({
            "dam_name": dam_name,
            "inflow_p70": inflow.quantile(0.70),
            "inflow_p90": inflow.quantile(0.90),
            "inflow_p95": inflow.quantile(0.95),
            "inflow_p99": inflow.quantile(0.99),
        })

    q_df = pd.DataFrame(rows)

    print("[STAT] 댐별 유입량 분위수 계산 완료")

    return q_df


# ==================================================
# 8. V3 feature 생성
# ==================================================
def apply_inflow_cap(dam_df: pd.DataFrame, cap_value: float) -> pd.DataFrame:
    df = dam_df.copy()

    df["inflow_original"] = df["inflow"]

    if pd.notna(cap_value) and cap_value > 0:
        df["inflow_cleaned"] = df["inflow"].clip(lower=0, upper=cap_value)
    else:
        df["inflow_cleaned"] = df["inflow"].clip(lower=0)

    return df


def create_features_for_one_dam(dam_df: pd.DataFrame) -> pd.DataFrame:
    df = dam_df.copy()
    df = df.sort_values("observed_at").reset_index(drop=True)

    df["hour"] = df["observed_at"].dt.hour
    df["month"] = df["observed_at"].dt.month
    df["dayofyear"] = df["observed_at"].dt.dayofyear

    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    for rain_col in ["hydrology_rainfall", "kma_rainfall"]:
        for window in RAIN_WINDOWS:
            df[f"{rain_col}_{window}h_sum"] = (
                df[rain_col]
                .rolling(window=window, min_periods=1)
                .sum()
            )

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

    df["inflow_diff_1h"] = df["inflow_cleaned"].diff(1)
    df["water_level_diff_1h"] = df["water_level"].diff(1)
    df["storage_rate_diff_1h"] = df["storage_rate"].diff(1)
    df["discharge_diff_1h"] = df["discharge"].diff(1)

    # 위험도 계산용 최근 6시간 수위 변화량
    df["water_level_diff_6h"] = df["water_level"] - df["water_level"].shift(6)

    return df


# ==================================================
# 9. 최신 시점 6시간 뒤 유입량 예측
# ==================================================
def predict_latest_inflow_for_one_dam(
    dam_name: str,
    dam_df: pd.DataFrame,
    package: dict
):
    cap_value = package.get("inflow_cap", np.nan)
    feature_cols = package["feature_cols"]
    medians = package["medians"]
    model = package["model"]

    dam_df = apply_inflow_cap(dam_df, cap_value)
    feature_df = create_features_for_one_dam(dam_df)

    latest_row = feature_df.sort_values("observed_at").tail(1).copy()

    X = latest_row[feature_cols].copy()
    X = X.fillna(medians)

    y_pred_log = model.predict(X)
    y_pred = np.expm1(y_pred_log)
    y_pred = np.clip(y_pred, 0, None)

    predicted_inflow = float(y_pred[0])

    return latest_row.iloc[0], predicted_inflow


# ==================================================
# 10. 위험도 점수 계산 함수
# ==================================================
def score_predicted_inflow(predicted_inflow, p70, p90, p95, p99):
    """
    예측 유입량 점수
    최대 35점

    보정:
    - 예측 유입량이 1 미만이면 위험 점수를 주지 않는다.
    - 분위수 기준값이 0 또는 너무 작으면 절대값 기준으로 보수적으로 판단한다.
    """

    if predicted_inflow is None or pd.isna(predicted_inflow):
        return 0

    predicted_inflow = float(predicted_inflow)

    # 매우 작은 유입량은 위험으로 보지 않음
    if predicted_inflow < 1:
        return 0

    # p90이 너무 작으면 분위수 기준이 과민하게 작동할 수 있으므로 절대값 기준 적용
    if p90 is None or pd.isna(p90) or p90 < 1:
        if predicted_inflow >= 100:
            return 28
        elif predicted_inflow >= 50:
            return 22
        elif predicted_inflow >= 20:
            return 12
        elif predicted_inflow >= 5:
            return 5
        else:
            return 0

    if p99 is not None and pd.notna(p99) and predicted_inflow >= p99:
        return 35
    elif p95 is not None and pd.notna(p95) and predicted_inflow >= p95:
        return 28
    elif p90 is not None and pd.notna(p90) and predicted_inflow >= p90:
        return 22
    elif p70 is not None and pd.notna(p70) and predicted_inflow >= p70:
        return 12
    else:
        return 3


def score_storage_rate(storage_rate):
    """
    저수율 점수
    최대 25점
    """

    if storage_rate is None or pd.isna(storage_rate):
        return 0

    storage_rate = float(storage_rate)

    if storage_rate >= 90:
        return 25
    elif storage_rate >= 80:
        return 20
    elif storage_rate >= 70:
        return 12
    elif storage_rate >= 60:
        return 6
    else:
        return 0


def score_rainfall_24h(kma_rainfall_24h, hydrology_rainfall_24h):
    """
    최근 24시간 강수량 점수
    최대 20점

    kma_rainfall_24h와 hydrology_rainfall_24h 중 큰 값을 기준으로 판단한다.
    """

    rainfall = max(
        0 if pd.isna(kma_rainfall_24h) else float(kma_rainfall_24h),
        0 if pd.isna(hydrology_rainfall_24h) else float(hydrology_rainfall_24h),
    )

    if rainfall >= 80:
        return 20
    elif rainfall >= 50:
        return 16
    elif rainfall >= 30:
        return 10
    elif rainfall >= 10:
        return 5
    else:
        return 0


def score_discharge_balance(predicted_inflow, current_discharge):
    """
    예측 유입량과 현재 방류량의 불균형 점수
    최대 10점

    보정:
    - 예측 유입량이 1 미만이면 위험 점수를 주지 않는다.
    - 현재 방류량이 0이어도 예측 유입량이 작은 경우 위험으로 보지 않는다.
    """

    if predicted_inflow is None or pd.isna(predicted_inflow):
        return 0

    predicted_inflow = float(predicted_inflow)

    # 매우 작은 유입량은 방류 불균형으로 보지 않음
    if predicted_inflow < 1:
        return 0

    if current_discharge is None or pd.isna(current_discharge) or current_discharge <= 0:
        if predicted_inflow >= 100:
            return 10
        elif predicted_inflow >= 50:
            return 8
        elif predicted_inflow >= 20:
            return 5
        elif predicted_inflow >= 5:
            return 2
        else:
            return 0

    current_discharge = float(current_discharge)
    ratio = predicted_inflow / current_discharge

    if ratio >= 3:
        return 10
    elif ratio >= 2:
        return 8
    elif ratio >= 1.2:
        return 5
    elif ratio >= 1:
        return 2
    else:
        return 0


def score_water_level_trend(water_level_diff_6h):
    """
    최근 6시간 수위 상승량 점수
    최대 10점
    """

    if water_level_diff_6h is None or pd.isna(water_level_diff_6h):
        return 0

    water_level_diff_6h = float(water_level_diff_6h)

    if water_level_diff_6h >= 1.0:
        return 10
    elif water_level_diff_6h >= 0.5:
        return 7
    elif water_level_diff_6h >= 0.2:
        return 4
    elif water_level_diff_6h > 0:
        return 2
    else:
        return 0


# ==================================================
# 11. 전체 위험도 계산
# ==================================================
def calculate_risk_scores(
    df: pd.DataFrame,
    models: dict,
    quantile_df: pd.DataFrame
) -> pd.DataFrame:
    result_rows = []

    quantile_map = {
        row["dam_name"]: row
        for _, row in quantile_df.iterrows()
    }

    dam_names = sorted(df["dam_name"].dropna().unique())

    for dam_name in dam_names:
        if dam_name not in models:
            print(f"[SKIP] {dam_name}: V3 모델 없음")
            continue

        dam_df = df[df["dam_name"] == dam_name].copy()

        if dam_df.empty:
            continue

        package = models[dam_name]

        latest_row, predicted_inflow = predict_latest_inflow_for_one_dam(
            dam_name=dam_name,
            dam_df=dam_df,
            package=package
        )

        q = quantile_map.get(dam_name)

        if q is None:
            print(f"[SKIP] {dam_name}: 분위수 정보 없음")
            continue

        p70 = safe_float(q["inflow_p70"])
        p90 = safe_float(q["inflow_p90"])
        p95 = safe_float(q["inflow_p95"])
        p99 = safe_float(q["inflow_p99"])

        current_inflow = safe_float(latest_row["inflow"])
        current_discharge = safe_float(latest_row["discharge"])
        water_level = safe_float(latest_row["water_level"])
        water_level_diff_6h = safe_float(latest_row["water_level_diff_6h"])
        storage_rate = safe_float(latest_row["storage_rate"])

        hydrology_rainfall_24h = safe_float(latest_row["hydrology_rainfall_24h_sum"])
        kma_rainfall_24h = safe_float(latest_row["kma_rainfall_24h_sum"])

        inflow_score = score_predicted_inflow(
            predicted_inflow,
            p70,
            p90,
            p95,
            p99
        )

        storage_score = score_storage_rate(storage_rate)

        rainfall_score = score_rainfall_24h(
            kma_rainfall_24h,
            hydrology_rainfall_24h
        )

        discharge_balance_score = score_discharge_balance(
            predicted_inflow,
            current_discharge
        )

        water_level_trend_score = score_water_level_trend(
            water_level_diff_6h
        )

        risk_score = (
            inflow_score
            + storage_score
            + rainfall_score
            + discharge_balance_score
            + water_level_trend_score
        )

        risk_score = min(100, max(0, risk_score))

        risk_level = get_risk_level(risk_score)
        risk_message = get_risk_message(risk_level)

        result_rows.append({
            "dam_code": latest_row["dam_code"],
            "dam_name": dam_name,
            "observed_at": latest_row["observed_at"],

            "current_inflow": current_inflow,
            "predicted_inflow_6h": predicted_inflow,
            "current_discharge": current_discharge,
            "water_level": water_level,
            "water_level_diff_6h": water_level_diff_6h,
            "storage_rate": storage_rate,

            "hydrology_rainfall_24h": hydrology_rainfall_24h,
            "kma_rainfall_24h": kma_rainfall_24h,

            "inflow_p70": p70,
            "inflow_p90": p90,
            "inflow_p95": p95,
            "inflow_p99": p99,

            "inflow_score": inflow_score,
            "storage_score": storage_score,
            "rainfall_score": rainfall_score,
            "discharge_balance_score": discharge_balance_score,
            "water_level_trend_score": water_level_trend_score,

            "risk_score": risk_score,
            "risk_level": risk_level,
            "risk_message": risk_message,

            "model_version": MODEL_VERSION,
            "source_table": latest_row.get("source_table", SOURCE_TABLE),
        })

    result_df = pd.DataFrame(result_rows)

    if result_df.empty:
        return result_df

    result_df = result_df.sort_values(
        ["risk_score", "dam_name"],
        ascending=[False, True]
    ).reset_index(drop=True)

    return result_df


# ==================================================
# 12. DB 저장
# ==================================================
def save_risk_scores_to_db(result_df: pd.DataFrame):
    if result_df.empty:
        print("[DB] 저장할 위험도 결과가 없습니다.")
        return

    insert_sql = text("""
        INSERT INTO dam_risk_score (
            dam_code,
            dam_name,
            observed_at,

            current_inflow,
            predicted_inflow_6h,
            current_discharge,
            water_level,
            water_level_diff_6h,
            storage_rate,

            hydrology_rainfall_24h,
            kma_rainfall_24h,

            inflow_p70,
            inflow_p90,
            inflow_p95,
            inflow_p99,

            inflow_score,
            storage_score,
            rainfall_score,
            discharge_balance_score,
            water_level_trend_score,

            risk_score,
            risk_level,
            risk_message,

            model_version,
            source_table
        )
        VALUES (
            :dam_code,
            :dam_name,
            :observed_at,

            :current_inflow,
            :predicted_inflow_6h,
            :current_discharge,
            :water_level,
            :water_level_diff_6h,
            :storage_rate,

            :hydrology_rainfall_24h,
            :kma_rainfall_24h,

            :inflow_p70,
            :inflow_p90,
            :inflow_p95,
            :inflow_p99,

            :inflow_score,
            :storage_score,
            :rainfall_score,
            :discharge_balance_score,
            :water_level_trend_score,

            :risk_score,
            :risk_level,
            :risk_message,

            :model_version,
            :source_table
        )
        ON DUPLICATE KEY UPDATE
            current_inflow = VALUES(current_inflow),
            predicted_inflow_6h = VALUES(predicted_inflow_6h),
            current_discharge = VALUES(current_discharge),
            water_level = VALUES(water_level),
            water_level_diff_6h = VALUES(water_level_diff_6h),
            storage_rate = VALUES(storage_rate),

            hydrology_rainfall_24h = VALUES(hydrology_rainfall_24h),
            kma_rainfall_24h = VALUES(kma_rainfall_24h),

            inflow_p70 = VALUES(inflow_p70),
            inflow_p90 = VALUES(inflow_p90),
            inflow_p95 = VALUES(inflow_p95),
            inflow_p99 = VALUES(inflow_p99),

            inflow_score = VALUES(inflow_score),
            storage_score = VALUES(storage_score),
            rainfall_score = VALUES(rainfall_score),
            discharge_balance_score = VALUES(discharge_balance_score),
            water_level_trend_score = VALUES(water_level_trend_score),

            risk_score = VALUES(risk_score),
            risk_level = VALUES(risk_level),
            risk_message = VALUES(risk_message),

            source_table = VALUES(source_table),
            updated_at = CURRENT_TIMESTAMP
    """)

    save_df = result_df.copy()
    save_df = save_df.astype(object).where(pd.notnull(save_df), None)

    records = save_df.to_dict("records")

    with engine.begin() as conn:
        conn.execute(insert_sql, records)

    print(f"[DB] dam_risk_score 저장 완료: {len(records)}건")


# ==================================================
# 13. CSV 저장
# ==================================================
def save_risk_scores_to_csv(result_df: pd.DataFrame):
    output_dir = "risk_results"
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "dam_risk_score_latest.csv")

    result_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print("[CSV] 위험도 결과 저장:", output_path)


# ==================================================
# 14. 요약 출력
# ==================================================
def print_summary(result_df: pd.DataFrame):
    print("\n" + "=" * 100)
    print("[위험도 계산 결과]")
    print("=" * 100)

    display_cols = [
        "dam_name",
        "observed_at",
        "current_inflow",
        "predicted_inflow_6h",
        "current_discharge",
        "storage_rate",
        "kma_rainfall_24h",
        "risk_score",
        "risk_level",
    ]

    print(result_df[display_cols].to_string(index=False))

    print("\n[점수 해석]")
    print("0~29   : 낮음")
    print("30~59  : 주의")
    print("60~79  : 경계")
    print("80~100 : 위험")

    print("\n[주의]")
    print("- 분위수와 모델 feature 기준은 final_historical_data를 사용합니다.")
    print("- 현재값은 dam_observation과 sluice_observation의 최신 관측 데이터를 연결해 계산합니다.")


# ==================================================
# 15. 실행
# ==================================================
def main():
    print("[START] 댐 위험도 점수 계산 시작")

    create_risk_table()

    models = load_v3_models()
    historical_df = load_final_data()
    quantile_df = calculate_inflow_quantiles(historical_df)

    observation_df = load_latest_observation_data()
    df = append_latest_observations(historical_df, observation_df)

    result_df = calculate_risk_scores(df, models, quantile_df)

    if result_df.empty:
        print("[WARNING] 계산된 위험도 결과가 없습니다.")
        return

    save_risk_scores_to_db(result_df)
    save_risk_scores_to_csv(result_df)
    print_summary(result_df)

    print("[END] 댐 위험도 점수 계산 종료")


if __name__ == "__main__":
    main()
