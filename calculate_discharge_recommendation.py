import os
import re
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

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

    if value is None or str(value).strip() == "":
        raise ValueError(f".env 파일에 {key} 값이 없습니다.")

    return value


DB_USER = get_required_env("DB_USER")
DB_PASSWORD = quote_plus(get_required_env("DB_PASSWORD"))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = get_required_env("DB_NAME")


# ==================================================
# 2. MySQL 연결
# ==================================================

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
)


# ==================================================
# 3. 기본 설정
# ==================================================

HORIZONS = [6, 12, 24, 48, 72, 120]
RISK_MODEL_VERSION = "V3_CLEANED_BY_DAM_6H"

OUTPUT_DIR = Path("discharge_results")
OUTPUT_DIR.mkdir(exist_ok=True)

LOW_CONFIDENCE_DAMS = {"소양강", "대청", "주암(조)"}

RECOMMENDATION_LEVEL_PRIORITY = {
    "강한 사전방류 검토": 5,
    "단계적 방류 증가 검토": 4,
    "사전방류 검토": 3,
    "관찰": 2,
    "유지": 1,
}


# ==================================================
# 4. 유틸 함수
# ==================================================

def normalize_name_key(name):
    if name is None:
        return ""

    text_value = str(name).strip()
    text_value = " ".join(text_value.split())

    text_value = text_value.replace("（", "(").replace("）", ")")
    text_value = text_value.replace("댐", "")
    text_value = text_value.replace(" ", "")
    text_value = text_value.lower()

    return text_value


def standardize_dam_name(name):
    if name is None:
        return None

    raw_name = str(name).strip()
    raw_name = " ".join(raw_name.split())
    raw_name = raw_name.replace("（", "(").replace("）", ")")

    DAM_NAME_ALIASES = {
        # 한글 표준명
        "소양강": "소양강",
        "충주": "충주",
        "횡성": "횡성",
        "안동": "안동",
        "임하": "임하",
        "성덕": "성덕",
        "영주": "영주",
        "군위": "군위",
        "보현산": "보현산",
        "대청": "대청",
        "용담": "용담",
        "섬진강": "섬진강",
        "주암(본)": "주암(본)",
        "주암(조)": "주암(조)",
        "합천": "합천",
        "남강": "남강",
        "밀양": "밀양",
        "보령": "보령",
        "부안": "부안",
        "장흥": "장흥",

        # 댐 접미사 포함
        "소양강댐": "소양강",
        "충주댐": "충주",
        "횡성댐": "횡성",
        "안동댐": "안동",
        "임하댐": "임하",
        "성덕댐": "성덕",
        "영주댐": "영주",
        "군위댐": "군위",
        "보현산댐": "보현산",
        "대청댐": "대청",
        "용담댐": "용담",
        "섬진강댐": "섬진강",
        "주암댐": "주암(본)",
        "주암본댐": "주암(본)",
        "주암조댐": "주암(조)",
        "주암조절지댐": "주암(조)",
        "합천댐": "합천",
        "남강댐": "남강",
        "밀양댐": "밀양",
        "보령댐": "보령",
        "부안댐": "부안",
        "장흥댐": "장흥",

        # 영문 / 자동 번역 대응
        "Soyang River": "소양강",
        "Soyang": "소양강",
        "Chungju": "충주",
        "Hoengseong": "횡성",
        "Andong": "안동",
        "Imha": "임하",
        "Seongdeok": "성덕",
        "Yeongju": "영주",
        "Youngju": "영주",
        "lord": "영주",
        "Gunwi": "군위",
        "military rank": "군위",
        "Bohyeon Mountain": "보현산",
        "Bohyeonsan": "보현산",
        "Daecheong": "대청",
        "daecheong": "대청",
        "Yongdam": "용담",
        "gentian": "용담",
        "lord gentian": "용담",
        "Seomjingang River": "섬진강",
        "Seomjingang": "섬진강",
        "Juam (Bon)": "주암(본)",
        "Juam(Bon)": "주암(본)",
        "Juam (Joe)": "주암(조)",
        "Juam(Joe)": "주암(조)",
        "Juam (Jo)": "주암(조)",
        "Juam(Jo)": "주암(조)",
        "Hapcheon": "합천",
        "Namgang": "남강",
        "Miryang": "밀양",
        "Boryeong": "보령",
        "Buan": "부안",
        "Jangheung": "장흥",
    }

    alias_by_key = {
        normalize_name_key(key): value
        for key, value in DAM_NAME_ALIASES.items()
    }

    name_key = normalize_name_key(raw_name)

    if name_key in alias_by_key:
        return alias_by_key[name_key]

    if get_standard_dam_name is not None:
        try:
            standardized = get_standard_dam_name(raw_name)
            if standardized:
                return standardized
        except Exception:
            pass

    return raw_name


def parse_rainfall(value):
    """
    기상청 강수량 문자열을 mm 숫자로 변환한다.

    예:
    "강수없음" -> 0
    "1mm 미만" -> 0.5
    "1.0mm" -> 1.0
    "30.0~50.0mm" -> 40.0
    "50.0mm 이상" -> 50.0
    """
    if value is None:
        return 0.0

    text_value = str(value).strip()

    if text_value == "":
        return 0.0

    if "강수없음" in text_value:
        return 0.0

    if "없음" in text_value:
        return 0.0

    if "1mm 미만" in text_value:
        return 0.5

    numbers = re.findall(r"\d+\.?\d*", text_value)

    if not numbers:
        return 0.0

    if len(numbers) >= 2:
        return (float(numbers[0]) + float(numbers[1])) / 2

    return float(numbers[0])


def safe_float(value, default=0.0):
    if value is None:
        return default

    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def clean_date_time_value(value):
    if value is None:
        return ""

    if pd.isna(value):
        return ""

    text_value = str(value).strip()

    if text_value.endswith(".0"):
        text_value = text_value[:-2]

    text_value = re.sub(r"[^0-9]", "", text_value)

    return text_value


def combine_datetime(date_value, time_value):
    date_text = clean_date_time_value(date_value)
    time_text = clean_date_time_value(time_value)

    if date_text == "" or time_text == "":
        return pd.NaT

    time_text = time_text.zfill(4)

    return pd.to_datetime(
        date_text + time_text,
        format="%Y%m%d%H%M",
        errors="coerce"
    )


# ==================================================
# 5. DB 준비 및 조회 함수
# ==================================================

def create_discharge_recommendation_table():
    """
    방류 추천 결과 저장 테이블을 준비한다.

    calculation_time은 분 단위로 저장하므로, 같은 계산 시각/댐/예측시간 조합은
    ON DUPLICATE KEY UPDATE로 최신 결과를 덮어쓴다.
    """
    sql = text("""
        CREATE TABLE IF NOT EXISTS discharge_recommendation (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,

            dam_name VARCHAR(100) NOT NULL,
            calculation_time DATETIME NOT NULL,
            risk_observed_at DATETIME,
            forecast_horizon_hours INT NOT NULL,
            forecast_time DATETIME NOT NULL,

            current_inflow DECIMAL(12,3),
            predicted_inflow_6h DECIMAL(12,3),
            expected_inflow DECIMAL(12,3),

            current_discharge DECIMAL(12,3),
            discharge_gap DECIMAL(12,3),
            recommended_discharge DECIMAL(12,3),

            storage_rate DECIMAL(7,3),

            rainfall_6h DECIMAL(10,3),
            rainfall_12h DECIMAL(10,3),
            rainfall_24h DECIMAL(10,3),
            rainfall_48h DECIMAL(10,3),
            rainfall_72h DECIMAL(10,3),
            rainfall_120h DECIMAL(10,3),
            rainfall_until_horizon DECIMAL(10,3),

            recommendation_level VARCHAR(50),
            recommendation_message VARCHAR(500),
            data_warning VARCHAR(500),

            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

            UNIQUE KEY uq_discharge_recommendation (
                dam_name,
                calculation_time,
                forecast_horizon_hours
            )
        );
    """)

    with engine.begin() as conn:
        conn.execute(sql)

        try:
            conn.execute(text("""
                ALTER TABLE discharge_recommendation
                ADD UNIQUE KEY uq_discharge_recommendation (
                    dam_name,
                    calculation_time,
                    forecast_horizon_hours
                );
            """))
        except OperationalError as exc:
            error_code = getattr(getattr(exc, "orig", None), "args", [None])[0]

            if error_code == 1061:
                pass
            else:
                raise

    print("[DB] discharge_recommendation 테이블 준비 완료")


def load_latest_risk_score():
    """
    dam_risk_score에서 댐별 최신 위험도 계산 결과를 불러온다.
    여기에는 V3 모델의 predicted_inflow_6h가 들어 있다.
    """
    query = text("""
        SELECT r.*
        FROM dam_risk_score r
        INNER JOIN (
            SELECT
                dam_name,
                MAX(observed_at) AS max_observed_at
            FROM dam_risk_score
            WHERE model_version = :model_version
            GROUP BY dam_name
        ) latest
            ON r.dam_name = latest.dam_name
           AND r.observed_at = latest.max_observed_at
        WHERE r.model_version = :model_version
        ORDER BY r.dam_name;
    """)

    df = pd.read_sql(
        query,
        engine,
        params={"model_version": RISK_MODEL_VERSION}
    )

    if df.empty:
        raise ValueError(
            f"dam_risk_score 테이블에 {RISK_MODEL_VERSION} 데이터가 없습니다. "
            "먼저 calculate_dam_risk_score.py를 실행하세요."
        )

    df["dam_name"] = df["dam_name"].apply(standardize_dam_name)

    return df


def load_latest_forecast(table_name, category):
    """
    weather_forecast_short 또는 weather_forecast_mid에서
    특정 category 데이터 전체를 불러온 뒤,
    Python에서 댐별 최신 base_time만 남긴다.

    기존 SQL JOIN 방식보다 이름 매칭과 base_time 처리에 안전하다.
    """
    allowed_tables = {"weather_forecast_short", "weather_forecast_mid"}

    if table_name not in allowed_tables:
        raise ValueError(f"허용되지 않은 예보 테이블입니다: {table_name}")

    query = text(f"""
        SELECT
            source,
            region_name,
            nx,
            ny,
            base_date,
            base_time,
            forecast_date,
            forecast_time,
            category,
            forecast_value
        FROM {table_name}
        WHERE category = :category
        ORDER BY region_name, base_date, base_time, forecast_date, forecast_time;
    """)

    df = pd.read_sql(query, engine, params={"category": category})

    if df.empty:
        print(f"[WARNING] {table_name}에서 category={category} 데이터가 없습니다.")
        return pd.DataFrame()

    df["dam_name_std"] = df["region_name"].apply(standardize_dam_name)

    df["forecast_datetime"] = df.apply(
        lambda row: combine_datetime(row["forecast_date"], row["forecast_time"]),
        axis=1
    )
    df["base_datetime"] = df.apply(
        lambda row: combine_datetime(row["base_date"], row["base_time"]),
        axis=1
    )
    df["rainfall_mm"] = df["forecast_value"].apply(parse_rainfall)

    df = df.dropna(subset=["forecast_datetime", "base_datetime"]).copy()

    if df.empty:
        print(f"[WARNING] {table_name}의 날짜/시간 변환 후 남은 데이터가 없습니다.")
        return pd.DataFrame()

    df["base_key"] = df["base_datetime"].dt.strftime("%Y%m%d%H%M")

    latest_base_by_dam = df.groupby("dam_name_std")["base_key"].transform("max")

    latest_df = df[df["base_key"] == latest_base_by_dam].copy()

    print(f"[INFO] {table_name} / {category} 원본 행 수: {len(df)}")
    print(f"[INFO] {table_name} / {category} 최신 행 수: {len(latest_df)}")
    print(f"[INFO] {table_name} / {category} 매칭 댐 목록:")
    print(sorted(latest_df["dam_name_std"].dropna().unique().tolist()))

    return latest_df


# ==================================================
# 6. 강수량 누적 계산
# ==================================================

def cumulative_rainfall(forecast_df, dam_name, base_time, horizon_hours):
    """
    특정 댐의 현재 시점부터 horizon_hours까지 누적 강수량을 계산한다.
    """
    if forecast_df.empty:
        return 0.0

    sub_df = forecast_df[forecast_df["dam_name_std"] == dam_name].copy()

    if sub_df.empty:
        return 0.0

    end_time = base_time + timedelta(hours=horizon_hours)

    target_df = sub_df[
        (sub_df["forecast_datetime"] > base_time) &
        (sub_df["forecast_datetime"] <= end_time)
    ]

    if target_df.empty:
        return 0.0

    return round(float(target_df["rainfall_mm"].sum()), 3)


def get_latest_forecast_base_time(forecast_df, dam_name):
    if forecast_df.empty or "base_datetime" not in forecast_df.columns:
        return None

    if "dam_name_std" not in forecast_df.columns:
        return None

    sub_df = forecast_df[forecast_df["dam_name_std"] == dam_name]

    if sub_df.empty:
        return None

    latest_base_time = sub_df["base_datetime"].dropna().max()

    if pd.isna(latest_base_time):
        return None

    if hasattr(latest_base_time, "to_pydatetime"):
        latest_base_time = latest_base_time.to_pydatetime()

    return latest_base_time.replace(second=0, microsecond=0)


def get_forecast_reference_time(dam_name, calculation_time, short_df, mid_df):
    """
    예보 누적 기준 시각을 정한다.

    배치 실행 시각과 기상청 예보 base_time이 다를 수 있으므로,
    해당 댐에 대해 수집된 최신 base_datetime을 우선 사용한다.
    """
    reference_times = []

    for forecast_df in [short_df, mid_df]:
        if forecast_df.empty:
            continue

        latest_base_time = get_latest_forecast_base_time(forecast_df, dam_name)

        if latest_base_time is not None:
            reference_times.append(latest_base_time)

    if reference_times:
        return max(reference_times).replace(second=0, microsecond=0)

    return calculation_time


def build_rainfall_features(dam_name, reference_time, short_df, mid_df):
    """
    0~6시간은 초단기예보 RN1을 우선 사용하고,
    12시간 이상은 단기예보 PCP를 사용한다.
    """
    short_reference_time = (
        get_latest_forecast_base_time(short_df, dam_name) or reference_time
    )
    mid_reference_time = (
        get_latest_forecast_base_time(mid_df, dam_name) or reference_time
    )

    rainfall_6h_short = cumulative_rainfall(
        short_df,
        dam_name,
        short_reference_time,
        6
    )

    rainfall_6h_mid = cumulative_rainfall(
        mid_df,
        dam_name,
        mid_reference_time,
        6
    )

    rainfall_6h = rainfall_6h_short if rainfall_6h_short > 0 else rainfall_6h_mid

    features = {
        "rainfall_6h": rainfall_6h,
        "rainfall_12h": cumulative_rainfall(mid_df, dam_name, mid_reference_time, 12),
        "rainfall_24h": cumulative_rainfall(mid_df, dam_name, mid_reference_time, 24),
        "rainfall_48h": cumulative_rainfall(mid_df, dam_name, mid_reference_time, 48),
        "rainfall_72h": cumulative_rainfall(mid_df, dam_name, mid_reference_time, 72),
        "rainfall_120h": cumulative_rainfall(mid_df, dam_name, mid_reference_time, 120),
        "short_reference_time": short_reference_time,
        "mid_reference_time": mid_reference_time,
    }

    return features


def get_rainfall_until_horizon(rainfall_features, horizon_hours):
    key = f"rainfall_{horizon_hours}h"
    return safe_float(rainfall_features.get(key))


# ==================================================
# 7. 방류 추천 로직
# ==================================================

def estimate_expected_inflow(
    current_inflow,
    predicted_inflow_6h,
    rainfall_until_horizon,
    horizon_hours
):
    """
    MVP용 예상 유입량 추정값.

    현재 V3 모델은 6시간 뒤 유입량만 예측하므로,
    12~120시간 구간은 단기예보 누적 강수량을 이용해
    보수적으로 유입 압력을 보정한다.

    이 값은 공식 유량 예측값이 아니라 방류 추천 보조용 추정값이다.
    """
    current_inflow = safe_float(current_inflow)
    predicted_inflow_6h = safe_float(predicted_inflow_6h)
    rainfall_until_horizon = safe_float(rainfall_until_horizon)

    base_inflow = max(current_inflow, predicted_inflow_6h)

    if horizon_hours <= 6:
        rainfall_factor = 0.30
    elif horizon_hours <= 24:
        rainfall_factor = 0.45
    elif horizon_hours <= 72:
        rainfall_factor = 0.60
    else:
        rainfall_factor = 0.75

    rainfall_pressure = rainfall_until_horizon * rainfall_factor

    expected_inflow = base_inflow + rainfall_pressure

    return round(max(expected_inflow, 0.0), 3)


def calculate_recommended_discharge(
    current_inflow,
    predicted_inflow_6h,
    expected_inflow,
    current_discharge,
    storage_rate,
    rainfall_6h,
    rainfall_12h,
    rainfall_24h,
    rainfall_48h,
    rainfall_72h,
    rainfall_120h,
    rainfall_until_horizon,
    horizon_hours
):
    """
    방류 추천량 계산.

    주의:
    이 값은 실제 방류 명령이 아니라,
    관리자가 검토할 수 있는 AI 기반 보조 추천값이다.
    """
    current_inflow = safe_float(current_inflow)
    predicted_inflow_6h = safe_float(predicted_inflow_6h)
    expected_inflow = safe_float(expected_inflow)
    current_discharge = safe_float(current_discharge)
    storage_rate = safe_float(storage_rate)

    rainfall_6h = safe_float(rainfall_6h)
    rainfall_12h = safe_float(rainfall_12h)
    rainfall_24h = safe_float(rainfall_24h)
    rainfall_48h = safe_float(rainfall_48h)
    rainfall_72h = safe_float(rainfall_72h)
    rainfall_120h = safe_float(rainfall_120h)
    rainfall_until_horizon = safe_float(rainfall_until_horizon)

    discharge_gap = max(0.0, expected_inflow - current_discharge)

    increase_rate = 0.0
    level = "유지"
    reasons = []

    # 강수량이 없고, 저수율이 낮거나 보통이며, 예상 유입량도 작으면 유지
    if (
        expected_inflow < 5
        and rainfall_24h < 10
        and rainfall_48h < 20
        and storage_rate < 75
    ):
        recommended_discharge = current_discharge
        discharge_gap = max(0.0, expected_inflow - current_discharge)
        level = "유지"
        message = "강수량과 예상 유입량이 낮고 저수율도 안정적이므로 현재 방류량 유지가 적절합니다."
        return round(recommended_discharge, 3), round(discharge_gap, 3), level, message

    # 저수율 기준
    if storage_rate >= 90:
        increase_rate += 0.80
        reasons.append("저수율 90% 이상")
    elif storage_rate >= 85:
        increase_rate += 0.60
        reasons.append("저수율 85% 이상")
    elif storage_rate >= 80:
        increase_rate += 0.40
        reasons.append("저수율 80% 이상")
    elif storage_rate >= 75:
        increase_rate += 0.20
        reasons.append("저수율 75% 이상")

    # 강수량 기준: 예측 horizon 안에 실제로 포함되는 강수 지표만 반영한다.
    if horizon_hours <= 6:
        if rainfall_6h >= 20:
            increase_rate += 0.35
            reasons.append("6시간 누적 강수량 20mm 이상")
        elif rainfall_6h >= 10:
            increase_rate += 0.20
            reasons.append("6시간 누적 강수량 10mm 이상")
        elif rainfall_6h >= 5:
            increase_rate += 0.10
            reasons.append("6시간 누적 강수량 5mm 이상")
    elif horizon_hours <= 12:
        if rainfall_12h >= 35:
            increase_rate += 0.45
            reasons.append("12시간 누적 강수량 35mm 이상")
        elif rainfall_12h >= 20:
            increase_rate += 0.30
            reasons.append("12시간 누적 강수량 20mm 이상")
        elif rainfall_12h >= 10:
            increase_rate += 0.15
            reasons.append("12시간 누적 강수량 10mm 이상")
    elif horizon_hours <= 24:
        if rainfall_24h >= 50:
            increase_rate += 0.60
            reasons.append("24시간 누적 강수량 50mm 이상")
        elif rainfall_24h >= 30:
            increase_rate += 0.40
            reasons.append("24시간 누적 강수량 30mm 이상")
        elif rainfall_24h >= 10:
            increase_rate += 0.15
            reasons.append("24시간 누적 강수량 10mm 이상")
    elif horizon_hours <= 48:
        if rainfall_24h >= 50:
            increase_rate += 0.45
            reasons.append("24시간 누적 강수량 50mm 이상")
        elif rainfall_24h >= 30:
            increase_rate += 0.30
            reasons.append("24시간 누적 강수량 30mm 이상")

        if rainfall_48h >= 80:
            increase_rate += 0.40
            reasons.append("48시간 누적 강수량 80mm 이상")
        elif rainfall_48h >= 50:
            increase_rate += 0.25
            reasons.append("48시간 누적 강수량 50mm 이상")
    elif horizon_hours <= 72:
        if rainfall_48h >= 80:
            increase_rate += 0.30
            reasons.append("48시간 누적 강수량 80mm 이상")
        elif rainfall_48h >= 50:
            increase_rate += 0.20
            reasons.append("48시간 누적 강수량 50mm 이상")

        if rainfall_72h >= 120:
            increase_rate += 0.40
            reasons.append("72시간 누적 강수량 120mm 이상")
        elif rainfall_72h >= 80:
            increase_rate += 0.25
            reasons.append("72시간 누적 강수량 80mm 이상")
    else:
        if rainfall_72h >= 120:
            increase_rate += 0.30
            reasons.append("72시간 누적 강수량 120mm 이상")
        elif rainfall_72h >= 80:
            increase_rate += 0.20
            reasons.append("72시간 누적 강수량 80mm 이상")

        if rainfall_120h >= 150:
            increase_rate += 0.30
            reasons.append("5일 누적 강수량 150mm 이상")
        elif rainfall_120h >= 100:
            increase_rate += 0.15
            reasons.append("5일 누적 강수량 100mm 이상")

    # 유입량 대비 방류량 부족 기준
    if expected_inflow >= 1:
        if current_discharge <= 0:
            increase_rate += 0.30
            reasons.append("현재 방류량이 0에 가까움")
        elif expected_inflow >= current_discharge * 3:
            increase_rate += 0.50
            reasons.append("예상 유입량이 현재 방류량의 3배 이상")
        elif expected_inflow >= current_discharge * 2:
            increase_rate += 0.35
            reasons.append("예상 유입량이 현재 방류량의 2배 이상")
        elif expected_inflow > current_discharge:
            increase_rate += 0.20
            reasons.append("예상 유입량이 현재 방류량보다 큼")

    # 너무 작은 유입량/강수량이면 추천 방류량을 과하게 만들지 않음
    if expected_inflow < 1 and rainfall_until_horizon < 10 and storage_rate < 80:
        recommended_discharge = current_discharge
        level = "유지"
        message = "예상 유입량과 강수량이 낮아 현재 방류량 유지가 적절합니다."
        return round(recommended_discharge, 3), round(discharge_gap, 3), level, message

    # 기본 추천 방류량
    if current_discharge > 0:
        recommended_by_current = current_discharge * (1 + increase_rate)
    else:
        recommended_by_current = expected_inflow * min(0.5 + increase_rate, 1.5)

    recommended_by_inflow = expected_inflow * min(0.6 + increase_rate, 1.8)

    recommended_discharge = max(
        current_discharge,
        recommended_by_current,
        recommended_by_inflow
    )

    # 급격한 과대 추천 방지
    if current_discharge > 0:
        max_allowed = current_discharge * 3.0
        recommended_discharge = min(recommended_discharge, max_allowed)

    recommended_discharge = round(max(recommended_discharge, 0.0), 3)

    # 등급 산정
    if increase_rate >= 1.5 or (storage_rate >= 90 and rainfall_until_horizon >= 30):
        level = "강한 사전방류 검토"
    elif increase_rate >= 1.0 or (storage_rate >= 85 and rainfall_until_horizon >= 50):
        level = "단계적 방류 증가 검토"
    elif increase_rate >= 0.5 or (storage_rate >= 80 and rainfall_until_horizon >= 10):
        level = "사전방류 검토"
    elif increase_rate > 0:
        level = "관찰"
    else:
        level = "유지"

    if reasons:
        reason_text = ", ".join(reasons)
        message = (
            f"{reason_text} 조건이 확인되었습니다. "
            f"현재 방류량 {current_discharge:.3f} 대비 "
            f"{recommended_discharge:.3f} 수준의 방류 검토가 필요합니다."
        )
    else:
        message = "강수량, 저수율, 예상 유입량 기준으로 현재 방류량 유지가 적절합니다."

    return recommended_discharge, round(discharge_gap, 3), level, message


# ==================================================
# 8. 결과 저장
# ==================================================

def save_recommendations(result_df):
    if result_df.empty:
        print("[WARNING] 저장할 방류 추천 결과가 없습니다.")
        return

    insert_sql = text("""
        INSERT INTO discharge_recommendation (
            dam_name,
            calculation_time,
            risk_observed_at,
            forecast_horizon_hours,
            forecast_time,

            current_inflow,
            predicted_inflow_6h,
            expected_inflow,

            current_discharge,
            discharge_gap,
            recommended_discharge,

            storage_rate,

            rainfall_6h,
            rainfall_12h,
            rainfall_24h,
            rainfall_48h,
            rainfall_72h,
            rainfall_120h,
            rainfall_until_horizon,

            recommendation_level,
            recommendation_message,
            data_warning
        )
        VALUES (
            :dam_name,
            :calculation_time,
            :risk_observed_at,
            :forecast_horizon_hours,
            :forecast_time,

            :current_inflow,
            :predicted_inflow_6h,
            :expected_inflow,

            :current_discharge,
            :discharge_gap,
            :recommended_discharge,

            :storage_rate,

            :rainfall_6h,
            :rainfall_12h,
            :rainfall_24h,
            :rainfall_48h,
            :rainfall_72h,
            :rainfall_120h,
            :rainfall_until_horizon,

            :recommendation_level,
            :recommendation_message,
            :data_warning
        )
        ON DUPLICATE KEY UPDATE
            risk_observed_at = VALUES(risk_observed_at),
            forecast_time = VALUES(forecast_time),

            current_inflow = VALUES(current_inflow),
            predicted_inflow_6h = VALUES(predicted_inflow_6h),
            expected_inflow = VALUES(expected_inflow),

            current_discharge = VALUES(current_discharge),
            discharge_gap = VALUES(discharge_gap),
            recommended_discharge = VALUES(recommended_discharge),

            storage_rate = VALUES(storage_rate),

            rainfall_6h = VALUES(rainfall_6h),
            rainfall_12h = VALUES(rainfall_12h),
            rainfall_24h = VALUES(rainfall_24h),
            rainfall_48h = VALUES(rainfall_48h),
            rainfall_72h = VALUES(rainfall_72h),
            rainfall_120h = VALUES(rainfall_120h),
            rainfall_until_horizon = VALUES(rainfall_until_horizon),

            recommendation_level = VALUES(recommendation_level),
            recommendation_message = VALUES(recommendation_message),
            data_warning = VALUES(data_warning);
    """)

    db_columns = [
        "dam_name",
        "calculation_time",
        "risk_observed_at",
        "forecast_horizon_hours",
        "forecast_time",
        "current_inflow",
        "predicted_inflow_6h",
        "expected_inflow",
        "current_discharge",
        "discharge_gap",
        "recommended_discharge",
        "storage_rate",
        "rainfall_6h",
        "rainfall_12h",
        "rainfall_24h",
        "rainfall_48h",
        "rainfall_72h",
        "rainfall_120h",
        "rainfall_until_horizon",
        "recommendation_level",
        "recommendation_message",
        "data_warning",
    ]

    db_df = result_df[db_columns].astype(object)
    records = db_df.where(pd.notnull(db_df), None).to_dict("records")

    with engine.begin() as conn:
        conn.execute(insert_sql, records)

    print(f"[SUCCESS] discharge_recommendation 저장 완료: {len(records)}건")


# ==================================================
# 9. 전체 실행
# ==================================================

def calculate_discharge_recommendation():
    print("[STEP 1] discharge_recommendation 테이블 준비")
    create_discharge_recommendation_table()

    print("[STEP 2] dam_risk_score 최신 V3 데이터 조회")
    risk_df = load_latest_risk_score()

    print("[STEP 3] 초단기예보 RN1 조회")
    short_df = load_latest_forecast(
        table_name="weather_forecast_short",
        category="RN1"
    )

    print("[STEP 4] 단기예보 PCP 조회")
    mid_df = load_latest_forecast(
        table_name="weather_forecast_mid",
        category="PCP"
    )

    calculation_time = datetime.now().replace(second=0, microsecond=0)

    print(f"[INFO] 계산 기준 시각: {calculation_time}")
    print(f"[INFO] 위험도 대상 댐 수: {risk_df['dam_name'].nunique()}")
    print(f"[INFO] 초단기예보 RN1 행 수: {len(short_df)}")
    print(f"[INFO] 단기예보 PCP 행 수: {len(mid_df)}")

    print("[DEBUG] risk_df 댐 목록:")
    print(sorted(risk_df["dam_name"].dropna().unique().tolist()))

    if not short_df.empty and "dam_name_std" in short_df.columns:
        print("[DEBUG] short_df 표준 댐 목록:")
        print(sorted(short_df["dam_name_std"].dropna().unique().tolist()))

    if not mid_df.empty and "dam_name_std" in mid_df.columns:
        print("[DEBUG] mid_df 표준 댐 목록:")
        print(sorted(mid_df["dam_name_std"].dropna().unique().tolist()))

    results = []

    for _, row in risk_df.iterrows():
        dam_name = standardize_dam_name(row["dam_name"])

        current_inflow = safe_float(row.get("current_inflow"))
        predicted_inflow_6h = safe_float(row.get("predicted_inflow_6h"))
        current_discharge = safe_float(row.get("current_discharge"))
        storage_rate = safe_float(row.get("storage_rate"))
        risk_observed_at = row.get("observed_at")

        rainfall_reference_time = get_forecast_reference_time(
            dam_name=dam_name,
            calculation_time=calculation_time,
            short_df=short_df,
            mid_df=mid_df
        )

        rainfall_features = build_rainfall_features(
            dam_name=dam_name,
            reference_time=rainfall_reference_time,
            short_df=short_df,
            mid_df=mid_df
        )

        rainfall_6h = rainfall_features["rainfall_6h"]
        rainfall_12h = rainfall_features["rainfall_12h"]
        rainfall_24h = rainfall_features["rainfall_24h"]
        rainfall_48h = rainfall_features["rainfall_48h"]
        rainfall_72h = rainfall_features["rainfall_72h"]
        rainfall_120h = rainfall_features["rainfall_120h"]
        short_reference_time = rainfall_features["short_reference_time"]
        mid_reference_time = rainfall_features["mid_reference_time"]

        data_warnings = []

        if dam_name in LOW_CONFIDENCE_DAMS:
            data_warnings.append("V3 유입량 예측 성능이 낮은 댐이므로 참고용으로 해석 필요")

        has_short = (
            not short_df[short_df["dam_name_std"] == dam_name].empty
            if not short_df.empty and "dam_name_std" in short_df.columns
            else False
        )

        has_mid = (
            not mid_df[mid_df["dam_name_std"] == dam_name].empty
            if not mid_df.empty and "dam_name_std" in mid_df.columns
            else False
        )

        if not has_short:
            data_warnings.append("초단기예보 RN1 데이터 없음")

        if not has_mid:
            data_warnings.append("단기예보 PCP 데이터 없음")

        for horizon in HORIZONS:
            rainfall_until_horizon = get_rainfall_until_horizon(
                rainfall_features,
                horizon
            )

            expected_inflow = estimate_expected_inflow(
                current_inflow=current_inflow,
                predicted_inflow_6h=predicted_inflow_6h,
                rainfall_until_horizon=rainfall_until_horizon,
                horizon_hours=horizon
            )

            recommended_discharge, discharge_gap, level, message = calculate_recommended_discharge(
                current_inflow=current_inflow,
                predicted_inflow_6h=predicted_inflow_6h,
                expected_inflow=expected_inflow,
                current_discharge=current_discharge,
                storage_rate=storage_rate,
                rainfall_6h=rainfall_6h,
                rainfall_12h=rainfall_12h,
                rainfall_24h=rainfall_24h,
                rainfall_48h=rainfall_48h,
                rainfall_72h=rainfall_72h,
                rainfall_120h=rainfall_120h,
                rainfall_until_horizon=rainfall_until_horizon,
                horizon_hours=horizon
            )

            if horizon == 6 and has_short:
                forecast_base_time = short_reference_time
            elif has_mid:
                forecast_base_time = mid_reference_time
            else:
                forecast_base_time = rainfall_reference_time

            forecast_time = forecast_base_time + timedelta(hours=horizon)

            results.append({
                "dam_name": dam_name,
                "calculation_time": calculation_time,
                "risk_observed_at": risk_observed_at,
                "forecast_horizon_hours": horizon,
                "forecast_time": forecast_time,

                "current_inflow": round(current_inflow, 3),
                "predicted_inflow_6h": round(predicted_inflow_6h, 3),
                "expected_inflow": round(expected_inflow, 3),

                "current_discharge": round(current_discharge, 3),
                "discharge_gap": round(discharge_gap, 3),
                "recommended_discharge": round(recommended_discharge, 3),

                "storage_rate": round(storage_rate, 3),

                "rainfall_6h": round(rainfall_6h, 3),
                "rainfall_12h": round(rainfall_12h, 3),
                "rainfall_24h": round(rainfall_24h, 3),
                "rainfall_48h": round(rainfall_48h, 3),
                "rainfall_72h": round(rainfall_72h, 3),
                "rainfall_120h": round(rainfall_120h, 3),
                "rainfall_until_horizon": round(rainfall_until_horizon, 3),

                "recommendation_level": level,
                "recommendation_message": message,
                "data_warning": " / ".join(data_warnings) if data_warnings else None
            })

    result_df = pd.DataFrame(results)

    if result_df.empty:
        print("[WARNING] 방류 추천 결과가 생성되지 않았습니다.")
        return

    result_df["recommendation_priority"] = (
        result_df["recommendation_level"]
        .map(RECOMMENDATION_LEVEL_PRIORITY)
        .fillna(0)
        .astype(int)
    )

    save_recommendations(result_df)

    output_path = OUTPUT_DIR / "discharge_recommendation_latest.csv"
    result_df.drop(columns=["recommendation_priority"]).to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig"
    )

    print("[SUCCESS] CSV 저장 완료:", output_path)

    print("\n" + "=" * 120)
    print("[방류 추천 결과 상위 30건]")
    print("=" * 120)

    show_cols = [
        "dam_name",
        "forecast_horizon_hours",
        "current_inflow",
        "predicted_inflow_6h",
        "expected_inflow",
        "current_discharge",
        "recommended_discharge",
        "storage_rate",
        "rainfall_until_horizon",
        "recommendation_level"
    ]

    print(
        result_df[show_cols]
        .assign(
            recommendation_priority=result_df["recommendation_priority"]
        )
        .sort_values(
            ["recommendation_priority", "recommended_discharge"],
            ascending=[False, False]
        )
        .drop(columns=["recommendation_priority"])
        .head(30)
        .to_string(index=False)
    )

    print("\n[END] 방류 추천 계산 완료")


if __name__ == "__main__":
    print("[START] 방류 추천 계산 시작")
    calculate_discharge_recommendation()
