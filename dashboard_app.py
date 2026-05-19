import os
import sys
import re
import subprocess
from pathlib import Path
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# ============================================================
# 1. Streamlit 기본 설정
# ============================================================

st.set_page_config(
    page_title="AI Dam Management System",
    page_icon="🌊",
    layout="wide"
)

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(BASE_DIR / ".env")

DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "ai_dam_management")

if not DB_PASSWORD:
    st.error(".env 파일에 DB_PASSWORD가 없습니다.")
    st.stop()

encoded_password = quote_plus(DB_PASSWORD)

DATABASE_URL = (
    f"mysql+pymysql://{DB_USER}:{encoded_password}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
)

engine = create_engine(DATABASE_URL)


# ============================================================
# 2. 동기화 실행 파일 목록
# ============================================================

SYNC_SCRIPTS = [
    "insert_dam_data.py",
    "insert_sluice_data.py",
    "weather_short_service.py",
    "weather_mid_service.py",
    "calculate_dam_risk_score.py",
    "calculate_discharge_recommendation.py",
]


# ============================================================
# 3. 제외 댐 설정
# ============================================================

EXCLUDED_DAM_KEYWORDS = {
    "김천부항",
    "김천부항댐",
    "gimcheonbuhang",
    "gimcheonbuhangdam",
}


# ============================================================
# 4. 공통 유틸 함수
# ============================================================

def normalize_name_key(name):
    if name is None:
        return ""

    value = str(name).strip()
    value = " ".join(value.split())
    value = value.replace("（", "(").replace("）", ")")
    value = value.replace("댐", "")
    value = value.replace(" ", "")
    value = value.lower()

    return value


def standardize_dam_name(name):
    if name is None:
        return None

    raw_name = str(name).strip()
    raw_name = " ".join(raw_name.split())
    raw_name = raw_name.replace("（", "(").replace("）", ")")

    aliases = {
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

        # 김천부항은 표준화는 하되, 선택 목록에서는 제외
        "김천부항": "김천부항",
        "김천부항댐": "김천부항",
        "Gimcheon Buhang": "김천부항",
        "gimcheon buhang": "김천부항",
        "gimcheonbuhang": "김천부항",

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
        for key, value in aliases.items()
    }

    key = normalize_name_key(raw_name)

    return alias_by_key.get(key, raw_name)


def is_excluded_dam(name):
    if name is None:
        return True

    std_name = standardize_dam_name(name)
    key = normalize_name_key(std_name)

    if std_name == "김천부항":
        return True

    if "김천부항" in str(std_name):
        return True

    if key in EXCLUDED_DAM_KEYWORDS:
        return True

    return False


def safe_float(value, default=0.0):
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def parse_precipitation(value):
    if value is None:
        return 0.0

    value = str(value).strip()

    if value == "":
        return 0.0

    if "강수없음" in value or "적설없음" in value or "없음" in value:
        return 0.0

    if "1mm 미만" in value or "1cm 미만" in value:
        return 0.5

    numbers = re.findall(r"\d+\.?\d*", value)

    if not numbers:
        return 0.0

    if len(numbers) >= 2:
        return round((float(numbers[0]) + float(numbers[1])) / 2, 3)

    return round(float(numbers[0]), 3)


def clean_date_time_value(value):
    if value is None or pd.isna(value):
        return ""

    value = str(value).strip()

    if value.endswith(".0"):
        value = value[:-2]

    value = re.sub(r"[^0-9]", "", value)

    return value


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


def sky_to_text(value):
    value = str(value).strip()

    mapping = {
        "1": "맑음",
        "3": "구름많음",
        "4": "흐림",
    }

    return mapping.get(value, value)


def pty_to_text(value):
    value = str(value).strip()

    mapping = {
        "0": "없음",
        "1": "비",
        "2": "비/눈",
        "3": "눈",
        "4": "소나기",
    }

    return mapping.get(value, value)


def risk_badge(level):
    level = str(level).strip()

    if level.lower() == "low":
        level = "낮음"
    elif level.lower() == "caution":
        level = "주의"
    elif level.lower() == "warning":
        level = "경계"
    elif level.lower() == "danger":
        level = "위험"

    if level == "낮음":
        return "🟢 낮음"
    if level == "주의":
        return "🟡 주의"
    if level == "경계":
        return "🟠 경계"
    if level == "위험":
        return "🔴 위험"

    return f"⚪ {level}"


def recommendation_badge(level):
    level = str(level).strip()

    if level == "유지":
        return "🟢 유지"
    if level == "관찰":
        return "🟡 관찰"
    if level == "사전방류 검토":
        return "🟠 사전방류 검토"
    if level == "단계적 방류 증가 검토":
        return "🔴 단계적 방류 증가 검토"
    if level == "강한 사전방류 검토":
        return "🚨 강한 사전방류 검토"

    return level


def safe_read_sql(query, params=None):
    try:
        return pd.read_sql(text(query), engine, params=params)
    except Exception:
        return pd.DataFrame()


def table_exists(table_name):
    query = """
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_schema = :db_name
          AND table_name = :table_name;
    """

    df = safe_read_sql(
        query,
        params={
            "db_name": DB_NAME,
            "table_name": table_name,
        }
    )

    if df.empty:
        return False

    return int(df.iloc[0]["cnt"]) > 0


# ============================================================
# 5. 동기화 실행 함수
# ============================================================

def run_python_script(script_name):
    script_path = BASE_DIR / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"{script_name} 파일을 찾을 수 없습니다.")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=1800,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"{script_name} 실행 실패\n\n"
            f"[STDOUT]\n{result.stdout}\n\n"
            f"[STDERR]\n{result.stderr}"
        )

    return result.stdout


def run_sync_pipeline():
    sync_started_at = datetime.now()
    logs = []

    for script in SYNC_SCRIPTS:
        logs.append(f"[START] {script}")

        output = run_python_script(script)

        logs.append(f"[SUCCESS] {script}")

        if output:
            logs.append(output[-3000:])

    sync_finished_at = datetime.now()

    return {
        "sync_started_at": sync_started_at,
        "sync_finished_at": sync_finished_at,
        "logs": logs,
    }


# ============================================================
# 6. DB 조회 함수
# ============================================================

@st.cache_data(ttl=60)
def load_dam_names():
    names = []

    if table_exists("dam_location"):
        df = safe_read_sql("SELECT DISTINCT dam_name FROM dam_location;")
        if not df.empty:
            names.extend(df["dam_name"].dropna().tolist())

    if table_exists("dam_risk_score"):
        df = safe_read_sql("SELECT DISTINCT dam_name FROM dam_risk_score;")
        if not df.empty:
            names.extend(df["dam_name"].dropna().tolist())

    if table_exists("discharge_recommendation"):
        df = safe_read_sql("SELECT DISTINCT dam_name FROM discharge_recommendation;")
        if not df.empty:
            names.extend(df["dam_name"].dropna().tolist())

    names = [standardize_dam_name(name) for name in names]

    names = [
        name for name in names
        if name
        and not is_excluded_dam(name)
    ]

    names = sorted(list(set(names)))

    return names


@st.cache_data(ttl=60)
def load_latest_sync_info():
    query = """
        SELECT
            (SELECT MAX(observed_at) FROM dam_risk_score) AS latest_risk_observed_at,
            (SELECT MAX(calculation_time) FROM discharge_recommendation) AS latest_recommendation_time,
            (SELECT MAX(CONCAT(base_date, base_time)) FROM weather_forecast_short) AS latest_short_base,
            (SELECT MAX(CONCAT(base_date, base_time)) FROM weather_forecast_mid) AS latest_mid_base;
    """

    return safe_read_sql(query)


@st.cache_data(ttl=60)
def load_dam_location():
    if not table_exists("dam_location"):
        return pd.DataFrame()

    df = safe_read_sql("SELECT * FROM dam_location;")

    if df.empty or "dam_name" not in df.columns:
        return pd.DataFrame()

    df["dam_name_std"] = df["dam_name"].apply(standardize_dam_name)
    df = df[~df["dam_name_std"].apply(is_excluded_dam)].copy()

    return df


@st.cache_data(ttl=60)
def load_latest_risk_data():
    if not table_exists("dam_risk_score"):
        return pd.DataFrame()

    query = """
        SELECT r.*
        FROM dam_risk_score r
        INNER JOIN (
            SELECT
                dam_name,
                MAX(observed_at) AS max_observed_at
            FROM dam_risk_score
            GROUP BY dam_name
        ) latest
            ON r.dam_name = latest.dam_name
           AND r.observed_at = latest.max_observed_at
        ORDER BY r.dam_name;
    """

    df = safe_read_sql(query)

    if df.empty:
        return pd.DataFrame()

    df["dam_name_std"] = df["dam_name"].apply(standardize_dam_name)
    df = df[~df["dam_name_std"].apply(is_excluded_dam)].copy()

    return df


@st.cache_data(ttl=60)
def load_latest_recommendation_data():
    if not table_exists("discharge_recommendation"):
        return pd.DataFrame()

    query = """
        SELECT dr.*
        FROM discharge_recommendation dr
        INNER JOIN (
            SELECT
                dam_name,
                MAX(calculation_time) AS max_calculation_time
            FROM discharge_recommendation
            GROUP BY dam_name
        ) latest
            ON dr.dam_name = latest.dam_name
           AND dr.calculation_time = latest.max_calculation_time
        ORDER BY dr.dam_name, dr.forecast_horizon_hours;
    """

    df = safe_read_sql(query)

    if df.empty:
        return pd.DataFrame()

    df["dam_name_std"] = df["dam_name"].apply(standardize_dam_name)
    df = df[~df["dam_name_std"].apply(is_excluded_dam)].copy()

    return df


@st.cache_data(ttl=60)
def load_latest_observation_data():
    frames = []

    if table_exists("dam_observation"):
        query = """
            SELECT
                dam_name,
                observed_at,
                inflow,
                water_level,
                rainfall,
                storage_amount,
                storage_rate,
                discharge
            FROM dam_observation;
        """
        df = safe_read_sql(query)
        if not df.empty:
            df["source"] = "dam_observation"
            frames.append(df)

    if table_exists("sluice_observation"):
        query = """
            SELECT
                dam_name,
                observed_at,
                inflow,
                water_level,
                rainfall,
                storage_amount,
                storage_rate,
                discharge
            FROM sluice_observation;
        """
        df = safe_read_sql(query)
        if not df.empty:
            df["source"] = "sluice_observation"
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result["dam_name_std"] = result["dam_name"].apply(standardize_dam_name)
    result = result[~result["dam_name_std"].apply(is_excluded_dam)].copy()

    result["observed_at"] = pd.to_datetime(result["observed_at"], errors="coerce")

    result = (
        result
        .sort_values(["dam_name_std", "observed_at"])
        .groupby("dam_name_std")
        .tail(1)
        .reset_index(drop=True)
    )

    return result


@st.cache_data(ttl=60)
def load_weather_mid_data():
    if not table_exists("weather_forecast_mid"):
        return pd.DataFrame()

    query = """
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
        FROM weather_forecast_mid
        WHERE category IN ('PCP', 'SNO', 'TMP', 'TMN', 'TMX', 'SKY', 'PTY', 'REH', 'WSD')
        ORDER BY region_name, base_date, base_time, forecast_date, forecast_time;
    """

    df = safe_read_sql(query)

    if df.empty:
        return pd.DataFrame()

    df["dam_name_std"] = df["region_name"].apply(standardize_dam_name)
    df = df[~df["dam_name_std"].apply(is_excluded_dam)].copy()

    df["forecast_datetime"] = df.apply(
        lambda row: combine_datetime(row["forecast_date"], row["forecast_time"]),
        axis=1
    )
    df["base_datetime"] = df.apply(
        lambda row: combine_datetime(row["base_date"], row["base_time"]),
        axis=1
    )

    df = df.dropna(subset=["forecast_datetime", "base_datetime"]).copy()

    if df.empty:
        return pd.DataFrame()

    df["base_key"] = df["base_datetime"].dt.strftime("%Y%m%d%H%M")

    latest_base_by_dam = df.groupby("dam_name_std")["base_key"].transform("max")
    df = df[df["base_key"] == latest_base_by_dam].copy()

    return df


@st.cache_data(ttl=60)
def load_weather_short_data():
    if not table_exists("weather_forecast_short"):
        return pd.DataFrame()

    query = """
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
        FROM weather_forecast_short
        WHERE category IN ('RN1', 'T1H', 'SKY', 'PTY', 'REH', 'WSD')
        ORDER BY region_name, base_date, base_time, forecast_date, forecast_time;
    """

    df = safe_read_sql(query)

    if df.empty:
        return pd.DataFrame()

    df["dam_name_std"] = df["region_name"].apply(standardize_dam_name)
    df = df[~df["dam_name_std"].apply(is_excluded_dam)].copy()

    df["forecast_datetime"] = df.apply(
        lambda row: combine_datetime(row["forecast_date"], row["forecast_time"]),
        axis=1
    )
    df["base_datetime"] = df.apply(
        lambda row: combine_datetime(row["base_date"], row["base_time"]),
        axis=1
    )

    df = df.dropna(subset=["forecast_datetime", "base_datetime"]).copy()

    if df.empty:
        return pd.DataFrame()

    df["base_key"] = df["base_datetime"].dt.strftime("%Y%m%d%H%M")

    latest_base_by_dam = df.groupby("dam_name_std")["base_key"].transform("max")
    df = df[df["base_key"] == latest_base_by_dam].copy()

    return df


@st.cache_data(ttl=60)
def load_downstream_data():
    candidate_tables = [
        "downstream_water_level",
        "downstream_observation",
        "water_level_observation",
    ]

    for table_name in candidate_tables:
        if table_exists(table_name):
            df = safe_read_sql(f"SELECT * FROM {table_name};")
            if not df.empty:
                if "dam_name" in df.columns:
                    df["dam_name_std"] = df["dam_name"].apply(standardize_dam_name)
                    df = df[~df["dam_name_std"].apply(is_excluded_dam)].copy()
                return df

    return pd.DataFrame()


# ============================================================
# 7. 데이터 가공 함수
# ============================================================

def get_row_by_dam(df, dam_name):
    if df.empty or "dam_name_std" not in df.columns:
        return None

    sub_df = df[df["dam_name_std"] == dam_name].copy()

    if sub_df.empty:
        return None

    return sub_df.iloc[0]


def get_recommendation_by_dam(df, dam_name):
    if df.empty or "dam_name_std" not in df.columns:
        return pd.DataFrame()

    sub_df = df[df["dam_name_std"] == dam_name].copy()

    if sub_df.empty:
        return pd.DataFrame()

    sub_df = sub_df.sort_values("forecast_horizon_hours")

    return sub_df


def get_weather_by_dam(df, dam_name):
    if df.empty or "dam_name_std" not in df.columns:
        return pd.DataFrame()

    sub_df = df[df["dam_name_std"] == dam_name].copy()

    if sub_df.empty:
        return pd.DataFrame()

    return sub_df


def build_daily_weather_table(mid_df):
    if mid_df.empty:
        return pd.DataFrame()

    df = mid_df.copy()
    df["forecast_date_only"] = df["forecast_datetime"].dt.date

    result_rows = []

    for date_value, group in df.groupby("forecast_date_only"):
        pcp = group[group["category"] == "PCP"]["forecast_value"].apply(parse_precipitation).sum()
        sno = group[group["category"] == "SNO"]["forecast_value"].apply(parse_precipitation).sum()

        tmp_values = pd.to_numeric(
            group[group["category"] == "TMP"]["forecast_value"],
            errors="coerce"
        ).dropna()

        tmn_values = pd.to_numeric(
            group[group["category"] == "TMN"]["forecast_value"],
            errors="coerce"
        ).dropna()

        tmx_values = pd.to_numeric(
            group[group["category"] == "TMX"]["forecast_value"],
            errors="coerce"
        ).dropna()

        sky_values = group[group["category"] == "SKY"]["forecast_value"].dropna().astype(str).tolist()
        pty_values = group[group["category"] == "PTY"]["forecast_value"].dropna().astype(str).tolist()

        sky_text = sky_to_text(sky_values[0]) if sky_values else "-"
        pty_text = pty_to_text(pty_values[0]) if pty_values else "-"

        if not tmn_values.empty:
            min_temp = round(float(tmn_values.min()), 1)
        elif not tmp_values.empty:
            min_temp = round(float(tmp_values.min()), 1)
        else:
            min_temp = None

        if not tmx_values.empty:
            max_temp = round(float(tmx_values.max()), 1)
        elif not tmp_values.empty:
            max_temp = round(float(tmp_values.max()), 1)
        else:
            max_temp = None

        result_rows.append({
            "날짜": str(date_value),
            "예상 강수량(mm)": round(float(pcp), 3),
            "예상 적설량": round(float(sno), 3),
            "최저기온": min_temp,
            "최고기온": max_temp,
            "하늘상태": sky_text,
            "강수형태": pty_text,
        })

    return pd.DataFrame(result_rows)


def build_current_weather(short_df, mid_df):
    if not short_df.empty:
        df = short_df.copy()
        df = df.sort_values("forecast_datetime")

        pivot = (
            df.pivot_table(
                index="forecast_datetime",
                columns="category",
                values="forecast_value",
                aggfunc="first"
            )
            .reset_index()
            .sort_values("forecast_datetime")
        )

        if not pivot.empty:
            row = pivot.iloc[0]

            return {
                "기준": "초단기예보",
                "예보시각": row.get("forecast_datetime"),
                "기온": row.get("T1H", row.get("TMP", "-")),
                "강수량": row.get("RN1", "-"),
                "하늘상태": sky_to_text(row.get("SKY", "-")),
                "강수형태": pty_to_text(row.get("PTY", "-")),
                "습도": row.get("REH", "-"),
                "풍속": row.get("WSD", "-"),
            }

    if not mid_df.empty:
        df = mid_df.copy()
        df = df.sort_values("forecast_datetime")

        pivot = (
            df.pivot_table(
                index="forecast_datetime",
                columns="category",
                values="forecast_value",
                aggfunc="first"
            )
            .reset_index()
            .sort_values("forecast_datetime")
        )

        if not pivot.empty:
            row = pivot.iloc[0]

            return {
                "기준": "단기예보",
                "예보시각": row.get("forecast_datetime"),
                "기온": row.get("TMP", "-"),
                "강수량": row.get("PCP", "-"),
                "하늘상태": sky_to_text(row.get("SKY", "-")),
                "강수형태": pty_to_text(row.get("PTY", "-")),
                "습도": row.get("REH", "-"),
                "풍속": row.get("WSD", "-"),
            }

    return None


# ============================================================
# 8. 화면 시작
# ============================================================

st.title("🌊 AI Dam Management System")
st.caption("김천부항댐 제외 20개 댐 기준 / V3 유입량 예측 · 위험도 계산 · AI 방류 추천 대시보드")


# ============================================================
# 9. 사이드바: 동기화 및 댐 선택
# ============================================================

st.sidebar.header("데이터 동기화")

st.sidebar.caption(
    "동기화 버튼을 누르면 최신 댐 데이터, 기상예보, 위험도, 방류 추천을 순서대로 갱신합니다."
)

if st.sidebar.button("🔄 현재 시각 기준 동기화 실행", width="stretch"):
    try:
        with st.status("데이터 동기화 중입니다...", expanded=True) as status:
            sync_result = run_sync_pipeline()

            for log in sync_result["logs"]:
                if log:
                    st.write(log)

            st.session_state["last_sync_started_at"] = sync_result["sync_started_at"]
            st.session_state["last_sync_finished_at"] = sync_result["sync_finished_at"]

            status.update(
                label="동기화 완료",
                state="complete",
                expanded=False
            )

        st.cache_data.clear()
        st.rerun()

    except Exception as e:
        st.error("동기화 중 오류가 발생했습니다.")
        st.exception(e)


if "last_sync_started_at" in st.session_state:
    st.sidebar.write("최근 동기화 시작")
    st.sidebar.code(st.session_state["last_sync_started_at"].strftime("%Y-%m-%d %H:%M:%S"))

if "last_sync_finished_at" in st.session_state:
    st.sidebar.write("최근 동기화 완료")
    st.sidebar.code(st.session_state["last_sync_finished_at"].strftime("%Y-%m-%d %H:%M:%S"))


st.sidebar.markdown("---")
st.sidebar.header("댐 선택")

dam_names = load_dam_names()

if not dam_names:
    st.error("조회 가능한 댐 목록이 없습니다. DB 데이터를 먼저 확인하세요.")
    st.stop()

selected_dam = st.sidebar.selectbox(
    "댐 이름",
    dam_names,
    index=0
)

st.sidebar.markdown("---")
st.sidebar.caption("김천부항댐 제외 20개 댐 기준")


# ============================================================
# 10. 데이터 로드
# ============================================================

location_df = load_dam_location()
risk_df = load_latest_risk_data()
recommendation_df = load_latest_recommendation_data()
observation_df = load_latest_observation_data()
weather_mid_df = load_weather_mid_data()
weather_short_df = load_weather_short_data()
downstream_df = load_downstream_data()
sync_info_df = load_latest_sync_info()

selected_location = get_row_by_dam(location_df, selected_dam)
selected_risk = get_row_by_dam(risk_df, selected_dam)
selected_observation = get_row_by_dam(observation_df, selected_dam)
selected_recommendations = get_recommendation_by_dam(recommendation_df, selected_dam)
selected_mid_weather = get_weather_by_dam(weather_mid_df, selected_dam)
selected_short_weather = get_weather_by_dam(weather_short_df, selected_dam)


# ============================================================
# 11. 상단 정보
# ============================================================

st.header(f"{selected_dam} 댐 상세 현황")

now = datetime.now()

top_col1, top_col2, top_col3, top_col4 = st.columns(4)

with top_col1:
    st.metric("현재 시간", now.strftime("%Y-%m-%d %H:%M"))

with top_col2:
    if selected_risk is not None:
        st.metric("최신 수문 관측 시각", str(selected_risk.get("observed_at", "-")))
    else:
        st.metric("최신 수문 관측 시각", "-")

with top_col3:
    if not selected_recommendations.empty:
        latest_calc = selected_recommendations["calculation_time"].max()
        st.metric("방류 추천 계산 시각", str(latest_calc))
    else:
        st.metric("방류 추천 계산 시각", "-")

with top_col4:
    if not sync_info_df.empty:
        st.metric("DB 최신 추천 시각", str(sync_info_df.iloc[0].get("latest_recommendation_time", "-")))
    else:
        st.metric("DB 최신 추천 시각", "-")


# ============================================================
# 12. 댐 기본 정보
# ============================================================

with st.expander("댐 기본 정보 / 위치 정보", expanded=True):
    info_col1, info_col2, info_col3, info_col4 = st.columns(4)

    if selected_location is not None:
        location_dict = selected_location.to_dict()

        latitude = location_dict.get("latitude", location_dict.get("lat", "-"))
        longitude = location_dict.get("longitude", location_dict.get("lon", "-"))
        nx = location_dict.get("nx", "-")
        ny = location_dict.get("ny", "-")

        address = (
            location_dict.get("address")
            or location_dict.get("dam_address")
            or location_dict.get("location")
            or "주소 컬럼 없음"
        )

        with info_col1:
            st.metric("댐 이름", selected_dam)

        with info_col2:
            st.metric("위도", latitude)

        with info_col3:
            st.metric("경도", longitude)

        with info_col4:
            st.metric("기상청 격자", f"{nx}, {ny}")

        st.write(f"주소: {address}")

    else:
        st.warning("dam_location 테이블에서 해당 댐의 위치 정보를 찾지 못했습니다.")


# ============================================================
# 13. 핵심 지표 카드
# ============================================================

st.subheader("핵심 현황")

metric_col1, metric_col2, metric_col3, metric_col4, metric_col5, metric_col6 = st.columns(6)

current_inflow = "-"
current_discharge = "-"
storage_rate = "-"
water_level = "-"
risk_score = "-"
risk_level = "-"

if selected_risk is not None:
    current_inflow = safe_float(selected_risk.get("current_inflow"))
    current_discharge = safe_float(selected_risk.get("current_discharge"))
    storage_rate = safe_float(selected_risk.get("storage_rate"))
    water_level = safe_float(selected_risk.get("water_level"))
    risk_score = safe_float(selected_risk.get("risk_score"))
    risk_level = selected_risk.get("risk_level", "-")
elif selected_observation is not None:
    current_inflow = safe_float(selected_observation.get("inflow"))
    current_discharge = safe_float(selected_observation.get("discharge"))
    storage_rate = safe_float(selected_observation.get("storage_rate"))
    water_level = safe_float(selected_observation.get("water_level"))

with metric_col1:
    st.metric("현재 유입량", f"{current_inflow:.3f}" if isinstance(current_inflow, float) else "-")

with metric_col2:
    st.metric("현재 방류량", f"{current_discharge:.3f}" if isinstance(current_discharge, float) else "-")

with metric_col3:
    st.metric("저수율", f"{storage_rate:.2f}%" if isinstance(storage_rate, float) else "-")

with metric_col4:
    st.metric("수위", f"{water_level:.3f}" if isinstance(water_level, float) else "-")

with metric_col5:
    st.metric("위험도 점수", f"{risk_score:.1f}" if isinstance(risk_score, float) else "-")

with metric_col6:
    st.metric("위험도 등급", risk_badge(risk_level))


# ============================================================
# 14. 탭 구성
# ============================================================

tab_summary, tab_weather, tab_hydrology, tab_risk, tab_recommendation, tab_downstream = st.tabs([
    "요약",
    "기상예보",
    "수문 운영 정보",
    "위험도 분석",
    "AI 방류 추천",
    "하류 수위"
])


# ============================================================
# 15. 요약 탭
# ============================================================

with tab_summary:
    st.subheader("댐 상태 요약")

    summary_col1, summary_col2 = st.columns([1, 1])

    with summary_col1:
        st.write("### 현재 기상 상태")

        current_weather = build_current_weather(selected_short_weather, selected_mid_weather)

        if current_weather:
            st.write(f"기준: {current_weather['기준']}")
            st.write(f"예보시각: {current_weather['예보시각']}")
            st.write(f"기온: {current_weather['기온']}")
            st.write(f"강수량: {current_weather['강수량']}")
            st.write(f"하늘상태: {current_weather['하늘상태']}")
            st.write(f"강수형태: {current_weather['강수형태']}")
            st.write(f"습도: {current_weather['습도']}")
            st.write(f"풍속: {current_weather['풍속']}")
        else:
            st.info("현재 기상 정보를 찾지 못했습니다.")

    with summary_col2:
        st.write("### AI 판단 요약")

        if selected_risk is not None:
            st.write(f"위험도 등급: {risk_badge(selected_risk.get('risk_level', '-'))}")
            st.write(f"위험도 점수: {safe_float(selected_risk.get('risk_score')):.1f}")
            st.write(f"V3 예측 6시간 뒤 유입량: {safe_float(selected_risk.get('predicted_inflow_6h')):.3f}")
            st.info(str(selected_risk.get("risk_message", "")))
        else:
            st.warning("위험도 데이터가 없습니다.")

        if not selected_recommendations.empty:
            first_rec = selected_recommendations.sort_values("forecast_horizon_hours").iloc[0]
            st.write(f"6시간 추천 등급: {recommendation_badge(first_rec.get('recommendation_level', '-'))}")
            st.write(f"6시간 추천 방류량: {safe_float(first_rec.get('recommended_discharge')):.3f}")
            st.info(str(first_rec.get("recommendation_message", "")))
        else:
            st.warning("방류 추천 데이터가 없습니다.")


# ============================================================
# 16. 기상예보 탭
# ============================================================

with tab_weather:
    st.subheader("향후 5일 기상예보")

    if selected_mid_weather.empty and selected_short_weather.empty:
        st.warning("기상예보 데이터가 없습니다.")
    else:
        if not selected_short_weather.empty:
            st.write("### 초단기예보")
            short_display = selected_short_weather.copy()
            short_display = short_display.sort_values(["forecast_datetime", "category"])

            st.dataframe(
                short_display[[
                    "base_datetime",
                    "forecast_datetime",
                    "category",
                    "forecast_value"
                ]],
                width="stretch",
                hide_index=True
            )

        if not selected_mid_weather.empty:
            st.write("### 단기예보 일별 요약")

            daily_weather = build_daily_weather_table(selected_mid_weather)

            if not daily_weather.empty:
                st.dataframe(
                    daily_weather,
                    width="stretch",
                    hide_index=True
                )

            st.write("### 단기예보 원본")

            mid_display = selected_mid_weather.copy()
            mid_display = mid_display.sort_values(["forecast_datetime", "category"])

            st.dataframe(
                mid_display[[
                    "base_datetime",
                    "forecast_datetime",
                    "category",
                    "forecast_value"
                ]],
                width="stretch",
                hide_index=True
            )


# ============================================================
# 17. 수문 운영 정보 탭
# ============================================================

with tab_hydrology:
    st.subheader("현재 수문 운영 정보")

    if selected_observation is not None:
        observation_display = pd.DataFrame([selected_observation.to_dict()])
        st.dataframe(
            observation_display,
            width="stretch",
            hide_index=True
        )
    else:
        st.warning("dam_observation 또는 sluice_observation에서 최신 수문 데이터를 찾지 못했습니다.")

    st.write("### 위험도 계산에 사용된 수문 값")

    if selected_risk is not None:
        risk_hydro_cols = [
            "dam_name",
            "observed_at",
            "current_inflow",
            "predicted_inflow_6h",
            "current_discharge",
            "water_level",
            "water_level_diff_6h",
            "storage_rate",
            "hydrology_rainfall_24h",
            "kma_rainfall_24h",
            "source_table",
        ]

        available_cols = [col for col in risk_hydro_cols if col in selected_risk.index]

        st.dataframe(
            pd.DataFrame([selected_risk[available_cols].to_dict()]),
            width="stretch",
            hide_index=True
        )
    else:
        st.warning("위험도 계산 데이터가 없습니다.")


# ============================================================
# 18. 위험도 분석 탭
# ============================================================

with tab_risk:
    st.subheader("위험도 분석 결과")

    if selected_risk is None:
        st.warning("위험도 데이터가 없습니다. calculate_dam_risk_score.py를 먼저 실행하세요.")
    else:
        r1, r2, r3, r4 = st.columns(4)

        with r1:
            st.metric("위험도 점수", f"{safe_float(selected_risk.get('risk_score')):.1f}")

        with r2:
            st.metric("위험도 등급", risk_badge(selected_risk.get("risk_level", "-")))

        with r3:
            st.metric("예측 유입량 6h", f"{safe_float(selected_risk.get('predicted_inflow_6h')):.3f}")

        with r4:
            st.metric("수위 변화 6h", f"{safe_float(selected_risk.get('water_level_diff_6h')):.3f}")

        st.info(str(selected_risk.get("risk_message", "")))

        score_cols = [
            "inflow_score",
            "storage_score",
            "rainfall_score",
            "discharge_balance_score",
            "water_level_trend_score",
            "risk_score",
        ]

        available_score_cols = [col for col in score_cols if col in selected_risk.index]

        st.write("### 위험도 세부 점수")

        st.dataframe(
            pd.DataFrame([selected_risk[available_score_cols].to_dict()]),
            width="stretch",
            hide_index=True
        )


# ============================================================
# 19. AI 방류 추천 탭
# ============================================================

with tab_recommendation:
    st.subheader("AI 방류 추천 결과")

    if selected_recommendations.empty:
        st.warning("방류 추천 데이터가 없습니다. calculate_discharge_recommendation.py를 먼저 실행하세요.")
    else:
        display_df = selected_recommendations.copy()

        display_df["추천 등급"] = display_df["recommendation_level"].apply(recommendation_badge)

        show_cols = [
            "forecast_horizon_hours",
            "forecast_time",
            "current_inflow",
            "predicted_inflow_6h",
            "expected_inflow",
            "current_discharge",
            "discharge_gap",
            "recommended_discharge",
            "storage_rate",
            "rainfall_until_horizon",
            "추천 등급",
            "recommendation_message",
            "data_warning",
        ]

        available_cols = [col for col in show_cols if col in display_df.columns]

        display_df = display_df[available_cols].rename(columns={
            "forecast_horizon_hours": "예측 구간(h)",
            "forecast_time": "예측 시각",
            "current_inflow": "현재 유입량",
            "predicted_inflow_6h": "V3 예측 유입량 6h",
            "expected_inflow": "예상 유입량",
            "current_discharge": "현재 방류량",
            "discharge_gap": "방류 부족량",
            "recommended_discharge": "AI 추천 방류량",
            "storage_rate": "저수율",
            "rainfall_until_horizon": "누적 강수량",
            "recommendation_message": "추천 메시지",
            "data_warning": "주의사항",
        })

        st.dataframe(
            display_df,
            width="stretch",
            hide_index=True
        )

        st.write("### 추천 방류량 변화")

        chart_df = selected_recommendations.copy()
        chart_df = chart_df.sort_values("forecast_horizon_hours")
        chart_df = chart_df.set_index("forecast_horizon_hours")

        chart_cols = [
            "current_discharge",
            "recommended_discharge",
            "expected_inflow",
        ]

        available_chart_cols = [col for col in chart_cols if col in chart_df.columns]

        if available_chart_cols:
            st.line_chart(chart_df[available_chart_cols])


# ============================================================
# 20. 하류 수위 탭
# ============================================================

with tab_downstream:
    st.subheader("하류 수위관측소 정보")

    if downstream_df.empty:
        st.info(
            "하류 수위관측소 API는 아직 연동 전입니다. "
            "향후 하류 수위, 하류 수위 상승 추세, 위험 수위 정보를 추가할 예정입니다."
        )
    else:
        if "dam_name_std" in downstream_df.columns:
            sub_downstream = downstream_df[downstream_df["dam_name_std"] == selected_dam].copy()
        else:
            sub_downstream = downstream_df.copy()

        if sub_downstream.empty:
            st.warning("선택한 댐과 연결된 하류 수위 데이터가 없습니다.")
        else:
            st.dataframe(
                sub_downstream,
                width="stretch",
                hide_index=True
            )


# ============================================================
# 21. 하단 설명
# ============================================================

st.markdown("---")
st.caption(
    "본 대시보드는 AI 기반 댐 관리 시스템 MVP입니다. "
    "김천부항댐을 제외한 20개 댐을 기준으로 V3 유입량 예측 모델, 위험도 계산 결과, "
    "기상청 예보, AI 방류 추천 결과를 통합하여 표시합니다. "
    "방류 추천은 실제 방류 명령이 아니라 관리자 의사결정을 보조하기 위한 참고 지표입니다."
)