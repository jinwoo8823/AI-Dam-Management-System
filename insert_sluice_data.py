import os
import re
from datetime import datetime
from urllib.parse import quote_plus

import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from target_dams import filter_target_dams


# ==================================================
# 1. .env 파일 불러오기
# ==================================================
load_dotenv()


def get_required_env(key: str) -> str:
    value = os.getenv(key)

    if value is None or value.strip() == "":
        raise ValueError(f".env 파일에 {key} 값이 없습니다.")

    return value


API_KEY = get_required_env("API_KEY")

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
# 3. 공통 함수
# ==================================================
def to_float(value):
    if value is None:
        return None

    value = str(value).strip()

    if value == "":
        return None

    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None


def normalize_items(items):
    if not items:
        return []

    if isinstance(items, dict):
        return [items]

    return items


def parse_obsrdt(obsrdt: str, base_year: int):
    """
    K-water 수문 운영 정보 obsrdt 파싱

    예:
    "05-12 14시" -> 2026-05-12 14:00:00
    """
    if obsrdt is None:
        return None

    text_value = str(obsrdt).strip()
    numbers = re.findall(r"\d+", text_value)

    if len(numbers) < 3:
        return None

    month = int(numbers[0])
    day = int(numbers[1])
    hour = int(numbers[2])

    return datetime(base_year, month, day, hour, 0, 0)


# ==================================================
# 4. 댐코드 목록 조회
# ==================================================
def fetch_dam_codes() -> pd.DataFrame:
    url = "http://apis.data.go.kr/B500001/dam/damCode/damCodelist"

    params = {
        "serviceKey": API_KEY,
        "_type": "json",
    }

    safe_params = params.copy()
    safe_params["serviceKey"] = "********"

    print("[DAM CODE REQUEST PARAMS]", safe_params)

    response = requests.get(url, params=params, timeout=30)
    print("[DAM CODE STATUS]", response.status_code)

    response.raise_for_status()

    data = response.json()

    header = data.get("response", {}).get("header", {})
    print("[DAM CODE API RESULT]", header.get("resultCode"), header.get("resultMsg"))

    body = data.get("response", {}).get("body", {})
    items = body.get("items", {}).get("item", [])
    items = normalize_items(items)

    rows = []

    for item in items:
        rows.append({
            "dam_code": str(item.get("damcode")).strip(),
            "dam_name": str(item.get("damnm")).strip(),
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print("[WARNING] dam_code API에서 받은 데이터가 없습니다.")
        return df

    df = df.dropna(subset=["dam_code", "dam_name"])
    df = df.drop_duplicates(subset=["dam_code"])

    # 김천부항 제외 20개 댐 코드만 남김
    df = filter_target_dams(df, dam_col="dam_name", standardize=True)

    print(f"[TARGET DAM CODE COUNT] {len(df)}개")
    print(df.to_string(index=False))

    return df


# ==================================================
# 5. dam_code 저장/갱신
# ==================================================
def save_dam_codes(df: pd.DataFrame):
    if df.empty:
        print("[SKIP] 저장할 대상 댐코드가 없습니다.")
        return

    sql = text("""
        INSERT INTO dam_code (
            dam_code,
            dam_name
        )
        VALUES (
            :dam_code,
            :dam_name
        )
        ON DUPLICATE KEY UPDATE
            dam_name = VALUES(dam_name),
            updated_at = CURRENT_TIMESTAMP
    """)

    records = df.where(pd.notnull(df), None).to_dict("records")

    with engine.begin() as conn:
        conn.execute(sql, records)

    print(f"[SUCCESS] dam_code 저장/갱신 완료: {len(records)}건")


# ==================================================
# 6. 특정 댐코드의 시간별 수문 운영 정보 조회
# ==================================================
def fetch_sluice_hour_data(
    dam_code: str,
    dam_name: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    url = "http://apis.data.go.kr/B500001/dam/sluicePresentCondition/hourlist"

    params = {
        "serviceKey": API_KEY,
        "_type": "json",
        "pageNo": 1,
        "numOfRows": 1000,
        "damcode": dam_code,
        "stdt": start_date,
        "eddt": end_date,
    }

    safe_params = params.copy()
    safe_params["serviceKey"] = "********"

    print(f"[SLUICE REQUEST] {dam_name} / {dam_code}")
    print(safe_params)

    response = requests.get(url, params=params, timeout=30)
    print("[SLUICE STATUS]", response.status_code)

    response.raise_for_status()

    data = response.json()

    header = data.get("response", {}).get("header", {})
    print("[SLUICE API RESULT]", header.get("resultCode"), header.get("resultMsg"))

    body = data.get("response", {}).get("body", {})
    items = body.get("items", {}).get("item", [])
    items = normalize_items(items)

    if not items:
        print(f"[WARNING] {dam_name} 수문 운영 데이터 없음")
        return pd.DataFrame()

    base_year = datetime.strptime(start_date, "%Y-%m-%d").year

    rows = []

    for item in items:
        observed_at = parse_obsrdt(item.get("obsrdt"), base_year)

        if observed_at is None:
            continue

        rows.append({
            "dam_code": dam_code,
            "dam_name": dam_name,
            "observed_at": observed_at,
            "water_level": to_float(item.get("lowlevel")),
            "rainfall": to_float(item.get("rf")),
            "inflow": to_float(item.get("inflowqy")),
            "discharge": to_float(item.get("totdcwtrqy")),
            "storage_amount": to_float(item.get("rsvwtqy")),
            "storage_rate": to_float(item.get("rsvwtrt")),
        })

    df = pd.DataFrame(rows)

    if not df.empty:
        df = filter_target_dams(df, dam_col="dam_name", standardize=True)

    return df


# ==================================================
# 7. sluice_observation 저장/갱신
# ==================================================
def save_sluice_data(df: pd.DataFrame):
    if df.empty:
        print("[SKIP] 저장할 수문 운영 데이터가 없습니다.")
        return

    sql = text("""
        INSERT INTO sluice_observation (
            dam_code,
            dam_name,
            observed_at,
            water_level,
            rainfall,
            inflow,
            discharge,
            storage_amount,
            storage_rate
        )
        VALUES (
            :dam_code,
            :dam_name,
            :observed_at,
            :water_level,
            :rainfall,
            :inflow,
            :discharge,
            :storage_amount,
            :storage_rate
        )
        ON DUPLICATE KEY UPDATE
            dam_name = VALUES(dam_name),
            water_level = VALUES(water_level),
            rainfall = VALUES(rainfall),
            inflow = VALUES(inflow),
            discharge = VALUES(discharge),
            storage_amount = VALUES(storage_amount),
            storage_rate = VALUES(storage_rate),
            updated_at = CURRENT_TIMESTAMP
    """)

    records = df.where(pd.notnull(df), None).to_dict("records")

    with engine.begin() as conn:
        conn.execute(sql, records)

    print(f"[SUCCESS] sluice_observation 저장/갱신 완료: {len(records)}건")


# ==================================================
# 8. 전체 수문 운영 정보 수집
# ==================================================
def collect_sluice_data():
    today = datetime.now().strftime("%Y-%m-%d")

    start_date = today
    end_date = today

    print("[TARGET DATE]", start_date, "~", end_date)

    dam_code_df = fetch_dam_codes()
    save_dam_codes(dam_code_df)

    if dam_code_df.empty:
        print("[ERROR] 대상 20개 댐코드 목록이 비어 있습니다.")
        return

    all_dataframes = []

    for _, row in dam_code_df.iterrows():
        dam_code = row["dam_code"]
        dam_name = row["dam_name"]

        df = fetch_sluice_hour_data(
            dam_code=dam_code,
            dam_name=dam_name,
            start_date=start_date,
            end_date=end_date
        )

        if not df.empty:
            all_dataframes.append(df)

    if not all_dataframes:
        print("[WARNING] 전체 수문 운영 데이터가 비어 있습니다.")
        return

    result_df = pd.concat(all_dataframes, ignore_index=True)

    # 최종 저장 전에도 20개 댐만 한 번 더 필터링
    result_df = filter_target_dams(result_df, dam_col="dam_name", standardize=True)

    print("[TOTAL ROW COUNT]", len(result_df))
    print("[TARGET DAM COUNT]", result_df["dam_name"].nunique())
    print("[DATA PREVIEW - TARGET 20 DAMS]")
    print(result_df.to_string(index=False))

    save_sluice_data(result_df)


# ==================================================
# 9. 실행
# ==================================================
if __name__ == "__main__":
    print("[START] K-water 수문 운영 정보 수집 시작")

    collect_sluice_data()

    print("[END] K-water 수문 운영 정보 수집 종료")