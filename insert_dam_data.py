import os
from datetime import datetime, timedelta
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
# 3. 숫자 변환 함수
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


# ==================================================
# 4. 특정 시각 기준 다목적댐 운영 정보 조회
# ==================================================
def request_dam_data_by_time(target_datetime: datetime) -> pd.DataFrame:
    url = "http://apis.data.go.kr/B500001/dam/multipurPoseDam/multipurPoseDamlist"

    vdate = target_datetime.strftime("%Y-%m-%d")
    vtime = target_datetime.strftime("%H")

    tdate = (target_datetime - timedelta(days=1)).strftime("%Y-%m-%d")
    ldate = (target_datetime - timedelta(days=365)).strftime("%Y-%m-%d")

    params = {
        "serviceKey": API_KEY,
        "_type": "json",
        "pageNo": 1,
        "numOfRows": 100,
        "tdate": tdate,
        "ldate": ldate,
        "vdate": vdate,
        "vtime": vtime,
    }

    safe_params = params.copy()
    safe_params["serviceKey"] = "********"

    print("[TRY TARGET DATETIME]", target_datetime)
    print("[REQUEST PARAMS]", safe_params)

    response = requests.get(url, params=params, timeout=30)
    print("[STATUS CODE]", response.status_code)

    response.raise_for_status()

    data = response.json()

    header = data.get("response", {}).get("header", {})
    print("[API RESULT]", header.get("resultCode"), header.get("resultMsg"))

    body = data.get("response", {}).get("body", {})
    items = body.get("items", {}).get("item", [])

    if not items:
        return pd.DataFrame()

    if isinstance(items, dict):
        items = [items]

    rows = []

    for item in items:
        rows.append({
            "dam_name": item.get("damnm"),
            "river_system": item.get("suge"),
            "observed_at": target_datetime,
            "water_level": to_float(item.get("nowlowlevel")),
            "storage_amount": to_float(item.get("nowrsvwtqy")),
            "storage_rate": to_float(item.get("rsvwtrt")),
            "inflow": to_float(item.get("inflowqy")),
            "discharge": to_float(item.get("totdcwtrqy")),
            "rainfall": to_float(item.get("prcptqy")),
        })

    df = pd.DataFrame(rows)

    # 김천부항 제외 20개 댐만 사용
    df = filter_target_dams(df, dam_col="dam_name", standardize=True)

    return df


# ==================================================
# 5. 최신 사용 가능한 다목적댐 데이터 조회
# ==================================================
def fetch_latest_dam_data() -> pd.DataFrame:
    now = datetime.now()

    base_datetime = now.replace(
        minute=0,
        second=0,
        microsecond=0
    )

    print("[CURRENT PC TIME]", now)
    print("[BASE DATETIME]", base_datetime)

    max_retry_hours = 6

    for hour_back in range(0, max_retry_hours + 1):
        target_datetime = base_datetime - timedelta(hours=hour_back)

        df = request_dam_data_by_time(target_datetime)

        if not df.empty:
            print(f"[SUCCESS] 사용 가능한 다목적댐 데이터 발견: {target_datetime}")
            print(f"[COUNT] 수집된 대상 댐 개수: {df['dam_name'].nunique()}")
            return df

        print(f"[WARNING] {target_datetime} 데이터 없음. 이전 시간으로 재시도합니다.")

    print("[ERROR] 최근 6시간 이내 사용 가능한 다목적댐 데이터를 찾지 못했습니다.")
    return pd.DataFrame()


# ==================================================
# 6. dam_observation 저장/갱신
# ==================================================
def save_dam_data(df: pd.DataFrame):
    if df.empty:
        print("[SKIP] 저장할 다목적댐 데이터가 없습니다.")
        return

    sql = text("""
        INSERT INTO dam_observation (
            dam_name,
            river_system,
            observed_at,
            water_level,
            storage_amount,
            storage_rate,
            inflow,
            discharge,
            rainfall
        )
        VALUES (
            :dam_name,
            :river_system,
            :observed_at,
            :water_level,
            :storage_amount,
            :storage_rate,
            :inflow,
            :discharge,
            :rainfall
        )
        ON DUPLICATE KEY UPDATE
            river_system = VALUES(river_system),
            water_level = VALUES(water_level),
            storage_amount = VALUES(storage_amount),
            storage_rate = VALUES(storage_rate),
            inflow = VALUES(inflow),
            discharge = VALUES(discharge),
            rainfall = VALUES(rainfall)
    """)

    records = df.where(pd.notnull(df), None).to_dict("records")

    with engine.begin() as conn:
        conn.execute(sql, records)

    print(f"[SUCCESS] dam_observation 저장/갱신 완료: {len(records)}건")


# ==================================================
# 7. 실행
# ==================================================
if __name__ == "__main__":
    print("[START] K-water 다목적댐 운영 데이터 수집 시작")

    df = fetch_latest_dam_data()

    print("[DATA PREVIEW - TARGET 20 DAMS]")
    if not df.empty:
        print(df.to_string(index=False))
    else:
        print("조회된 대상 댐 데이터가 없습니다.")

    save_dam_data(df)

    print("[END] K-water 다목적댐 운영 데이터 수집 종료")