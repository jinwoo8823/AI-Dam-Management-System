import os
import re
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from target_dams import filter_target_dams, get_standard_dam_name


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
# 3. 강수량 문자열 숫자 변환
# ==================================================
def parse_rainfall(value):
    """
    초단기예보 RN1 값을 mm 숫자로 변환한다.

    예:
    "강수없음" -> 0
    "1mm 미만" -> 0.5
    "1.0mm" -> 1.0
    "30.0mm 이상" -> 30.0
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

    return float(numbers[0])


# ==================================================
# 4. API 응답 item 정규화
# ==================================================
def normalize_items(items):
    if not items:
        return []

    if isinstance(items, dict):
        return [items]

    return items


# ==================================================
# 5. 초단기예보 base_time 후보 생성
# ==================================================
def get_ultra_short_base_candidates(max_retry=12):
    """
    초단기예보는 30분 단위 발표이므로,
    현재 시각에서 가장 가까운 과거 30분 단위부터 역순으로 후보를 만든다.
    """
    now = datetime.now()

    minute = 30 if now.minute >= 30 else 0

    base = now.replace(
        minute=minute,
        second=0,
        microsecond=0
    )

    candidates = []

    for i in range(max_retry + 1):
        candidates.append(base - timedelta(minutes=30 * i))

    return candidates


# ==================================================
# 6. dam_location에서 대상 20개 댐만 조회
# ==================================================
def load_target_dams():
    query = """
        SELECT
            dam_name,
            latitude,
            longitude,
            nx,
            ny
        FROM dam_location
    """

    dam_df = pd.read_sql(query, engine)

    dam_df = filter_target_dams(dam_df, dam_col="dam_name", standardize=True)

    if dam_df.empty:
        raise ValueError("dam_location에서 김천부항 제외 20개 대상 댐을 찾지 못했습니다.")

    print("[TARGET DAM COUNT]", dam_df["dam_name"].nunique())
    print(dam_df[["dam_name", "nx", "ny"]].to_string(index=False))

    return dam_df


# ==================================================
# 7. 특정 댐의 초단기예보 조회
# ==================================================
def request_short_forecast(region_name, nx, ny):
    url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst"

    candidates = get_ultra_short_base_candidates()

    for base_datetime in candidates:
        base_date = base_datetime.strftime("%Y%m%d")
        base_time = base_datetime.strftime("%H%M")

        params = {
            "serviceKey": API_KEY,
            "pageNo": 1,
            "numOfRows": 1000,
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": int(nx),
            "ny": int(ny),
        }

        safe_params = params.copy()
        safe_params["serviceKey"] = "********"

        print(f"[SHORT REQUEST] {region_name} / base={base_date} {base_time} / nx={nx}, ny={ny}")

        try:
            response = requests.get(url, params=params, timeout=30)
            print("[SHORT STATUS]", response.status_code)
            response.raise_for_status()

            data = response.json()

            header = data.get("response", {}).get("header", {})
            result_code = header.get("resultCode")
            result_msg = header.get("resultMsg")
            print("[SHORT API RESULT]", result_code, result_msg)

            body = data.get("response", {}).get("body", {})
            items = body.get("items", {}).get("item", [])
            items = normalize_items(items)

            if not items:
                print(f"[WARNING] {region_name} {base_date} {base_time} 데이터 없음")
                continue

            rows = []

            for item in items:
                rows.append({
                    "source": "KMA_SHORT",
                    "region_name": region_name,
                    "nx": int(nx),
                    "ny": int(ny),
                    "base_date": item.get("baseDate"),
                    "base_time": item.get("baseTime"),
                    "forecast_date": item.get("fcstDate"),
                    "forecast_time": item.get("fcstTime"),
                    "category": item.get("category"),
                    "forecast_value": item.get("fcstValue"),
                })

            df = pd.DataFrame(rows)

            if not df.empty:
                print(f"[SUCCESS] {region_name} 초단기예보 수집: {len(df)}건")
                return df

        except Exception as e:
            print(f"[ERROR] {region_name} 초단기예보 요청 실패:", e)
            continue

    print(f"[ERROR] {region_name} 초단기예보 사용 가능한 base_time을 찾지 못했습니다.")
    return pd.DataFrame()


# ==================================================
# 8. 원본 초단기예보 저장
# ==================================================
def save_short_forecast(df):
    if df.empty:
        return

    region_name = df["region_name"].iloc[0]
    base_date = df["base_date"].iloc[0]
    base_time = df["base_time"].iloc[0]

    delete_sql = text("""
        DELETE FROM weather_forecast_short
        WHERE region_name = :region_name
          AND base_date = :base_date
          AND base_time = :base_time
    """)

    insert_sql = text("""
        INSERT INTO weather_forecast_short (
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
        )
        VALUES (
            :source,
            :region_name,
            :nx,
            :ny,
            :base_date,
            :base_time,
            :forecast_date,
            :forecast_time,
            :category,
            :forecast_value
        )
    """)

    records = df.where(pd.notnull(df), None).to_dict("records")

    with engine.begin() as conn:
        conn.execute(delete_sql, {
            "region_name": region_name,
            "base_date": base_date,
            "base_time": base_time,
        })
        conn.execute(insert_sql, records)

    print(f"[SUCCESS] weather_forecast_short 저장 완료: {region_name}, {len(records)}건")


# ==================================================
# 9. RN1 합산 summary 생성
# ==================================================
def build_short_rainfall_summary(df):
    if df.empty:
        return pd.DataFrame()

    rain_df = df[df["category"] == "RN1"].copy()

    if rain_df.empty:
        return pd.DataFrame()

    rain_df["rainfall_mm"] = rain_df["forecast_value"].apply(parse_rainfall)

    region_name = rain_df["region_name"].iloc[0]
    base_date = rain_df["base_date"].iloc[0]
    base_time = rain_df["base_time"].iloc[0]

    sorted_df = rain_df.sort_values(["forecast_date", "forecast_time"])

    total_rainfall = sorted_df["rainfall_mm"].sum()

    summary = pd.DataFrame([{
        "region_name": region_name,
        "base_date": base_date,
        "base_time": base_time,
        "forecast_start_date": sorted_df["forecast_date"].iloc[0],
        "forecast_start_time": sorted_df["forecast_time"].iloc[0],
        "forecast_end_date": sorted_df["forecast_date"].iloc[-1],
        "forecast_end_time": sorted_df["forecast_time"].iloc[-1],
        "total_rainfall_mm": round(float(total_rainfall), 2),
    }])

    return summary


# ==================================================
# 10. 초단기 강수량 summary 저장
# ==================================================
def save_short_rainfall_summary(df):
    if df.empty:
        return

    region_name = df["region_name"].iloc[0]
    base_date = df["base_date"].iloc[0]
    base_time = df["base_time"].iloc[0]

    delete_sql = text("""
        DELETE FROM weather_rainfall_summary_short
        WHERE region_name = :region_name
          AND base_date = :base_date
          AND base_time = :base_time
    """)

    insert_sql = text("""
        INSERT INTO weather_rainfall_summary_short (
            region_name,
            base_date,
            base_time,
            forecast_start_date,
            forecast_start_time,
            forecast_end_date,
            forecast_end_time,
            total_rainfall_mm
        )
        VALUES (
            :region_name,
            :base_date,
            :base_time,
            :forecast_start_date,
            :forecast_start_time,
            :forecast_end_date,
            :forecast_end_time,
            :total_rainfall_mm
        )
    """)

    records = df.where(pd.notnull(df), None).to_dict("records")

    with engine.begin() as conn:
        conn.execute(delete_sql, {
            "region_name": region_name,
            "base_date": base_date,
            "base_time": base_time,
        })
        conn.execute(insert_sql, records)

    print(f"[SUCCESS] weather_rainfall_summary_short 저장 완료: {region_name}")


# ==================================================
# 11. 전체 실행
# ==================================================
def collect_short_forecast():
    dam_df = load_target_dams()

    success_count = 0

    for _, row in dam_df.iterrows():
        raw_name = row["dam_name"]
        region_name = get_standard_dam_name(raw_name) or raw_name

        nx = row["nx"]
        ny = row["ny"]

        forecast_df = request_short_forecast(region_name, nx, ny)

        if forecast_df.empty:
            continue

        save_short_forecast(forecast_df)

        summary_df = build_short_rainfall_summary(forecast_df)

        if not summary_df.empty:
            save_short_rainfall_summary(summary_df)

        success_count += 1

    print(f"[RESULT] 초단기예보 수집 완료 대상 댐 수: {success_count}")


if __name__ == "__main__":
    print("[START] KMA 초단기예보 수집 시작 - TARGET 20 DAMS")

    collect_short_forecast()

    print("[END] KMA 초단기예보 수집 종료")