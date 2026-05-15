import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD", ""))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = os.getenv("DB_NAME", "ai_dam_management")

if not API_KEY:
    raise ValueError(".env 파일에 API_KEY가 없습니다.")

if not DB_PASSWORD:
    raise ValueError(".env 파일에 DB_PASSWORD가 없습니다.")

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
)

url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"


def get_realtime_base_datetime():
    now = datetime.now()

    # 초단기실황은 정시 기준 데이터입니다.
    # 예: 현재 19:09이면 최신 기준시각은 보통 1900입니다.
    # 다만 매시 초반에는 데이터 반영이 늦을 수 있어 10분 이전이면 전 시간 사용.
    if now.minute < 10:
        base_dt = now - timedelta(hours=1)
    else:
        base_dt = now

    base_date = base_dt.strftime("%Y%m%d")
    base_time = base_dt.strftime("%H00")

    return base_date, base_time


def fetch_realtime_weather(dam_name, nx, ny, base_date, base_time):
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

    response = requests.get(url, params=params, timeout=15)

    print(f"[{dam_name}] status_code:", response.status_code)

    try:
        data = response.json()
    except Exception:
        print(f"[{dam_name}] JSON 변환 실패")
        print(response.text[:1000])
        return []

    header = data.get("response", {}).get("header", {})
    result_code = header.get("resultCode")
    result_msg = header.get("resultMsg")

    if result_code != "00":
        print(f"[{dam_name}] API 오류: {result_code} / {result_msg}")
        return []

    body = data.get("response", {}).get("body")

    if body is None:
        print(f"[{dam_name}] body 없음")
        return []

    items = body.get("items", {}).get("item", [])

    rows = []

    for item in items:
        rows.append({
            "source": "KMA_REALTIME",
            "region_name": dam_name,
            "nx": int(nx),
            "ny": int(ny),
            "base_date": datetime.strptime(item.get("baseDate"), "%Y%m%d").date(),
            "base_time": item.get("baseTime"),
            "category": item.get("category"),
            "observed_value": item.get("obsrValue"),
        })

    return rows


def main():
    base_date, base_time = get_realtime_base_datetime()

    print("조회 기준일:", base_date)
    print("조회 기준시각:", base_time)

    dams = pd.read_sql(
        """
        SELECT dam_id, dam_name, nx, ny
        FROM dam_location
        WHERE nx IS NOT NULL
          AND ny IS NOT NULL
        """,
        engine
    )

    print(f"조회 대상 댐 수: {len(dams)}")

    all_rows = []

    for _, dam in dams.iterrows():
        rows = fetch_realtime_weather(
            dam_name=dam["dam_name"],
            nx=dam["nx"],
            ny=dam["ny"],
            base_date=base_date,
            base_time=base_time
        )

        all_rows.extend(rows)

        print(f"{dam['dam_name']} 수집 데이터 수: {len(rows)}")

        # API 과다 호출 방지용 짧은 대기
        time.sleep(0.2)

    if not all_rows:
        print("저장할 데이터가 없습니다.")
        return

    df = pd.DataFrame(all_rows)

    df.to_sql(
        "weather_realtime",
        con=engine,
        if_exists="append",
        index=False
    )

    print("전체 실시간 기상 데이터 저장 완료")
    print(df.head(20))
    print(f"전체 저장 데이터 수: {len(df)}")


if __name__ == "__main__":
    main()