import os
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


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
# 2. final_data.csv 경로
# ==================================================
CSV_PATH = "final_data.csv"


# ==================================================
# 3. 김천부항 제외 20개 댐 코드 매핑
# ==================================================
TARGET_DAM_CODE_MAP = {
    "1012110": "소양강",
    "1003110": "충주",
    "1006110": "횡성",

    "2001110": "안동",
    "2002110": "임하",
    "2002111": "성덕",
    "2004101": "영주",
    "2008101": "군위",
    "2012101": "보현산",

    "3008110": "대청",
    "3001110": "용담",

    "4001110": "섬진강",
    "4007110": "주암(본)",
    "4104610": "주암(조)",

    "2015110": "합천",
    "2018110": "남강",
    "2021110": "밀양",

    "3203110": "보령",
    "3303110": "부안",
    "5101110": "장흥",
}


# ==================================================
# 4. 숫자 변환
# ==================================================
def to_numeric(series):
    return pd.to_numeric(series, errors="coerce")


def get_column_or_none(df, column_name):
    if column_name in df.columns:
        return to_numeric(df[column_name])

    return None


# ==================================================
# 5. DB 저장 SQL
# ==================================================
INSERT_SQL = text("""
    INSERT INTO final_historical_data (
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
        snow,

        source_file
    )
    VALUES (
        :dam_code,
        :dam_name,
        :observed_at,

        :inflow,
        :water_level,
        :hydrology_rainfall,
        :storage_amount,
        :storage_rate,
        :discharge,

        :temperature,
        :kma_rainfall,
        :snow,

        :source_file
    )
    ON DUPLICATE KEY UPDATE
        dam_name = VALUES(dam_name),
        inflow = VALUES(inflow),
        water_level = VALUES(water_level),
        hydrology_rainfall = VALUES(hydrology_rainfall),
        storage_amount = VALUES(storage_amount),
        storage_rate = VALUES(storage_rate),
        discharge = VALUES(discharge),
        temperature = VALUES(temperature),
        kma_rainfall = VALUES(kma_rainfall),
        snow = VALUES(snow),
        source_file = VALUES(source_file),
        updated_at = CURRENT_TIMESTAMP
""")


# ==================================================
# 6. chunk 저장
# ==================================================
def save_chunk(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    records = df.astype(object).where(pd.notnull(df), None).to_dict("records")

    with engine.begin() as conn:
        conn.execute(INSERT_SQL, records)

    return len(records)


# ==================================================
# 7. final_data.csv 처리
# ==================================================
def insert_final_data_csv():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(
            f"{CSV_PATH} 파일을 찾을 수 없습니다. "
            f"final_data.csv를 C:\\Ai_Dam_Manager 폴더에 넣었는지 확인하세요."
        )

    print("[START] final_data.csv 저장 시작")
    print("[CSV PATH]", CSV_PATH)

    chunk_size = 50000

    total_read = 0
    total_target = 0
    total_saved = 0
    chunk_no = 0

    required_columns = [
        "dam_code",
        "obsrdt",
        "inflowqy",
        "lowlevel",
        "rf",
        "rsvwtqy",
        "rsvwtrt",
        "totdcwtrqy",
        "tmp",
        "rain",
        "snow",
    ]

    reader = pd.read_csv(
        CSV_PATH,
        chunksize=chunk_size,
        encoding="utf-8-sig"
    )

    for chunk in reader:
        chunk_no += 1
        total_read += len(chunk)

        missing_columns = [col for col in required_columns if col not in chunk.columns]

        if missing_columns:
            raise ValueError(f"CSV에 필요한 컬럼이 없습니다: {missing_columns}")

        chunk["dam_code"] = chunk["dam_code"].astype(str).str.strip()

        # 김천부항 제외 20개 댐 코드만 필터링
        chunk = chunk[chunk["dam_code"].isin(TARGET_DAM_CODE_MAP.keys())].copy()

        if chunk.empty:
            print(f"[CHUNK {chunk_no}] 대상 20개 댐 데이터 없음")
            continue

        total_target += len(chunk)

        chunk["dam_name"] = chunk["dam_code"].map(TARGET_DAM_CODE_MAP)

        result_df = pd.DataFrame({
            "dam_code": chunk["dam_code"],
            "dam_name": chunk["dam_name"],
            "observed_at": pd.to_datetime(chunk["obsrdt"], errors="coerce"),

            "inflow": to_numeric(chunk["inflowqy"]),
            "water_level": to_numeric(chunk["lowlevel"]),
            "hydrology_rainfall": to_numeric(chunk["rf"]),
            "storage_amount": to_numeric(chunk["rsvwtqy"]),
            "storage_rate": to_numeric(chunk["rsvwtrt"]),
            "discharge": to_numeric(chunk["totdcwtrqy"]),

            "temperature": to_numeric(chunk["tmp"]),
            "kma_rainfall": to_numeric(chunk["rain"]),
            "snow": to_numeric(chunk["snow"]),

            "source_file": os.path.basename(CSV_PATH),
        })

        result_df = result_df.dropna(subset=["observed_at"])

        saved_count = save_chunk(result_df)
        total_saved += saved_count

        print(
            f"[CHUNK {chunk_no}] "
            f"원본 읽음: {len(chunk):,}건 / "
            f"저장: {saved_count:,}건 / "
            f"누적 저장: {total_saved:,}건"
        )

    print("[END] final_data.csv 저장 완료")
    print(f"[TOTAL READ] {total_read:,}건")
    print(f"[TOTAL TARGET 20 DAM ROWS] {total_target:,}건")
    print(f"[TOTAL SAVED] {total_saved:,}건")


# ==================================================
# 8. 실행
# ==================================================
if __name__ == "__main__":
    insert_final_data_csv()