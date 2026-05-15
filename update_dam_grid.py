import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
from grid_converter import convert_to_grid

load_dotenv()

DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD", ""))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = os.getenv("DB_NAME", "ai_dam_management")

if not DB_PASSWORD:
    raise ValueError(".env 파일에 DB_PASSWORD가 없습니다.")

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
)

df = pd.read_sql(
    "SELECT dam_id, dam_name, latitude, longitude FROM dam_location",
    engine
)

with engine.begin() as conn:
    for _, row in df.iterrows():
        nx, ny = convert_to_grid(
            float(row["latitude"]),
            float(row["longitude"])
        )

        conn.execute(
            text("""
                UPDATE dam_location
                SET nx = :nx, ny = :ny
                WHERE dam_id = :dam_id
            """),
            {
                "nx": nx,
                "ny": ny,
                "dam_id": int(row["dam_id"])
            }
        )

        print(row["dam_name"], nx, ny)

print("댐 격자 변환 완료")