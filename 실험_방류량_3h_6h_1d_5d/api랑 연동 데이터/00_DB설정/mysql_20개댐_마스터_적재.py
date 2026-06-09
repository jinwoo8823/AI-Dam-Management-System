from __future__ import annotations

import csv
import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parents[1]
MASTER_CSV = BASE_DIR / "20개댐_마스터.csv"
SCHEMA_SQL = BASE_DIR / "mysql_스키마.sql"


def load_dotenv_if_exists() -> None:
    for env_path in [Path.cwd() / ".env", PROJECT_ROOT / ".env"]:
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip().lstrip("\ufeff")
            value = value.strip().strip('"').strip("'")
            if value:
                os.environ[key] = value
        return


def connect_mysql():
    try:
        import pymysql
    except ImportError as exc:
        raise SystemExit("pymysql이 설치되어 있지 않습니다. `pip install pymysql` 후 다시 실행하세요.") from exc

    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    database = os.getenv("MYSQL_DATABASE", "ai댐 프로젝트")

    server_conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        with server_conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{database}` "
                "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
    finally:
        server_conn.close()

    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=False,
    )


def read_sql_statements(sql_text: str) -> list[str]:
    statements = []
    current = []
    for line in sql_text.splitlines():
        current.append(line)
        if line.rstrip().endswith(";"):
            statements.append("\n".join(current).strip())
            current = []
    if current:
        statements.append("\n".join(current).strip())
    return [stmt for stmt in statements if stmt]


def main() -> None:
    load_dotenv_if_exists()

    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            for statement in read_sql_statements(SCHEMA_SQL.read_text(encoding="utf-8")):
                cur.execute(statement)

            with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
                rows = list(csv.DictReader(f))

            sql = """
                INSERT INTO dam_code
                    (dam_code, dam_name, latitude, longitude, grid_x, grid_y, is_multipurpose)
                VALUES
                    (%s, %s, %s, %s, %s, %s, TRUE)
                ON DUPLICATE KEY UPDATE
                    dam_name = VALUES(dam_name),
                    latitude = VALUES(latitude),
                    longitude = VALUES(longitude),
                    grid_x = VALUES(grid_x),
                    grid_y = VALUES(grid_y),
                    is_multipurpose = TRUE
            """
            values = [
                (
                    int(row["dam_code"]),
                    row["dam_name"],
                    float(row["latitude"]),
                    float(row["longitude"]),
                    int(row["grid_x"]),
                    int(row["grid_y"]),
                )
                for row in rows
            ]
            cur.executemany(sql, values)
            cur.execute("SELECT COUNT(*) FROM dam_code WHERE is_multipurpose = TRUE")
            count = cur.fetchone()[0]

        conn.commit()
        print(f"20개댐 마스터 적재 완료: {count}개")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
