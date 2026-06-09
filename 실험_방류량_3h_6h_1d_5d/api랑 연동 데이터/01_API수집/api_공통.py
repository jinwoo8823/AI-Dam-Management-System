from __future__ import annotations

import os
from pathlib import Path

import pymysql


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_dotenv_if_exists() -> None:
    candidates = [
        Path.cwd() / ".env",
        project_root() / ".env",
    ]
    for env_path in candidates:
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


def get_service_key() -> str:
    load_dotenv_if_exists()
    service_key = os.getenv("DATA_GO_KR_SERVICE_KEY", "").strip()
    if not service_key:
        raise RuntimeError(".env에 DATA_GO_KR_SERVICE_KEY가 없습니다.")
    return service_key


def connect_mysql():
    load_dotenv_if_exists()
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "ai댐 프로젝트"),
        charset="utf8mb4",
        autocommit=False,
    )


def load_dam_master():
    conn = connect_mysql()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                """
                SELECT dam_code, dam_name, latitude, longitude, grid_x, grid_y
                FROM dam_code
                WHERE is_multipurpose = TRUE
                ORDER BY dam_code
                """
            )
            return cur.fetchall()
    finally:
        conn.close()
