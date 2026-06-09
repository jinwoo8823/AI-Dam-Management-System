from __future__ import annotations

import argparse
import calendar
import json
import os
import time
from datetime import datetime
from pathlib import Path

import pymysql
import requests


BASE_URL = "https://apis.data.go.kr/B500001/dam/excllncobsrvt"
HOURLY_WATER_LEVEL_PATH = "/hourwal/hourwallist"


def load_dotenv() -> None:
    candidates = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parents[2] / ".env",
    ]
    for env_path in candidates:
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ[key.strip().lstrip("\ufeff")] = value.strip().strip('"').strip("'")
        return


def service_key() -> str:
    load_dotenv()
    key = os.getenv("DATA_GO_KR_SERVICE_KEY", "").strip()
    if not key:
        raise RuntimeError(".env에 DATA_GO_KR_SERVICE_KEY가 필요합니다.")
    return key


def connect_mysql():
    load_dotenv()
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "ai댐 프로젝트"),
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def month_iter(start_ym: str, end_ym: str):
    start_year, start_month = map(int, start_ym.split("-"))
    end_year, end_month = map(int, end_ym.split("-"))
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        last_day = calendar.monthrange(year, month)[1]
        yield (
            f"{year:04d}-{month:02d}",
            f"{year:04d}-{month:02d}-01",
            f"{year:04d}-{month:02d}-{last_day:02d}",
        )
        month += 1
        if month == 13:
            year += 1
            month = 1


def parse_datetime(value):
    if value is None:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def parse_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def extract_items(payload: dict) -> list[dict]:
    body = payload.get("response", {}).get("body", {})
    items = body.get("items", {})
    item = items.get("item", []) if isinstance(items, dict) else items
    if isinstance(item, dict):
        return [item]
    if isinstance(item, list):
        return item
    return []


def get_representative_stations(dam_name: str | None = None, limit: int = 0) -> list[dict]:
    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT dam_code, dam_name, station_code, station_name
                FROM dam_representative_downstream_station
            """
            params: list[object] = []
            if dam_name:
                sql += " WHERE dam_name = %s"
                params.append(dam_name)
            sql += " ORDER BY dam_code"
            if limit:
                sql += " LIMIT %s"
                params.append(limit)
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def request_month(dam_code: int, station_code: str, start_date: str, end_date: str) -> list[dict]:
    params = {
        "serviceKey": service_key(),
        "pageNo": 1,
        "numOfRows": 1000,
        "sdate": start_date,
        "stime": "00",
        "edate": end_date,
        "etime": "23",
        "damcode": dam_code,
        "wal": station_code,
        "tms": "60",
        "_type": "json",
    }
    response = requests.get(f"{BASE_URL}{HOURLY_WATER_LEVEL_PATH}", params=params, timeout=40)
    if response.status_code >= 400:
        raise RuntimeError(f"API HTTP 오류: status={response.status_code}, body={response.text[:300]}")
    if not response.text.strip():
        return []
    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"API JSON 파싱 실패: {response.text[:300]}") from exc
    header = payload.get("response", {}).get("header", {})
    if header.get("resultCode") not in {None, "00"}:
        raise RuntimeError(f"API 응답 오류: {header}")

    rows = []
    for item in extract_items(payload):
        observed_at = parse_datetime(item.get("obsrdt"))
        if observed_at is None:
            continue
        rows.append(
            {
                "station_code": str(station_code),
                "observed_at": observed_at,
                "water_level": parse_float(item.get("hourwal")),
                "flow_rate": parse_float(item.get("flux")),
                "source": "K_WATER_HOURWAL_HISTORY",
            }
        )
    return rows


def upsert_rows(rows: list[dict]) -> int:
    if not rows:
        return 0
    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO downstream_water_level_observation
                    (station_code, observed_at, water_level, flow_rate, source)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    water_level = VALUES(water_level),
                    flow_rate = VALUES(flow_rate),
                    source = VALUES(source)
            """
            values = [
                (
                    row["station_code"],
                    row["observed_at"],
                    row["water_level"],
                    row["flow_rate"],
                    row["source"],
                )
                for row in rows
            ]
            cur.executemany(sql, values)
        conn.commit()
        return len(values)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="대표 하류 수위관측소 과거 월별 시자료 수집")
    parser.add_argument("--start-ym", default="2023-01", help="시작 연월, 예: 2023-01")
    parser.add_argument("--end-ym", default="2025-12", help="종료 연월, 예: 2025-12")
    parser.add_argument("--dam-name", default="", help="특정 댐만 테스트할 때 사용")
    parser.add_argument("--limit", type=int, default=0, help="앞에서 N개 대표 관측소만 수집")
    parser.add_argument("--sleep", type=float, default=0.15, help="API 호출 사이 대기초")
    parser.add_argument("--dry-run", action="store_true", help="DB 저장 없이 호출 가능 여부만 확인")
    args = parser.parse_args()

    stations = get_representative_stations(args.dam_name or None, args.limit)
    if not stations:
        raise RuntimeError("수집할 대표 하류 수위관측소가 없습니다.")

    total_rows = 0
    for station in stations:
        dam_code = int(station["dam_code"])
        dam_name = station["dam_name"]
        station_code = str(station["station_code"])
        station_name = station["station_name"]
        dam_total = 0
        print(f"[START] {dam_name} / {station_name}({station_code})")
        for ym, start_date, end_date in month_iter(args.start_ym, args.end_ym):
            rows = request_month(dam_code, station_code, start_date, end_date)
            inserted = 0 if args.dry_run else upsert_rows(rows)
            dam_total += len(rows)
            total_rows += len(rows)
            print(f"  {ym}: fetched={len(rows)}, saved={inserted}")
            time.sleep(args.sleep)
        print(f"[DONE] {dam_name}: fetched_total={dam_total}")
    print(f"전체 수집 완료: fetched_total={total_rows}, dry_run={args.dry_run}")


if __name__ == "__main__":
    main()
