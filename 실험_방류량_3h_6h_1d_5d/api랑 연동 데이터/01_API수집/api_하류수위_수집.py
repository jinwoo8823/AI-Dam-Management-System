from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timedelta

import pandas as pd
import requests

from api_공통 import connect_mysql, get_service_key, load_dam_master, load_dotenv_if_exists


BASE_URL = "https://apis.data.go.kr/B500001/dam/excllncobsrvt"
WATER_LEVEL_STATION_PATH = "/wal/wallist"
HOURLY_WATER_LEVEL_PATH = "/hourwal/hourwallist"


def extract_items(payload: dict) -> list[dict]:
    body = payload.get("response", {}).get("body", {})
    items = body.get("items", {})
    item = items.get("item", []) if isinstance(items, dict) else items
    if isinstance(item, dict):
        return [item]
    if isinstance(item, list):
        return item
    return []


def request_json(path: str, params: dict) -> dict:
    response = requests.get(f"{BASE_URL}{path}", params=params, timeout=30)
    if response.status_code >= 400:
        raise RuntimeError(f"하류 수위 API 요청 실패: status={response.status_code}, path={path}, body={response.text[:300]}")
    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"하류 수위 API JSON 파싱 실패: {response.text[:300]}") from exc
    header = payload.get("response", {}).get("header", {})
    if header.get("resultCode") not in {None, "00"}:
        raise RuntimeError(f"하류 수위 API 오류: {header}")
    return payload


def request_station_codes(dam_code: int) -> list[dict]:
    payload = request_json(
        WATER_LEVEL_STATION_PATH,
        {
            "serviceKey": get_service_key(),
            "damcode": dam_code,
            "_type": "json",
        },
    )
    rows = []
    for item in extract_items(payload):
        station_code = item.get("walobsrvtcode")
        station_name = item.get("obsrvtNm")
        if station_code and station_name:
            rows.append({"station_code": str(station_code), "station_name": str(station_name)})
    return rows


def parse_datetime(value) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    match = re.match(r"(\d{4}-\d{2}-\d{2})\s+24:00", text)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d") + timedelta(days=1)
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


def request_hourly_water_level(dam_code: int, station_code: str, start: datetime, end: datetime) -> list[dict]:
    payload = request_json(
        HOURLY_WATER_LEVEL_PATH,
        {
            "serviceKey": get_service_key(),
            "pageNo": 1,
            "numOfRows": 1000,
            "sdate": start.strftime("%Y-%m-%d"),
            "stime": start.strftime("%H"),
            "edate": end.strftime("%Y-%m-%d"),
            "etime": end.strftime("%H"),
            "damcode": dam_code,
            "wal": station_code,
            "tms": "60",
            "_type": "json",
        },
    )
    rows = []
    for item in extract_items(payload):
        observed_at = parse_datetime(item.get("obsrdt"))
        if observed_at is None:
            continue
        rows.append(
            {
                "station_code": station_code,
                "observed_at": observed_at,
                "water_level": parse_float(item.get("hourwal")),
                "flow_rate": parse_float(item.get("flux")),
                "source": "K_WATER_HOURWAL",
            }
        )
    return rows


def upsert_official_station_map(dam: dict, stations: list[dict]) -> None:
    if not stations:
        return
    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            station_sql = """
                INSERT INTO downstream_station (station_code, station_name, source)
                VALUES (%s, %s, 'K_WATER_WAL_LIST')
                ON DUPLICATE KEY UPDATE
                    station_name = COALESCE(station_name, VALUES(station_name)),
                    source = source
            """
            cur.executemany(station_sql, [(row["station_code"], row["station_name"]) for row in stations])

            map_sql = """
                INSERT INTO dam_downstream_station_map
                    (dam_code, dam_name, station_code, station_name, rank_no, mapping_type, note)
                VALUES (%s, %s, %s, %s, %s, 'OFFICIAL_API', %s)
                ON DUPLICATE KEY UPDATE
                    station_name = VALUES(station_name),
                    mapping_type = CASE
                        WHEN mapping_type = 'DISTANCE_CANDIDATE' THEN 'DISTANCE_OFFICIAL'
                        ELSE mapping_type
                    END,
                    note = VALUES(note)
            """
            values = [
                (
                    int(dam["dam_code"]),
                    dam["dam_name"],
                    row["station_code"],
                    row["station_name"],
                    100 + idx,
                    "K-water 수위 관측소 코드 조회 API에서 내려온 공식 연결 후보",
                )
                for idx, row in enumerate(stations, start=1)
            ]
            cur.executemany(map_sql, values)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_water_level_rows(rows: list[dict]) -> int:
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
    parser = argparse.ArgumentParser(description="K-water 하류 수위 관측소/시간자료 수집")
    parser.add_argument("--hours", type=int, default=72)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--max-stations-per-dam", type=int, default=6)
    args = parser.parse_args()

    load_dotenv_if_exists()
    dams = load_dam_master()
    if args.limit:
        dams = dams[: args.limit]
    end = datetime.now()
    start = end - timedelta(hours=args.hours)

    total_station_count = 0
    total_observation_count = 0
    for dam in dams:
        stations = request_station_codes(int(dam["dam_code"]))
        upsert_official_station_map(dam, stations)
        total_station_count += len(stations)

        observation_rows = []
        for station in stations[: args.max_stations_per_dam]:
            observation_rows.extend(
                request_hourly_water_level(int(dam["dam_code"]), station["station_code"], start, end)
            )
        inserted = upsert_water_level_rows(observation_rows)
        total_observation_count += inserted
        print(f"{dam['dam_name']} 하류 수위 관측소 {len(stations)}개, 시자료 저장 {inserted}건")

    print(f"하류 수위 관측소 조회 완료: {total_station_count}개")
    print(f"하류 수위 시자료 저장 완료: {total_observation_count}건")


if __name__ == "__main__":
    main()
