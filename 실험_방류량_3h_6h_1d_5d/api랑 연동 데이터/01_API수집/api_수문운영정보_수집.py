from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timedelta

import requests

from api_공통 import connect_mysql, get_service_key, load_dam_master, load_dotenv_if_exists


def parse_datetime(value, default_year: int | None = None) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if default_year is not None:
        compact_match = re.search(r"(\d{2})-(\d{2})\s+(\d{1,2})", text)
        if compact_match:
            month, day, hour = map(int, compact_match.groups())
            parsed = datetime(default_year, month, day)
            if hour == 24:
                return parsed + timedelta(days=1)
            return parsed.replace(hour=hour)

    text = text.replace("시", ":00").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d%H%M", "%Y%m%d%H"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    if default_year is not None:
        for fmt in ("%m-%d %H:%M", "%m-%d %H"):
            try:
                parsed = datetime.strptime(text, fmt)
                return parsed.replace(year=default_year)
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


def request_operation_items(dam_code: int, start: datetime, end: datetime) -> list[dict]:
    load_dotenv_if_exists()
    url = os.getenv(
        "K_WATER_DAM_OPERATION_URL",
        "https://apis.data.go.kr/B500001/dam/sluicePresentCondition/hourlist",
    ).strip()

    params = {
        "serviceKey": get_service_key(),
        "pageNo": 1,
        "numOfRows": 1000,
        "_type": "json",
        os.getenv("K_WATER_DAM_CODE_PARAM", "damcode"): dam_code,
        os.getenv("K_WATER_START_PARAM", "stdt"): start.strftime("%Y-%m-%d"),
        os.getenv("K_WATER_END_PARAM", "eddt"): end.strftime("%Y-%m-%d"),
    }
    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"수문 운영 정보 API 접속 실패: {url}") from exc
    payload = response.json()
    header = payload.get("response", {}).get("header", {})
    if header.get("resultCode") not in {None, "00"}:
        raise RuntimeError(f"수문 운영 정보 API 오류: {header}")
    item = payload.get("response", {}).get("body", {}).get("items", {}).get("item", [])
    if isinstance(item, dict):
        return [item]
    return item


def normalize_operation_item(dam_code: int, item: dict, default_year: int) -> dict | None:
    obsrdt = (
        parse_datetime(item.get("obsrdt"), default_year)
        or parse_datetime(item.get("observationDate"), default_year)
        or parse_datetime(item.get("ymdhm"), default_year)
        or parse_datetime(item.get("obsrDt"), default_year)
    )
    if obsrdt is None:
        return None

    return {
        "dam_code": dam_code,
        "obsrdt": obsrdt,
        "inflowqy": parse_float(item.get("inflowqy")),
        "lowlevel": parse_float(item.get("lowlevel")),
        "rf": parse_float(item.get("rf")),
        "rsvwtqy": parse_float(item.get("rsvwtqy")),
        "rsvwtrt": parse_float(item.get("rsvwtrt")),
        "totdcwtrqy": parse_float(item.get("totdcwtrqy")),
        "source": "K_WATER_OPERATION",
    }


def upsert_observations(rows: list[dict]) -> int:
    if not rows:
        return 0
    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO dam_realtime_observation
                    (dam_code, obsrdt, inflowqy, lowlevel, rf, rsvwtqy, rsvwtrt, totdcwtrqy, source)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    inflowqy = VALUES(inflowqy),
                    lowlevel = VALUES(lowlevel),
                    rf = VALUES(rf),
                    rsvwtqy = VALUES(rsvwtqy),
                    rsvwtrt = VALUES(rsvwtrt),
                    totdcwtrqy = VALUES(totdcwtrqy),
                    source = VALUES(source)
            """
            values = [
                (
                    row["dam_code"],
                    row["obsrdt"],
                    row["inflowqy"],
                    row["lowlevel"],
                    row["rf"],
                    row["rsvwtqy"],
                    row["rsvwtrt"],
                    row["totdcwtrqy"],
                    row["source"],
                )
                for row in rows
            ]
            cur.executemany(sql, values)
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=72)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    end = datetime.now()
    start = end - timedelta(hours=args.hours)
    dams = load_dam_master()
    if args.limit:
        dams = dams[: args.limit]

    total = 0
    for dam in dams:
        items = request_operation_items(dam["dam_code"], start, end)
        rows = [
            row
            for item in items
            if (row := normalize_operation_item(dam["dam_code"], item, start.year)) is not None
        ]
        inserted = upsert_observations(rows)
        total += inserted
        print(f"{dam['dam_name']} 수문 운영 정보 저장: {inserted}건")

    print(f"수문 운영 정보 저장 완료: {total}건")


if __name__ == "__main__":
    main()
