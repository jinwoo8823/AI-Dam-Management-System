from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta

import requests

from api_공통 import connect_mysql, get_service_key, load_dam_master


ULTRA_NCST_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"
ULTRA_FCST_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst"


def ultra_base_datetime(now: datetime) -> datetime:
    base = now.replace(minute=30, second=0, microsecond=0)
    if now.minute < 45:
        base -= timedelta(hours=1)
    return base


def ncst_base_datetime(now: datetime) -> datetime:
    base = now.replace(minute=0, second=0, microsecond=0)
    if now.minute < 40:
        base -= timedelta(hours=1)
    return base


def parse_datetime(date_text: str, time_text: str) -> datetime:
    return datetime.strptime(f"{date_text}{time_text.zfill(4)}", "%Y%m%d%H%M")


def parse_amount(value) -> float:
    if value is None:
        return 0.0
    text = str(value).strip()
    if text in {"", "강수없음", "적설없음", "없음"}:
        return 0.0
    text = text.replace("mm", "").replace("cm", "").replace("미만", "").strip()
    if "~" in text:
        text = text.split("~", 1)[0].strip()
    try:
        return float(text)
    except ValueError:
        return 0.0


def request_items(url: str, params: dict, retries: int = 2) -> list[dict]:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()
            header = payload.get("response", {}).get("header", {})
            if header.get("resultCode") not in {None, "00"}:
                raise RuntimeError(f"기상청 API 오류: {header}")
            item = payload.get("response", {}).get("body", {}).get("items", {}).get("item", [])
            if isinstance(item, dict):
                return [item]
            return item
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"기상청 API 접속 실패: {url}") from last_error


def fetch_ultra_forecast(service_key: str, grid_x: int, grid_y: int, now: datetime) -> list[dict]:
    base = ultra_base_datetime(now)
    params = {
        "serviceKey": service_key,
        "pageNo": 1,
        "numOfRows": 1000,
        "dataType": "JSON",
        "base_date": base.strftime("%Y%m%d"),
        "base_time": base.strftime("%H%M"),
        "nx": grid_x,
        "ny": grid_y,
    }
    return request_items(ULTRA_FCST_URL, params)


def fetch_ultra_nowcast(service_key: str, grid_x: int, grid_y: int, now: datetime) -> list[dict]:
    base = ncst_base_datetime(now)
    params = {
        "serviceKey": service_key,
        "pageNo": 1,
        "numOfRows": 1000,
        "dataType": "JSON",
        "base_date": base.strftime("%Y%m%d"),
        "base_time": base.strftime("%H%M"),
        "nx": grid_x,
        "ny": grid_y,
    }
    return request_items(ULTRA_NCST_URL, params)


def normalize_items(dam: dict, items: list[dict], source: str) -> list[dict]:
    grouped: dict[tuple[datetime, datetime], dict] = {}
    for item in items:
        base_dt = parse_datetime(item["baseDate"], item["baseTime"])
        fcst_date = item.get("fcstDate", item["baseDate"])
        fcst_time = item.get("fcstTime", item["baseTime"])
        forecast_dt = parse_datetime(fcst_date, fcst_time)
        key = (base_dt, forecast_dt)
        row = grouped.setdefault(
            key,
            {
                "dam_code": dam["dam_code"],
                "base_datetime": base_dt,
                "forecast_datetime": forecast_dt,
                "grid_x": dam["grid_x"],
                "grid_y": dam["grid_y"],
                "tmp": None,
                "rain": None,
                "snow": None,
                "pty": None,
                "source": source,
                "raw_payload": [],
            },
        )
        category = item.get("category")
        value = item.get("fcstValue", item.get("obsrValue"))
        row["raw_payload"].append(item)
        if category in {"T1H", "TMP"}:
            try:
                row["tmp"] = float(value)
            except (TypeError, ValueError):
                row["tmp"] = None
        elif category in {"RN1", "PCP"}:
            row["rain"] = parse_amount(value)
        elif category == "SNO":
            row["snow"] = parse_amount(value)
        elif category == "PTY":
            row["pty"] = str(value)

    for row in grouped.values():
        if row["rain"] is None:
            row["rain"] = 0.0
        if row["snow"] is None:
            row["snow"] = 0.0
    return list(grouped.values())


def upsert_weather(rows: list[dict]) -> int:
    if not rows:
        return 0
    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO dam_weather_forecast
                    (dam_code, base_datetime, forecast_datetime, grid_x, grid_y,
                     tmp, rain, snow, pty, source, raw_payload)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    tmp = VALUES(tmp),
                    rain = VALUES(rain),
                    snow = VALUES(snow),
                    pty = VALUES(pty),
                    raw_payload = VALUES(raw_payload)
            """
            values = [
                (
                    row["dam_code"],
                    row["base_datetime"],
                    row["forecast_datetime"],
                    row["grid_x"],
                    row["grid_y"],
                    row["tmp"],
                    row["rain"],
                    row["snow"],
                    row["pty"],
                    row["source"],
                    json.dumps(row["raw_payload"], ensure_ascii=False),
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
    parser.add_argument("--mode", choices=["forecast", "nowcast", "both"], default="both")
    parser.add_argument("--limit", type=int, default=0, help="테스트용 댐 개수 제한. 0이면 전체 20개")
    args = parser.parse_args()

    service_key = get_service_key()
    dams = load_dam_master()
    if args.limit:
        dams = dams[: args.limit]

    now = datetime.now()
    total = 0
    failures = []
    for dam in dams:
        rows = []
        try:
            if args.mode in {"forecast", "both"}:
                items = fetch_ultra_forecast(service_key, dam["grid_x"], dam["grid_y"], now)
                rows.extend(normalize_items(dam, items, "KMA_ULTRA_FCST"))
            if args.mode in {"nowcast", "both"}:
                items = fetch_ultra_nowcast(service_key, dam["grid_x"], dam["grid_y"], now)
                rows.extend(normalize_items(dam, items, "KMA_ULTRA_NCST"))
            inserted = upsert_weather(rows)
            total += inserted
            print(f"{dam['dam_name']} 기상청 초단기 자료 저장: {inserted}건")
        except Exception as exc:
            failures.append((dam["dam_name"], str(exc)))
            print(f"{dam['dam_name']} 기상청 초단기 자료 수집 실패, 기존 DB 값을 사용합니다.")

    print(f"기상청 초단기 자료 저장 완료: {total}건")
    if failures:
        print(f"부분 실패: {len(failures)}개 댐")
        for dam_name, reason in failures:
            print(f"- {dam_name}: {reason}")

    if total == 0 and failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
