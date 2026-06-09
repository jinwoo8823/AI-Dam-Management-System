from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

from api_공통 import get_service_key, load_dotenv_if_exists


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR.parent / "07_하류수위"
OUTPUT_DIR.mkdir(exist_ok=True)

BASE_URL = "https://apis.data.go.kr/B500001/dam/excllncobsrvt"
HOURLY_WATER_LEVEL_PATH = "/hourwal/hourwallist"


def redact_service_key(text: str) -> str:
    return text.replace(get_service_key(), "[SERVICE_KEY_REDACTED]")


def extract_items(payload: dict) -> list[dict]:
    body = payload.get("response", {}).get("body", {})
    items = body.get("items", {})
    item = items.get("item", []) if isinstance(items, dict) else items
    if isinstance(item, dict):
        return [item]
    if isinstance(item, list):
        return item
    return []


def call_hourly_water_level(damcode: str, station_code: str, start: datetime, end: datetime, tms: str) -> tuple[dict, str]:
    params = {
        "serviceKey": get_service_key(),
        "pageNo": 1,
        "numOfRows": 1000,
        "sdate": start.strftime("%Y-%m-%d"),
        "stime": start.strftime("%H"),
        "edate": end.strftime("%Y-%m-%d"),
        "etime": end.strftime("%H"),
        "damcode": damcode,
        "wal": station_code,
        "tms": tms,
        "_type": "json",
    }
    response = requests.get(f"{BASE_URL}{HOURLY_WATER_LEVEL_PATH}", params=params, timeout=30)
    if response.status_code >= 400:
        raise RuntimeError(
            f"API 요청 실패: status={response.status_code}, url={redact_service_key(response.url)}, "
            f"body={response.text[:300]}"
        )
    try:
        return response.json(), response.url
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"JSON 응답이 아닙니다. 응답 앞부분: {response.text[:500]}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="K-water 하류 수위 시자료 API 응답 구조 확인")
    parser.add_argument("--damcode", default="2018110", help="댐코드. 기본값: 남강댐")
    parser.add_argument("--station-code", default="2018695", help="수위관측소 코드. 기본값: 남강댐 인근 진주시(판문동)")
    parser.add_argument("--hours", type=int, default=72, help="조회 시간 범위")
    parser.add_argument("--tms", default="60", help="조회시간단위. 시간자료는 60 우선 사용")
    args = parser.parse_args()

    load_dotenv_if_exists()
    end = datetime.now()
    start = end - timedelta(hours=args.hours)
    payload, url = call_hourly_water_level(args.damcode, args.station_code, start, end, args.tms)

    safe_code = str(args.station_code)
    (OUTPUT_DIR / f"수위시자료_{safe_code}_원본.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    items = extract_items(payload)
    frame = pd.DataFrame(items)
    if not frame.empty:
        frame.to_csv(OUTPUT_DIR / f"수위시자료_{safe_code}_샘플.csv", index=False, encoding="utf-8-sig")

    header = payload.get("response", {}).get("header", {})
    body = payload.get("response", {}).get("body", {})
    print("하류 수위 시자료 조회 완료")
    print(f"요청 URL: {redact_service_key(url)}")
    print(f"응답 코드: {header.get('resultCode')} / {header.get('resultMsg')}")
    print(f"totalCount: {body.get('totalCount')}")
    print(f"행 수: {len(frame)}")
    if frame.empty:
        print("응답 item이 비어 있습니다. 원본 JSON을 확인하세요.")
        return
    print("컬럼 목록:")
    print(", ".join(frame.columns.astype(str)))
    print("샘플 10건:")
    print(frame.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
