from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import requests

from api_공통 import get_service_key, load_dotenv_if_exists


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR.parent / "07_하류수위"
OUTPUT_DIR.mkdir(exist_ok=True)

BASE_URL = "https://apis.data.go.kr/B500001/dam/excllncobsrvt"
WATER_LEVEL_STATION_PATH_CANDIDATES = ["/wal/wallist", "/wal/walList", "/rwl/walList"]


def redact_service_key(text: str) -> str:
    key = get_service_key()
    return text.replace(key, "[SERVICE_KEY_REDACTED]")


def extract_items(payload: dict) -> list[dict]:
    body = payload.get("response", {}).get("body", {})
    items = body.get("items", {})
    item = items.get("item", []) if isinstance(items, dict) else items
    if isinstance(item, dict):
        return [item]
    if isinstance(item, list):
        return item
    return []


def call_api(path: str, params: dict) -> dict:
    response = requests.get(f"{BASE_URL}{path}", params=params, timeout=30)
    if response.status_code >= 400:
        raise RuntimeError(
            f"API 요청 실패: status={response.status_code}, url={redact_service_key(response.url)}, "
            f"body={response.text[:300]}"
        )
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"JSON 응답이 아닙니다. 응답 앞부분: {response.text[:500]}") from exc


def fetch_water_level_station_codes(damcode: str, num_rows: int) -> pd.DataFrame:
    params = {
        "serviceKey": get_service_key(),
        "damcode": damcode,
        "pageNo": 1,
        "numOfRows": num_rows,
        "_type": "json",
    }
    last_error = None
    payload = None
    used_path = None
    for path in WATER_LEVEL_STATION_PATH_CANDIDATES:
        try:
            payload = call_api(path, params)
            used_path = path
            break
        except Exception as exc:
            last_error = exc
    if payload is None:
        raise RuntimeError(f"수위 관측소 코드 조회 실패: {last_error}") from last_error

    (OUTPUT_DIR / "수위관측소_코드조회_원본.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    items = extract_items(payload)
    frame = pd.DataFrame(items)
    if not frame.empty:
        frame.to_csv(OUTPUT_DIR / "수위관측소_코드조회_샘플.csv", index=False, encoding="utf-8-sig")
    print(f"사용한 상세 경로: {used_path}")
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description="K-water 우량수위 관측소 API 응답 구조 확인")
    parser.add_argument("--damcode", default="2018110", help="댐코드. 기본값: 남강댐")
    parser.add_argument("--num-rows", type=int, default=100, help="코드 조회 샘플 개수")
    args = parser.parse_args()

    load_dotenv_if_exists()
    stations = fetch_water_level_station_codes(args.damcode, args.num_rows)

    print("수위 관측소 코드 조회 완료")
    print(f"저장 폴더: {OUTPUT_DIR}")
    print(f"행 수: {len(stations)}")
    if stations.empty:
        print("응답 item이 비어 있습니다. 원본 JSON을 확인하세요.")
        return
    print("컬럼 목록:")
    print(", ".join(stations.columns.astype(str)))
    print("샘플 10건:")
    print(stations.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
