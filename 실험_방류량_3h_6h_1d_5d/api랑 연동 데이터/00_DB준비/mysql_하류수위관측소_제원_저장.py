from __future__ import annotations

from math import asin, cos, radians, sin, sqrt
from pathlib import Path
import sys

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
API_DIR = BASE_DIR.parent / "01_API수집"
sys.path.insert(0, str(API_DIR))

from api_공통 import connect_mysql, load_dam_master  # noqa: E402


DATA_DIR = BASE_DIR.parent / "07_하류수위"
STATION_SPEC_PATH = DATA_DIR / "수위관측소_제원정보.csv"
CANDIDATE_OUTPUT_PATH = DATA_DIR / "20개댐_하류수위관측소_거리기반후보.csv"
DDL_PATH = BASE_DIR / "mysql_하류수위관측소_테이블.sql"


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * radius * asin(sqrt(a))


def read_station_specs() -> pd.DataFrame:
    for encoding in ("cp949", "utf-8-sig", "euc-kr"):
        try:
            frame = pd.read_csv(STATION_SPEC_PATH, encoding=encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise RuntimeError(f"수위관측소 제원정보 인코딩을 확인할 수 없습니다: {STATION_SPEC_PATH}")

    frame = frame.rename(
        columns={
            "수위관측소 코드": "station_code",
            "수위관측소 명": "station_name",
            "수계": "basin",
            "경도": "longitude",
            "위도": "latitude",
            "관측개시일": "observation_start_year",
        }
    )
    frame["station_code"] = frame["station_code"].astype(str)
    return frame


def build_candidates(stations: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    dams = load_dam_master()
    rows = []
    for dam in dams:
        tmp = stations.copy()
        tmp["distance_km"] = tmp.apply(
            lambda row: haversine_km(
                float(dam["latitude"]),
                float(dam["longitude"]),
                float(row["latitude"]),
                float(row["longitude"]),
            ),
            axis=1,
        )
        for rank, (_, row) in enumerate(tmp.sort_values("distance_km").head(top_n).iterrows(), start=1):
            rows.append(
                {
                    "dam_code": int(dam["dam_code"]),
                    "dam_name": dam["dam_name"],
                    "station_code": str(row["station_code"]),
                    "station_name": row["station_name"],
                    "basin": row["basin"],
                    "distance_km": round(float(row["distance_km"]), 2),
                    "rank_no": rank,
                    "mapping_type": "DISTANCE_CANDIDATE",
                    "note": "거리 기반 후보입니다. 공식 하류 대표 관측소 여부는 추가 확인 필요",
                }
            )
    return pd.DataFrame(rows)


def execute_ddl() -> None:
    sql_text = DDL_PATH.read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]
    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
        conn.commit()
    finally:
        conn.close()


def upsert_stations(stations: pd.DataFrame) -> int:
    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO downstream_station
                    (station_code, station_name, basin, longitude, latitude, observation_start_year)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    station_name = VALUES(station_name),
                    basin = VALUES(basin),
                    longitude = VALUES(longitude),
                    latitude = VALUES(latitude),
                    observation_start_year = VALUES(observation_start_year)
            """
            values = [
                (
                    row.station_code,
                    row.station_name,
                    None if pd.isna(row.basin) else row.basin,
                    None if pd.isna(row.longitude) else float(row.longitude),
                    None if pd.isna(row.latitude) else float(row.latitude),
                    None if pd.isna(row.observation_start_year) else int(row.observation_start_year),
                )
                for row in stations.itertuples(index=False)
            ]
            cur.executemany(sql, values)
        conn.commit()
        return len(values)
    finally:
        conn.close()


def upsert_candidates(candidates: pd.DataFrame) -> int:
    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO dam_downstream_station_map
                    (dam_code, dam_name, station_code, station_name, basin, distance_km, rank_no, mapping_type, note)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    dam_name = VALUES(dam_name),
                    station_name = VALUES(station_name),
                    basin = VALUES(basin),
                    distance_km = VALUES(distance_km),
                    rank_no = VALUES(rank_no),
                    mapping_type = VALUES(mapping_type),
                    note = VALUES(note)
            """
            values = [
                (
                    int(row.dam_code),
                    row.dam_name,
                    str(row.station_code),
                    row.station_name,
                    None if pd.isna(row.basin) else row.basin,
                    None if pd.isna(row.distance_km) else float(row.distance_km),
                    int(row.rank_no),
                    row.mapping_type,
                    row.note,
                )
                for row in candidates.itertuples(index=False)
            ]
            cur.executemany(sql, values)
        conn.commit()
        return len(values)
    finally:
        conn.close()


def upsert_representatives() -> int:
    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            sql = """
                REPLACE INTO dam_representative_downstream_station
                    (dam_code, dam_name, station_code, station_name, basin, distance_km,
                     latitude, longitude, selection_type, selection_reason, note)
                SELECT
                    ranked.dam_code,
                    ranked.dam_name,
                    ranked.station_code,
                    ranked.station_name,
                    ranked.basin,
                    ranked.distance_km,
                    ranked.latitude,
                    ranked.longitude,
                    'AUTO_SELECTED' AS selection_type,
                    '공식 API 연결 후보 중 실제 수위 시자료와 제원 좌표가 확인되는 관측소를 우선하고, 그중 댐과의 직선거리가 가장 가까운 관측소를 선정' AS selection_reason,
                    '추후 수계도 확인 후 MANUAL_CONFIRMED로 변경 가능' AS note
                FROM (
                    SELECT
                        m.dam_code,
                        m.dam_name,
                        m.station_code,
                        m.station_name,
                        COALESCE(m.basin, s.basin) AS basin,
                        m.distance_km,
                        s.latitude,
                        s.longitude,
                        ROW_NUMBER() OVER (
                            PARTITION BY m.dam_code
                            ORDER BY
                                CASE WHEN o.station_code IS NULL THEN 1 ELSE 0 END,
                                m.distance_km IS NULL,
                                CASE WHEN s.latitude IS NULL OR s.longitude IS NULL THEN 1 ELSE 0 END,
                                m.distance_km,
                                CASE WHEN m.mapping_type LIKE '%OFFICIAL%' THEN 0 ELSE 1 END,
                                m.rank_no
                        ) AS rn
                    FROM dam_downstream_station_map m
                    JOIN downstream_station s
                      ON s.station_code = m.station_code
                    LEFT JOIN (
                        SELECT DISTINCT station_code
                        FROM downstream_water_level_observation
                    ) o
                      ON o.station_code = m.station_code
                ) ranked
                WHERE ranked.rn = 1
            """
            affected = cur.execute(sql)
        conn.commit()
        return affected
    finally:
        conn.close()


def main() -> None:
    execute_ddl()
    stations = read_station_specs()
    candidates = build_candidates(stations)
    candidates.to_csv(CANDIDATE_OUTPUT_PATH, index=False, encoding="utf-8-sig")

    station_count = upsert_stations(stations)
    candidate_count = upsert_candidates(candidates)
    representative_count = upsert_representatives()

    print(f"수위관측소 제원 저장 완료: {station_count}개")
    print(f"20개 댐 거리 기반 후보 저장 완료: {candidate_count}개")
    print(f"대표 하류 수위관측소 자동 선정 완료: {representative_count}개")
    print(f"후보 CSV: {CANDIDATE_OUTPUT_PATH}")
    print(candidates.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
