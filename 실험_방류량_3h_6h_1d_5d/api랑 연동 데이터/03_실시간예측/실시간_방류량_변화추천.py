from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
API_DIR = BASE_DIR.parent / "01_API수집"
sys.path.insert(0, str(API_DIR))

from api_공통 import connect_mysql  # noqa: E402


INPUT_PATH = BASE_DIR.parent / "02_실시간입력" / "실시간 예측 입력" / "대시보드_현재상태_스냅샷.csv"
OUTPUT_DIR = BASE_DIR / "실시간 예측 결과"
OUTPUT_DIR.mkdir(exist_ok=True)


RELIABILITY = {
    "남강": "높음",
    "주암(조)": "높음",
    "섬진강": "높음",
    "용담": "높음",
    "대청": "높음",
    "합천": "높음",
    "소양강": "보통",
    "충주": "보통",
    "주암(본)": "보통",
    "임하": "보통",
    "보령": "보통",
    "안동": "낮음",
    "횡성": "낮음",
    "부안": "낮음",
    "밀양": "낮음",
    "영주": "낮음",
    "장흥": "낮음",
    "성덕": "보류",
    "군위": "보류",
    "보현산": "보류",
}


def score_row(row: pd.Series) -> tuple[int, list[str]]:
    score = 0
    reasons = []

    rain_24h = row.get("rain_sum_24h", 0) or 0
    rain_72h = row.get("rain_sum_72h", 0) or 0
    forecast_rain = row.get("forecast_rain_sum", 0) or 0
    rsvwtrt = row.get("rsvwtrt", 0) or 0
    inflow = row.get("inflowqy", 0) or 0
    inflow_mean_24h = row.get("inflow_mean_24h", 0) or 0
    discharge = row.get("totdcwtrqy", 0) or 0
    discharge_mean_24h = row.get("discharge_mean_24h", 0) or 0

    if rain_24h >= 20:
        score += 3
        reasons.append("최근 24시간 누적강수 많음")
    elif rain_24h >= 5:
        score += 2
        reasons.append("최근 24시간 누적강수 있음")
    elif rain_72h >= 10:
        score += 1
        reasons.append("최근 72시간 누적강수 있음")

    if forecast_rain >= 10:
        score += 3
        reasons.append("초단기예보 강수 많음")
    elif forecast_rain > 0:
        score += 2
        reasons.append("초단기예보 강수 있음")

    if inflow_mean_24h and inflow >= inflow_mean_24h * 1.5:
        score += 3
        reasons.append("현재 유입량이 24시간 평균보다 큼")
    elif inflow_mean_24h and inflow >= inflow_mean_24h * 1.2:
        score += 2
        reasons.append("현재 유입량 증가 경향")

    if rsvwtrt >= 85:
        score += 3
        reasons.append("저수율 높음")
    elif rsvwtrt >= 75:
        score += 2
        reasons.append("저수율 주의 구간")

    if discharge_mean_24h and discharge >= discharge_mean_24h * 1.3:
        score += 1
        reasons.append("현재 방류량이 최근 평균보다 큼")

    return score, reasons


def level_from_score(score: int, confidence: str) -> tuple[str, str, str]:
    if confidence == "보류":
        return "보류", "데이터/모델 보류", "회색"
    if score >= 8:
        return "높음", "검토 필요", "빨강"
    if score >= 5:
        return "보통", "관찰 필요", "주황"
    if score >= 2:
        return "낮음", "관찰", "노랑"
    return "낮음", "불필요", "초록"


def upsert_prediction_results(result: pd.DataFrame) -> None:
    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO dam_prediction_result
                    (dam_code, pred_base_time, pred_target_time,
                     predicted_inflow_3h, predicted_discharge_3h,
                     discharge_change_level, release_review, confidence_level,
                     model_version)
                VALUES
                    (%s, %s, DATE_ADD(%s, INTERVAL 3 HOUR), %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    predicted_inflow_3h = VALUES(predicted_inflow_3h),
                    predicted_discharge_3h = VALUES(predicted_discharge_3h),
                    discharge_change_level = VALUES(discharge_change_level),
                    release_review = VALUES(release_review),
                    confidence_level = VALUES(confidence_level),
                    model_version = VALUES(model_version)
            """
            values = [
                (
                    int(row.dam_code),
                    row.obsrdt,
                    row.obsrdt,
                    None,
                    None,
                    row.discharge_change_level,
                    row.release_review,
                    row.confidence_level,
                    "rule-v0.1",
                )
                for row in result.itertuples(index=False)
            ]
            cur.executemany(sql, values)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    data = pd.read_csv(INPUT_PATH, parse_dates=["obsrdt"])
    rows = []
    for _, row in data.iterrows():
        confidence = RELIABILITY.get(row["dam_name"], "낮음")
        score, reasons = score_row(row)
        level, review, color = level_from_score(score, confidence)
        rows.append(
            {
                "dam_code": row["dam_code"],
                "dam_name": row["dam_name"],
                "obsrdt": row["obsrdt"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "current_inflow": row["inflowqy"],
                "current_discharge": row["totdcwtrqy"],
                "rsvwtrt": row["rsvwtrt"],
                "rain_sum_24h": row.get("rain_sum_24h"),
                "rain_sum_72h": row.get("rain_sum_72h"),
                "forecast_rain_sum": row.get("forecast_rain_sum"),
                "risk_score": score,
                "discharge_change_level": level,
                "release_review": review,
                "confidence_level": confidence,
                "marker_color": color,
                "reason": ", ".join(reasons) if reasons else "특이사항 없음",
            }
        )

    result = pd.DataFrame(rows).sort_values(["risk_score", "confidence_level"], ascending=[False, True])
    result.to_csv(OUTPUT_DIR / "방류량_변화추천_최신.csv", index=False, encoding="utf-8-sig")
    upsert_prediction_results(result)

    print("방류량 변화 가능성/추천 생성 완료")
    print(
        result[
            ["dam_name", "risk_score", "discharge_change_level", "release_review", "confidence_level", "marker_color", "reason"]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
