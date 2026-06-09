from __future__ import annotations

import sys
from pathlib import Path

import joblib
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
API_DIR = BASE_DIR.parent / "01_API수집"
sys.path.insert(0, str(API_DIR))

from api_공통 import connect_mysql  # noqa: E402


INPUT_PATH = BASE_DIR.parent / "02_실시간입력" / "실시간 예측 입력" / "실시간_모델입력_최신.csv"
OUTPUT_DIR = BASE_DIR / "실시간 예측 결과"
MODEL_DIR = BASE_DIR.parent / "04_저장모델" / "models"
OUTPUT_DIR.mkdir(exist_ok=True)


def level_from_probability(prob: float, confidence: str) -> tuple[str, str, str]:
    if confidence == "보류":
        return "보류", "예측 보류", "회색"
    if prob >= 0.7:
        return "높음", "방류 조정 검토", "빨강"
    if prob >= 0.45:
        return "보통", "주의 관찰", "주황"
    if prob >= 0.25:
        return "낮음", "주의 관찰", "노랑"
    return "낮음", "현재 방류 유지", "초록"


def confidence_from_summary(row: pd.Series) -> str:
    auc = row.get("change_auc")
    change_rate = row.get("change_rate", 0)
    if pd.isna(auc) or change_rate == 0:
        return "보류"
    if auc >= 0.85 and row.get("train_rows_discharge", 0) >= 1000:
        return "높음"
    if auc >= 0.75:
        return "보통"
    return "낮음"


def upsert_predictions(result: pd.DataFrame) -> None:
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
                    None if pd.isna(row.predicted_inflow_3h) else float(row.predicted_inflow_3h),
                    None if pd.isna(row.predicted_discharge_3h) else float(row.predicted_discharge_3h),
                    row.discharge_change_level,
                    row.release_review,
                    row.confidence_level,
                    row.model_version,
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


def pending_row(row: pd.Series, bundle: dict, missing_features: list[str], predicted_inflow=pd.NA) -> dict:
    return {
        "dam_code": int(row["dam_code"]),
        "dam_name": row["dam_name"],
        "obsrdt": row["obsrdt"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "current_inflow": row["inflowqy"],
        "current_discharge": row["totdcwtrqy"],
        "predicted_inflow_3h": predicted_inflow,
        "predicted_discharge_3h": pd.NA,
        "discharge_change_probability": pd.NA,
        "discharge_change_level": "보류",
        "release_review": "예측 보류",
        "confidence_level": "보류",
        "marker_color": "회색",
        "model_version": bundle.get("model_version", "ml-v0.1"),
        "missing_features": ",".join(missing_features),
    }


def main() -> None:
    latest = pd.read_csv(INPUT_PATH, parse_dates=["obsrdt"])
    summary = pd.read_csv(MODEL_DIR / "모델학습_요약.csv")
    summary_map = {int(row.dam_code): row for row in summary.itertuples(index=False)}

    rows = []
    for _, row in latest.iterrows():
        model_path = MODEL_DIR / f"dam_{int(row['dam_code'])}_models.joblib"
        bundle = joblib.load(model_path)
        inflow_features = bundle["inflow_features"]
        discharge_features = bundle["discharge_features"]

        inflow_x = row[inflow_features].to_frame().T
        missing_inflow = [feature for feature in inflow_features if pd.isna(inflow_x.iloc[0][feature])]
        if missing_inflow:
            rows.append(pending_row(row, bundle, missing_inflow))
            continue

        predicted_inflow = float(bundle["inflow_model"].predict(inflow_x)[0])
        discharge_row = row.copy()
        discharge_row["predicted_inflow_3h"] = predicted_inflow
        discharge_x = discharge_row[discharge_features].to_frame().T
        missing_discharge = [feature for feature in discharge_features if pd.isna(discharge_x.iloc[0][feature])]
        if missing_discharge:
            rows.append(pending_row(row, bundle, missing_discharge, predicted_inflow))
            continue

        predicted_discharge = float(bundle["discharge_model"].predict(discharge_x)[0])
        change_prob = float(bundle["change_model"].predict_proba(discharge_x)[0, 1])

        summary_row = pd.Series(summary_map[int(row["dam_code"])]._asdict())
        confidence = confidence_from_summary(summary_row)
        level, review, color = level_from_probability(change_prob, confidence)

        rows.append(
            {
                "dam_code": int(row["dam_code"]),
                "dam_name": row["dam_name"],
                "obsrdt": row["obsrdt"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "current_inflow": row["inflowqy"],
                "current_discharge": row["totdcwtrqy"],
                "predicted_inflow_3h": predicted_inflow,
                "predicted_discharge_3h": predicted_discharge,
                "discharge_change_probability": change_prob,
                "discharge_change_level": level,
                "release_review": review,
                "confidence_level": confidence,
                "marker_color": color,
                "model_version": bundle["model_version"],
                "missing_features": "",
            }
        )

    result = pd.DataFrame(rows).sort_values("discharge_change_probability", ascending=False, na_position="last")
    result.to_csv(OUTPUT_DIR / "ML_방류량_변화예측_최신.csv", index=False, encoding="utf-8-sig")
    upsert_predictions(result)
    print("실시간 ML 예측 완료")
    print(
        result[
            [
                "dam_name",
                "predicted_inflow_3h",
                "predicted_discharge_3h",
                "discharge_change_probability",
                "discharge_change_level",
                "release_review",
                "confidence_level",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
