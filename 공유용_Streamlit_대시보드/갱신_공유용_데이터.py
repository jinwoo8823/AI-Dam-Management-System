from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = BASE_DIR / "data"
EXPERIMENT_DIR = PROJECT_ROOT / "실험_방류량_3h_6h_1d_5d"
API_DATA_DIR = EXPERIMENT_DIR / "api랑 연동 데이터"
SNAPSHOT_PATH = API_DATA_DIR / "02_실시간입력" / "실시간 예측 입력" / "대시보드_현재상태_스냅샷.csv"
PREDICTION_PATH = API_DATA_DIR / "03_실시간예측" / "실시간 예측 결과" / "ML_방류량_변화예측_최신.csv"
VALIDATION_PATH = API_DATA_DIR / "05_검증" / "실시간예측_실제값검증결과.csv"
MASTER_PATH = DATA_DIR / "dam_master.csv"
EXPERIMENT_DASHBOARD_DATA_DIR = EXPERIMENT_DIR / "대시보드" / "data"
API_SCRIPT_DIR = API_DATA_DIR / "01_API수집"
sys.path.insert(0, str(API_SCRIPT_DIR))

from api_공통 import connect_mysql  # noqa: E402


def find_training_data() -> Path:
    candidates = sorted(PROJECT_ROOT.glob("안정버전_*/final_data_20_weather.csv"))
    if not candidates:
        raise FileNotFoundError("안정버전의 final_data_20_weather.csv를 찾을 수 없습니다.")
    return candidates[0]


def update_latest_summary() -> None:
    snapshot = pd.read_csv(SNAPSHOT_PATH, encoding="utf-8-sig")
    predictions = pd.read_csv(PREDICTION_PATH, encoding="utf-8-sig")

    prediction_columns = [
        "dam_code",
        "obsrdt",
        "predicted_inflow_3h",
        "predicted_discharge_3h",
        "discharge_change_level",
        "release_review",
        "confidence_level",
        "model_version",
    ]
    merged = snapshot.merge(
        predictions[prediction_columns],
        on=["dam_code", "obsrdt"],
        how="left",
    )
    merged["pred_base_time"] = merged["obsrdt"]
    merged["pred_target_time"] = pd.to_datetime(merged["obsrdt"]) + pd.Timedelta(hours=3)

    output_columns = [
        "dam_code",
        "dam_name",
        "latitude",
        "longitude",
        "grid_x",
        "grid_y",
        "obsrdt",
        "inflowqy",
        "lowlevel",
        "rf",
        "rsvwtqy",
        "rsvwtrt",
        "totdcwtrqy",
        "pred_base_time",
        "pred_target_time",
        "predicted_inflow_3h",
        "predicted_discharge_3h",
        "discharge_change_level",
        "release_review",
        "confidence_level",
        "model_version",
    ]
    merged[output_columns].to_csv(
        DATA_DIR / "latest_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    print(f"공유용 최신 요약 갱신: {merged['obsrdt'].max()}")


def update_validation() -> None:
    if not VALIDATION_PATH.exists():
        return
    validation = pd.read_csv(VALIDATION_PATH, encoding="utf-8-sig")
    validation.to_csv(
        DATA_DIR / "prediction_validation.csv",
        index=False,
        encoding="utf-8-sig",
    )
    print(f"공유용 실시간 검증 갱신: {len(validation):,}건")


def update_db_snapshots() -> None:
    try:
        conn = connect_mysql()
    except Exception as exc:
        print(f"MySQL 연결 생략: 기존 공유용 이력/예보 유지 ({exc})")
        return

    try:
        history = pd.read_sql(
            """
            SELECT
                d.dam_name,
                o.dam_code,
                o.obsrdt,
                o.inflowqy,
                o.lowlevel,
                o.rf,
                o.rsvwtqy,
                o.rsvwtrt,
                o.totdcwtrqy
            FROM dam_realtime_observation o
            JOIN dam_code d ON d.dam_code = o.dam_code
            WHERE o.obsrdt >= (
                SELECT DATE_SUB(MAX(obsrdt), INTERVAL 72 HOUR)
                FROM dam_realtime_observation
            )
            ORDER BY o.dam_code, o.obsrdt
            """,
            conn,
        )
        weather = pd.read_sql(
            """
            SELECT
                dam_code,
                base_datetime,
                forecast_datetime,
                grid_x,
                grid_y,
                tmp,
                rain,
                snow,
                pty,
                source,
                raw_payload,
                created_at,
                updated_at
            FROM dam_weather_forecast
            WHERE base_datetime = (
                SELECT MAX(base_datetime)
                FROM dam_weather_forecast
            )
            ORDER BY dam_code, forecast_datetime
            """,
            conn,
        )
        if not history.empty:
            history.to_csv(DATA_DIR / "observation_history_72h.csv", index=False, encoding="utf-8-sig")
            print(f"공유용 최근 72시간 이력 갱신: {history['obsrdt'].max()}")
        if not weather.empty:
            weather.to_csv(DATA_DIR / "weather_forecast_latest.csv", index=False, encoding="utf-8-sig")
            print(f"공유용 기상예보 갱신: {weather['base_datetime'].max()}")
    except Exception as exc:
        print(f"MySQL 스냅샷 갱신 생략: 기존 공유용 이력/예보 유지 ({exc})")
    finally:
        conn.close()


def build_interest_history() -> None:
    raw = pd.read_csv(find_training_data(), encoding="utf-8-sig", parse_dates=["obsrdt"])
    master = pd.read_csv(MASTER_PATH, encoding="utf-8-sig")
    name_map = master.set_index("dam_code")["dam_name"].to_dict()

    numeric_columns = ["inflowqy", "lowlevel", "rf", "rsvwtqy", "rsvwtrt", "totdcwtrqy", "rain"]
    for column in numeric_columns:
        raw[column] = pd.to_numeric(raw[column], errors="coerce")
    raw["rain"] = raw["rain"].fillna(raw["rf"]).fillna(0)
    raw["rf"] = raw["rf"].fillna(0)
    raw["dam_name"] = raw["dam_code"].map(name_map)

    frames: list[pd.DataFrame] = []
    for dam_code, dam in raw.groupby("dam_code"):
        dam = dam.sort_values("obsrdt").copy()
        dam["rain_sum_24h"] = dam["rain"].rolling(24, min_periods=1).sum()
        inflow_threshold = dam["inflowqy"].quantile(0.75)
        rain_now = dam["rain"] > 0
        rain_recent = dam["rain_sum_24h"] > 0
        high_inflow = (dam["inflowqy"] > 0) & (dam["inflowqy"] >= inflow_threshold)
        interest = dam[rain_now | rain_recent | high_inflow].copy()

        reasons = []
        for _, row in interest.iterrows():
            labels = []
            if row["rain"] > 0:
                labels.append("관측 강수 발생")
            if row["rain_sum_24h"] > 0:
                labels.append("최근 24시간 누적강수")
            if row["inflowqy"] > 0 and row["inflowqy"] >= inflow_threshold:
                labels.append("유입량 과거 상위 25%")
            reasons.append(", ".join(labels))
        interest["interest_reason"] = reasons
        frames.append(interest)

    filtered = pd.concat(frames, ignore_index=True)
    filtered["obsrdt"] = filtered["obsrdt"].dt.floor("D")

    def merge_reasons(values: pd.Series) -> str:
        labels = []
        for value in values:
            for label in str(value).split(", "):
                if label and label not in labels:
                    labels.append(label)
        return ", ".join(labels)

    daily = (
        filtered.groupby(["dam_code", "dam_name", "obsrdt"], as_index=False)
        .agg(
            rain=("rain", "max"),
            rain_sum_24h=("rain_sum_24h", "max"),
            inflowqy=("inflowqy", "max"),
            totdcwtrqy=("totdcwtrqy", "max"),
            lowlevel=("lowlevel", "max"),
            rsvwtrt=("rsvwtrt", "max"),
            interest_reason=("interest_reason", merge_reasons),
        )
        .sort_values(["dam_code", "obsrdt"])
    )
    daily.to_csv(
        DATA_DIR / "interest_period_history.csv",
        index=False,
        encoding="utf-8-sig",
    )
    EXPERIMENT_DASHBOARD_DATA_DIR.mkdir(exist_ok=True)
    daily.to_csv(
        EXPERIMENT_DASHBOARD_DATA_DIR / "interest_period_history.csv",
        index=False,
        encoding="utf-8-sig",
    )

    thresholds = []
    for dam_code, dam in raw.groupby("dam_code"):
        dam = dam.sort_values("obsrdt").copy()
        inflow = dam["inflowqy"].dropna()
        discharge_increase_3h = (dam["totdcwtrqy"] - dam["totdcwtrqy"].shift(3)).clip(lower=0).dropna()
        thresholds.append(
            {
                "dam_code": int(dam_code),
                "dam_name": name_map.get(int(dam_code), ""),
                "inflow_q90": inflow.quantile(0.90),
                "inflow_q95": inflow.quantile(0.95),
                "discharge_increase_3h_q95": max(1.0, discharge_increase_3h.quantile(0.95)),
            }
        )
    threshold_df = pd.DataFrame(thresholds)
    threshold_df.to_csv(DATA_DIR / "risk_thresholds.csv", index=False, encoding="utf-8-sig")
    threshold_df.to_csv(
        EXPERIMENT_DASHBOARD_DATA_DIR / "risk_thresholds.csv",
        index=False,
        encoding="utf-8-sig",
    )
    print(f"강수·고유입 관심구간 일별 요약 생성: {len(daily):,}건")
    print(f"내부 운영 참고 신호 댐별 기준 생성: {len(threshold_df):,}건")


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    update_latest_summary()
    update_validation()
    update_db_snapshots()
    build_interest_history()


if __name__ == "__main__":
    main()
