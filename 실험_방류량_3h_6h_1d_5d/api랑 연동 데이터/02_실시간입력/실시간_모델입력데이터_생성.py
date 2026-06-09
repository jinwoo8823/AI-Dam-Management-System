from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
API_DIR = BASE_DIR.parent / "01_API수집"
sys.path.insert(0, str(API_DIR))

from api_공통 import connect_mysql  # noqa: E402


OUTPUT_DIR = BASE_DIR / "실시간 예측 입력"
OUTPUT_DIR.mkdir(exist_ok=True)


def load_realtime_observations() -> pd.DataFrame:
    conn = connect_mysql()
    try:
        query = """
            SELECT
                o.dam_code,
                d.dam_name,
                d.latitude,
                d.longitude,
                d.grid_x,
                d.grid_y,
                o.obsrdt,
                o.inflowqy,
                o.lowlevel,
                o.rf,
                o.rsvwtqy,
                o.rsvwtrt,
                o.totdcwtrqy,
                o.tmp,
                o.rain,
                o.snow
            FROM dam_realtime_observation o
            JOIN dam_code d ON d.dam_code = o.dam_code
            WHERE o.obsrdt >= (
                SELECT DATE_SUB(MAX(obsrdt), INTERVAL 120 HOUR)
                FROM dam_realtime_observation
            )
            ORDER BY o.dam_code, o.obsrdt
        """
        return pd.read_sql(query, conn, parse_dates=["obsrdt"])
    finally:
        conn.close()


def load_weather_forecasts() -> pd.DataFrame:
    conn = connect_mysql()
    try:
        query = """
            SELECT
                dam_code,
                base_datetime,
                forecast_datetime,
                tmp,
                rain,
                snow,
                pty,
                source
            FROM dam_weather_forecast
            WHERE base_datetime = (
                SELECT MAX(base_datetime)
                FROM dam_weather_forecast
            )
            ORDER BY dam_code, forecast_datetime
        """
        return pd.read_sql(
            query,
            conn,
            parse_dates=["base_datetime", "forecast_datetime"],
        )
    finally:
        conn.close()


def build_hourly_model_input(obs: pd.DataFrame) -> pd.DataFrame:
    df = obs.copy()
    df["obsrdt"] = pd.to_datetime(df["obsrdt"])
    df = df.sort_values(["dam_code", "obsrdt"]).reset_index(drop=True)

    df["rain"] = df["rain"].fillna(df["rf"]).fillna(0)
    df["tmp"] = df["tmp"].fillna(0)
    df["snow"] = df["snow"].fillna(0)

    frames = []
    for _, dam in df.groupby("dam_code"):
        dam = dam.sort_values("obsrdt").copy()
        state_columns = ["inflowqy", "lowlevel", "rsvwtqy", "rsvwtrt", "totdcwtrqy"]
        dam[state_columns] = dam[state_columns].ffill().bfill()
        for lag in [1, 3, 6, 12]:
            dam[f"inflow_lag_{lag}h"] = dam["inflowqy"].shift(lag)
            dam[f"rain_lag_{lag}h"] = dam["rain"].shift(lag)
            dam[f"discharge_lag_{lag}h"] = dam["totdcwtrqy"].shift(lag)

        for window in [3, 6, 12, 24]:
            dam[f"inflow_mean_{window}h"] = dam["inflowqy"].rolling(window, min_periods=window).mean()
            dam[f"discharge_mean_{window}h"] = dam["totdcwtrqy"].rolling(window, min_periods=window).mean()

        for window in [3, 6, 12, 24, 48, 72]:
            dam[f"rain_sum_{window}h"] = dam["rain"].rolling(window, min_periods=window).sum()

        dam["discharge_change_lag_1h"] = dam["totdcwtrqy"].diff(1)
        dam["discharge_change_lag_3h"] = dam["totdcwtrqy"] - dam["totdcwtrqy"].shift(3)
        frames.append(dam)

    result = pd.concat(frames, ignore_index=True)
    latest_idx = result.groupby("dam_code")["obsrdt"].idxmax()
    return result.loc[latest_idx].sort_values("dam_code").reset_index(drop=True)


def build_dashboard_snapshot(model_input: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
    latest_weather = (
        weather.sort_values(["dam_code", "forecast_datetime"])
        .groupby("dam_code", as_index=False)
        .agg(
            forecast_start=("forecast_datetime", "min"),
            forecast_end=("forecast_datetime", "max"),
            forecast_rain_sum=("rain", "sum"),
            forecast_tmp_latest=("tmp", "last"),
        )
    )
    snapshot = model_input.merge(latest_weather, on="dam_code", how="left")

    def rain_level(value):
        if pd.isna(value) or value <= 0:
            return "없음"
        if value < 5:
            return "보통"
        return "높음"

    snapshot["forecast_rain_level"] = snapshot["forecast_rain_sum"].apply(rain_level)
    snapshot["data_ready"] = snapshot[
        ["inflow_lag_12h", "inflow_mean_24h", "discharge_mean_24h"]
    ].notna().all(axis=1)
    snapshot["dashboard_status"] = snapshot["data_ready"].map({True: "예측준비완료", False: "데이터부족"})
    return snapshot


def main() -> None:
    obs = load_realtime_observations()
    weather = load_weather_forecasts()
    model_input = build_hourly_model_input(obs)
    snapshot = build_dashboard_snapshot(model_input, weather)

    model_input.to_csv(OUTPUT_DIR / "실시간_모델입력_최신.csv", index=False, encoding="utf-8-sig")
    snapshot.to_csv(OUTPUT_DIR / "대시보드_현재상태_스냅샷.csv", index=False, encoding="utf-8-sig")

    print("실시간 모델 입력 생성 완료")
    print(
        snapshot[
            [
                "dam_code",
                "dam_name",
                "obsrdt",
                "rain_sum_72h",
                "forecast_rain_sum",
                "forecast_rain_level",
                "dashboard_status",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
