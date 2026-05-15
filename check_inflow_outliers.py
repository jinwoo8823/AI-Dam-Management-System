import os
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


# ==================================================
# 1. .env 불러오기
# ==================================================
load_dotenv()


def get_required_env(key: str) -> str:
    value = os.getenv(key)

    if value is None or value.strip() == "":
        raise ValueError(f".env 파일에 {key} 값이 없습니다.")

    return value


DB_USER = get_required_env("DB_USER")
DB_PASSWORD = quote_plus(get_required_env("DB_PASSWORD"))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = get_required_env("DB_NAME")


engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
)


OUTPUT_DIR = "analysis_results"


# ==================================================
# 2. 데이터 불러오기
# ==================================================
def load_data():
    query = """
        SELECT
            dam_code,
            dam_name,
            observed_at,
            inflow,
            water_level,
            hydrology_rainfall,
            storage_amount,
            storage_rate,
            discharge,
            temperature,
            kma_rainfall,
            snow
        FROM final_historical_data
        ORDER BY dam_name, observed_at
    """

    print("[LOAD] final_historical_data 조회 시작")
    df = pd.read_sql(query, engine)

    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    df["inflow"] = pd.to_numeric(df["inflow"], errors="coerce")
    df["water_level"] = pd.to_numeric(df["water_level"], errors="coerce")
    df["storage_amount"] = pd.to_numeric(df["storage_amount"], errors="coerce")
    df["storage_rate"] = pd.to_numeric(df["storage_rate"], errors="coerce")
    df["discharge"] = pd.to_numeric(df["discharge"], errors="coerce")
    df["hydrology_rainfall"] = pd.to_numeric(df["hydrology_rainfall"], errors="coerce")
    df["kma_rainfall"] = pd.to_numeric(df["kma_rainfall"], errors="coerce")

    print(f"[LOAD] 완료: {len(df):,}건")
    print(f"[LOAD] 댐 개수: {df['dam_name'].nunique()}개")
    print(f"[LOAD] 기간: {df['observed_at'].min()} ~ {df['observed_at'].max()}")

    return df


# ==================================================
# 3. 댐별 요약 통계
# ==================================================
def make_dam_summary(df):
    summary = (
        df.groupby("dam_name")
        .agg(
            row_count=("inflow", "count"),
            inflow_mean=("inflow", "mean"),
            inflow_median=("inflow", "median"),
            inflow_std=("inflow", "std"),
            inflow_min=("inflow", "min"),
            inflow_max=("inflow", "max"),
            inflow_p90=("inflow", lambda x: x.quantile(0.90)),
            inflow_p95=("inflow", lambda x: x.quantile(0.95)),
            inflow_p99=("inflow", lambda x: x.quantile(0.99)),
            inflow_p999=("inflow", lambda x: x.quantile(0.999)),
            rainfall_max=("kma_rainfall", "max"),
            discharge_max=("discharge", "max"),
            storage_rate_max=("storage_rate", "max"),
        )
        .reset_index()
        .sort_values("inflow_max", ascending=False)
    )

    return summary


# ==================================================
# 4. 댐별 상위 유입량 행 추출
# ==================================================
def make_top_inflow_rows(df, top_n=30):
    rows = []

    for dam_name, dam_df in df.groupby("dam_name"):
        temp = dam_df.sort_values("inflow", ascending=False).head(top_n).copy()
        rows.append(temp)

    if not rows:
        return pd.DataFrame()

    result = pd.concat(rows, ignore_index=True)

    result = result.sort_values(["dam_name", "inflow"], ascending=[True, False])

    return result


# ==================================================
# 5. 급격한 유입량 변화 확인
# ==================================================
def make_inflow_jump_rows(df, top_n=30):
    df = df.copy()
    df = df.sort_values(["dam_name", "observed_at"])

    df["inflow_prev_1h"] = df.groupby("dam_name")["inflow"].shift(1)
    df["inflow_diff_1h"] = df["inflow"] - df["inflow_prev_1h"]
    df["inflow_diff_abs_1h"] = df["inflow_diff_1h"].abs()

    rows = []

    for dam_name, dam_df in df.groupby("dam_name"):
        temp = dam_df.sort_values("inflow_diff_abs_1h", ascending=False).head(top_n).copy()
        rows.append(temp)

    if not rows:
        return pd.DataFrame()

    result = pd.concat(rows, ignore_index=True)

    result = result.sort_values(
        ["dam_name", "inflow_diff_abs_1h"],
        ascending=[True, False]
    )

    return result


# ==================================================
# 6. 저장
# ==================================================
def save_results(summary_df, top_rows_df, jump_rows_df):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    summary_path = os.path.join(OUTPUT_DIR, "inflow_outlier_summary_by_dam.csv")
    top_path = os.path.join(OUTPUT_DIR, "inflow_top_rows_by_dam.csv")
    jump_path = os.path.join(OUTPUT_DIR, "inflow_jump_rows_by_dam.csv")

    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    top_rows_df.to_csv(top_path, index=False, encoding="utf-8-sig")
    jump_rows_df.to_csv(jump_path, index=False, encoding="utf-8-sig")

    print("[SAVE] 댐별 유입량 요약:", summary_path)
    print("[SAVE] 댐별 상위 유입량 행:", top_path)
    print("[SAVE] 댐별 급격한 변화 행:", jump_path)


# ==================================================
# 7. 요약 출력
# ==================================================
def print_summary(summary_df):
    print("\n" + "=" * 100)
    print("[SUMMARY] 댐별 유입량 이상치 요약")
    print("=" * 100)

    display_cols = [
        "dam_name",
        "row_count",
        "inflow_mean",
        "inflow_median",
        "inflow_max",
        "inflow_p99",
        "inflow_p999",
        "discharge_max",
        "storage_rate_max",
    ]

    print(summary_df[display_cols].to_string(index=False))

    print("\n[해석]")
    print("- inflow_max가 inflow_p999보다 지나치게 크면 이상치 가능성이 있습니다.")
    print("- 대청처럼 max가 비정상적으로 큰 댐은 상위 유입량 행을 확인해야 합니다.")
    print("- 실제 홍수 이벤트일 수도 있으므로 무조건 삭제하지 말고 시점과 주변 강수량을 함께 봐야 합니다.")


# ==================================================
# 8. 실행
# ==================================================
def main():
    print("[START] 유입량 이상치 점검 시작")

    df = load_data()

    summary_df = make_dam_summary(df)
    top_rows_df = make_top_inflow_rows(df, top_n=30)
    jump_rows_df = make_inflow_jump_rows(df, top_n=30)

    save_results(summary_df, top_rows_df, jump_rows_df)
    print_summary(summary_df)

    print("[END] 유입량 이상치 점검 종료")


if __name__ == "__main__":
    main()