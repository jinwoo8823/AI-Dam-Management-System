import os
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


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


# ==================================================
# 2. 분석 설정
# ==================================================

# 최근 몇 시간 누적 강수량을 볼 것인지
RAIN_WINDOWS = [1, 3, 6, 12, 24, 48, 72]

# 몇 시간 뒤 유입량과 비교할 것인지
INFLOW_TARGET_HOURS = [1, 3, 6, 12, 24]

# 분석에 사용할 강수량 컬럼
RAINFALL_COLUMNS = [
    "hydrology_rainfall",
    "kma_rainfall",
]

OUTPUT_DIR = "analysis_results"


# ==================================================
# 3. DB에서 final_historical_data 불러오기
# ==================================================
def load_final_historical_data() -> pd.DataFrame:
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

    print(f"[LOAD] 조회 완료: {len(df):,}건")
    print(f"[LOAD] 댐 개수: {df['dam_name'].nunique()}개")
    print("[LOAD] 기간:", df["observed_at"].min(), "~", df["observed_at"].max())

    return df


# ==================================================
# 4. 전처리
# ==================================================
def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    print("[PREPROCESS] 전처리 시작")

    df = df.copy()

    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")

    numeric_columns = [
        "inflow",
        "water_level",
        "hydrology_rainfall",
        "storage_amount",
        "storage_rate",
        "discharge",
        "temperature",
        "kma_rainfall",
        "snow",
    ]

    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["dam_name", "observed_at"])
    df = df.sort_values(["dam_name", "observed_at"]).reset_index(drop=True)

    # 강수량 결측치는 0으로 처리
    for col in RAINFALL_COLUMNS:
        df[col] = df[col].fillna(0)

    print("[PREPROCESS] 전처리 완료")
    print(f"[PREPROCESS] 남은 데이터: {len(df):,}건")

    return df


# ==================================================
# 5. 누적 강수량 컬럼 생성
# ==================================================
def add_rolling_rainfall_features(df: pd.DataFrame) -> pd.DataFrame:
    print("[FEATURE] 누적 강수량 컬럼 생성 시작")

    df = df.copy()
    df = df.sort_values(["dam_name", "observed_at"]).reset_index(drop=True)

    for rain_col in RAINFALL_COLUMNS:
        for window in RAIN_WINDOWS:
            new_col = f"{rain_col}_{window}h_sum"

            df[new_col] = (
                df.groupby("dam_name")[rain_col]
                .transform(lambda x: x.rolling(window=window, min_periods=1).sum())
            )

            print(f"[FEATURE] 생성 완료: {new_col}")

    print("[FEATURE] 누적 강수량 컬럼 생성 완료")

    return df


# ==================================================
# 6. 미래 유입량 컬럼 생성
# ==================================================
def add_future_inflow_targets(df: pd.DataFrame) -> pd.DataFrame:
    print("[TARGET] 미래 유입량 컬럼 생성 시작")

    df = df.copy()
    df = df.sort_values(["dam_name", "observed_at"]).reset_index(drop=True)

    for hour in INFLOW_TARGET_HOURS:
        target_col = f"inflow_after_{hour}h"

        df[target_col] = (
            df.groupby("dam_name")["inflow"]
            .shift(-hour)
        )

        print(f"[TARGET] 생성 완료: {target_col}")

    print("[TARGET] 미래 유입량 컬럼 생성 완료")

    return df


# ==================================================
# 7. 상관관계 분석
# ==================================================
def analyze_correlation(df: pd.DataFrame) -> pd.DataFrame:
    print("[ANALYSIS] 강수량-유입량 상관관계 분석 시작")

    results = []

    dam_names = sorted(df["dam_name"].dropna().unique())

    for dam_name in dam_names:
        dam_df = df[df["dam_name"] == dam_name].copy()

        for rain_col in RAINFALL_COLUMNS:
            for rain_window in RAIN_WINDOWS:
                rain_feature = f"{rain_col}_{rain_window}h_sum"

                for target_hour in INFLOW_TARGET_HOURS:
                    target_col = f"inflow_after_{target_hour}h"

                    temp = dam_df[[rain_feature, target_col]].dropna()

                    data_count = len(temp)

                    if data_count < 30:
                        corr = np.nan
                    elif temp[rain_feature].std() == 0 or temp[target_col].std() == 0:
                        corr = np.nan
                    else:
                        corr = temp[rain_feature].corr(temp[target_col])

                    results.append({
                        "dam_name": dam_name,
                        "rainfall_source": rain_col,
                        "rain_window_hour": rain_window,
                        "inflow_target_hour": target_hour,
                        "rainfall_feature": rain_feature,
                        "target_column": target_col,
                        "correlation": corr,
                        "abs_correlation": abs(corr) if pd.notna(corr) else np.nan,
                        "data_count": data_count,
                    })

    result_df = pd.DataFrame(results)

    result_df = result_df.sort_values(
        ["dam_name", "rainfall_source", "abs_correlation"],
        ascending=[True, True, False]
    ).reset_index(drop=True)

    print("[ANALYSIS] 상관관계 분석 완료")
    print(f"[ANALYSIS] 결과 행 수: {len(result_df):,}건")

    return result_df


# ==================================================
# 8. 댐별 최고 상관 조합 추출
# ==================================================
def get_best_lag_by_dam(result_df: pd.DataFrame) -> pd.DataFrame:
    print("[BEST] 댐별 최고 상관 조합 추출")

    valid_df = result_df.dropna(subset=["abs_correlation"]).copy()

    if valid_df.empty:
        print("[WARNING] 유효한 상관관계 결과가 없습니다.")
        return pd.DataFrame()

    best_df = (
        valid_df.sort_values(["dam_name", "abs_correlation"], ascending=[True, False])
        .groupby("dam_name")
        .head(1)
        .reset_index(drop=True)
    )

    return best_df


# ==================================================
# 9. 결과 저장
# ==================================================
def save_results_to_csv(result_df: pd.DataFrame, best_df: pd.DataFrame):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    result_path = os.path.join(OUTPUT_DIR, "rainfall_inflow_lag_correlation.csv")
    best_path = os.path.join(OUTPUT_DIR, "rainfall_inflow_best_lag_by_dam.csv")

    result_df.to_csv(result_path, index=False, encoding="utf-8-sig")
    best_df.to_csv(best_path, index=False, encoding="utf-8-sig")

    print("[SAVE CSV] 전체 결과 저장:", result_path)
    print("[SAVE CSV] 댐별 최고 결과 저장:", best_path)


# ==================================================
# 10. 결과 DB 저장
# ==================================================
def create_analysis_table():
    create_sql = text("""
        CREATE TABLE IF NOT EXISTS rainfall_inflow_lag_analysis (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,

            dam_name VARCHAR(100) NOT NULL,
            rainfall_source VARCHAR(100) NOT NULL,

            rain_window_hour INT NOT NULL,
            inflow_target_hour INT NOT NULL,

            rainfall_feature VARCHAR(100),
            target_column VARCHAR(100),

            correlation DECIMAL(10,6),
            abs_correlation DECIMAL(10,6),
            data_count INT,

            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

            UNIQUE KEY uq_lag_analysis (
                dam_name,
                rainfall_source,
                rain_window_hour,
                inflow_target_hour
            )
        );
    """)

    with engine.begin() as conn:
        conn.execute(create_sql)

    print("[DB] rainfall_inflow_lag_analysis 테이블 준비 완료")


def save_results_to_db(result_df: pd.DataFrame):
    if result_df.empty:
        print("[DB] 저장할 분석 결과가 없습니다.")
        return

    create_analysis_table()

    delete_sql = text("TRUNCATE TABLE rainfall_inflow_lag_analysis")

    insert_sql = text("""
        INSERT INTO rainfall_inflow_lag_analysis (
            dam_name,
            rainfall_source,
            rain_window_hour,
            inflow_target_hour,
            rainfall_feature,
            target_column,
            correlation,
            abs_correlation,
            data_count
        )
        VALUES (
            :dam_name,
            :rainfall_source,
            :rain_window_hour,
            :inflow_target_hour,
            :rainfall_feature,
            :target_column,
            :correlation,
            :abs_correlation,
            :data_count
        )
    """)

    save_df = result_df.copy()

    save_df = save_df.replace({np.nan: None})

    records = save_df.to_dict("records")

    with engine.begin() as conn:
        conn.execute(delete_sql)
        conn.execute(insert_sql, records)

    print(f"[DB] rainfall_inflow_lag_analysis 저장 완료: {len(records):,}건")


# ==================================================
# 11. 요약 출력
# ==================================================
def print_summary(best_df: pd.DataFrame):
    print("\n" + "=" * 80)
    print("[SUMMARY] 댐별 강수량-유입량 최고 상관 조합")
    print("=" * 80)

    if best_df.empty:
        print("유효한 분석 결과가 없습니다.")
        return

    display_columns = [
        "dam_name",
        "rainfall_source",
        "rain_window_hour",
        "inflow_target_hour",
        "correlation",
        "data_count",
    ]

    print(best_df[display_columns].to_string(index=False))

    print("\n[해석 방법]")
    print("- rain_window_hour: 최근 몇 시간 누적 강수량을 봤는지")
    print("- inflow_target_hour: 몇 시간 뒤 유입량과 비교했는지")
    print("- correlation: 강수량과 미래 유입량의 상관계수")
    print("- correlation이 1에 가까울수록 강한 양의 관계")
    print("- correlation이 0에 가까우면 관계가 약함")
    print("- correlation이 음수이면 강수 증가와 유입량 증가가 같은 방향이 아닐 수 있음")


# ==================================================
# 12. 전체 실행
# ==================================================
def main():
    print("[START] 강수량-유입량 시간차 분석 시작")

    df = load_final_historical_data()
    df = preprocess_data(df)
    df = add_rolling_rainfall_features(df)
    df = add_future_inflow_targets(df)

    result_df = analyze_correlation(df)
    best_df = get_best_lag_by_dam(result_df)

    save_results_to_csv(result_df, best_df)
    save_results_to_db(result_df)

    print_summary(best_df)

    print("[END] 강수량-유입량 시간차 분석 종료")


if __name__ == "__main__":
    main()