from __future__ import annotations

from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

st.set_page_config(
    page_title="AI 댐 관리 대시보드",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data
def read_csv(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    path = DATA_DIR / name
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, encoding="utf-8-sig", parse_dates=parse_dates or [])


def to_num(value, default: float | None = None) -> float | None:
    value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(value):
        return default
    return float(value)


def fmt_num(value, unit: str = "", digits: int = 2) -> str:
    value = to_num(value)
    if value is None:
        return "-"
    return f"{value:,.{digits}f}{unit}"


def fmt_pct(value, digits: int = 1) -> str:
    value = to_num(value)
    if value is None:
        return "-"
    if abs(value) <= 1:
        value *= 100
    return f"{value:,.{digits}f}%"


def fmt_dt(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    return pd.to_datetime(value, errors="coerce").strftime("%Y-%m-%d %H:%M")


def card(title: str, value: str, caption: str = "", color: str = "#2563eb") -> None:
    st.markdown(
        f"""
        <div class="metric-card" style="border-left-color:{color};">
            <div class="metric-title">{title}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-caption">{caption}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section(title: str, caption: str = "") -> None:
    st.markdown(f"### {title}")
    if caption:
        st.caption(caption)


def status_info(row: pd.Series) -> tuple[str, str, str, list[int]]:
    level = row.get("discharge_change_level")
    predicted = to_num(row.get("predicted_discharge_3h"))
    if predicted is None:
        return "예측 보류", "필수 예측값이 없는 스냅샷입니다.", "#64748b", [100, 116, 139, 170]
    if level == "높음":
        return "방류 조정 검토", "3시간 뒤 방류 변화 가능성이 높습니다.", "#dc2626", [220, 38, 38, 190]
    if level == "보통":
        return "주의 관찰", "3시간 뒤 방류 변화 가능성이 보통입니다.", "#d97706", [217, 119, 6, 185]
    return "현재 방류 유지", "3시간 뒤 큰 변화 가능성은 낮습니다.", "#16a34a", [22, 163, 74, 175]


def review_text(value) -> str:
    if value is None or pd.isna(value):
        return "예측 보류"
    text = str(value)
    return {
        "불필요": "현재 방류 유지",
        "관찰": "주의 관찰",
        "관찰 필요": "주의 관찰",
        "검토 필요": "방류 조정 검토",
        "데이터부족": "예측 보류",
        "데이터 부족": "예측 보류",
    }.get(text, text)


def add_validation_metrics(validation: pd.DataFrame, history: pd.DataFrame) -> pd.DataFrame:
    if validation.empty:
        return validation

    thresholds: dict[int, float] = {}
    for dam_code, group in history.groupby("dam_code"):
        discharge = pd.to_numeric(group["totdcwtrqy"], errors="coerce").dropna()
        if discharge.empty:
            thresholds[int(dam_code)] = 1.0
            continue
        mean_based = discharge.mean() * 0.05
        volatility_based = discharge.diff().abs().std() * 0.5
        thresholds[int(dam_code)] = round(float(max(1.0, mean_based, 0 if pd.isna(volatility_based) else volatility_based)), 2)

    result = validation.copy()
    result["유입량_절대오차"] = (
        pd.to_numeric(result["predicted_inflow_3h"], errors="coerce")
        - pd.to_numeric(result["actual_inflow_3h"], errors="coerce")
    ).abs()
    result["방류량_절대오차"] = (
        pd.to_numeric(result["predicted_discharge_3h"], errors="coerce")
        - pd.to_numeric(result["actual_discharge_3h"], errors="coerce")
    ).abs()
    result["댐별_변화기준"] = result["dam_code"].map(lambda code: thresholds.get(int(code), 1.0))
    result["실제_방류변화"] = (
        pd.to_numeric(result["actual_discharge_3h"], errors="coerce")
        - pd.to_numeric(result["base_discharge"], errors="coerce")
    ).abs() >= result["댐별_변화기준"]
    result["예측_변화신호"] = result["discharge_change_level"].isin(["보통", "높음"])
    result["변화신호_적중"] = result["실제_방류변화"] == result["예측_변화신호"]
    return result


def daily_weather_summary(weather: pd.DataFrame) -> pd.DataFrame:
    if weather.empty:
        return pd.DataFrame()
    data = weather.copy()
    data["forecast_datetime"] = pd.to_datetime(data["forecast_datetime"], errors="coerce")
    data["rain"] = pd.to_numeric(data.get("rain"), errors="coerce").fillna(0)
    data["tmp"] = pd.to_numeric(data.get("tmp"), errors="coerce")
    data["date"] = data["forecast_datetime"].dt.date
    return (
        data.groupby("date", as_index=False)
        .agg(예상강수량_mm=("rain", "sum"), 최저기온=("tmp", "min"), 최고기온=("tmp", "max"))
        .rename(columns={"date": "날짜"})
    )


def rain_until(weather: pd.DataFrame, hours: int) -> float | None:
    if weather.empty:
        return None
    data = weather.copy()
    data["forecast_datetime"] = pd.to_datetime(data["forecast_datetime"], errors="coerce")
    data["rain"] = pd.to_numeric(data["rain"], errors="coerce").fillna(0)
    base = data["forecast_datetime"].min()
    if pd.isna(base):
        return None
    end = base + pd.Timedelta(hours=hours)
    return float(data.loc[data["forecast_datetime"].between(base, end), "rain"].sum())


def internal_signal(
    row: pd.Series,
    selected_weather: pd.DataFrame,
    threshold_row: pd.Series | None,
) -> tuple[str, str, str, list[int]]:
    if pd.isna(row.get("predicted_inflow_3h")) or pd.isna(row.get("predicted_discharge_3h")):
        return "판단 보류", "예측에 필요한 최신 값이 충분하지 않습니다. 데이터 수집 상태를 확인한 뒤 다시 판단하세요.", "#64748b", [100, 116, 139, 180]
    current_inflow = to_num(row.get("inflowqy"), 0) or 0
    predicted_inflow = to_num(row.get("predicted_inflow_3h"), current_inflow) or current_inflow
    current_discharge = to_num(row.get("totdcwtrqy"), 0) or 0
    predicted_discharge = to_num(row.get("predicted_discharge_3h"), current_discharge) or current_discharge
    storage_rate = to_num(row.get("rsvwtrt"), 0) or 0
    observed_rain = to_num(row.get("rf"), 0) or 0
    rain_3h = rain_until(selected_weather, 3) or 0
    rain_12h = rain_until(selected_weather, 12) or 0
    inflow_reference = max(current_inflow, predicted_inflow)
    discharge_increase = max(0, predicted_discharge - current_discharge)

    inflow_q90 = to_num(threshold_row.get("inflow_q90") if threshold_row is not None else None)
    inflow_q95 = to_num(threshold_row.get("inflow_q95") if threshold_row is not None else None)
    discharge_q95 = to_num(threshold_row.get("discharge_increase_3h_q95") if threshold_row is not None else None)
    rain_caution = rain_3h >= 60 or rain_12h >= 110
    rain_emergency = rain_3h >= 90 or rain_12h >= 180
    inflow_watch = inflow_q90 is not None and inflow_reference >= inflow_q90
    inflow_high = inflow_q95 is not None and inflow_reference >= inflow_q95
    discharge_surge = discharge_q95 is not None and discharge_increase >= discharge_q95
    storage_watch = storage_rate >= 75

    reasons = []
    if rain_emergency:
        reasons.append("호우경보 수준의 예상 강수")
    elif rain_caution:
        reasons.append("호우주의보 수준의 예상 강수")
    elif observed_rain > 0 or rain_3h > 0:
        reasons.append("강수 관측 또는 예보")
    if inflow_high:
        reasons.append("유입량 과거 상위 5%")
    elif inflow_watch:
        reasons.append("유입량 과거 상위 10%")
    if discharge_surge:
        reasons.append("예상 방류 증가폭 과거 상위 5%")
    if storage_watch:
        reasons.append("저수율 75% 이상")

    if rain_emergency or (discharge_surge and inflow_watch) or (rain_caution and inflow_watch and storage_watch):
        return "긴급 검토", "비, 유입량, 예상 방류 증가가 함께 커졌습니다. 담당자가 즉시 강수량, 수문값과 하류 수위를 확인하세요.", "#dc2626", [220, 38, 38, 210]
    if discharge_surge or rain_caution or inflow_high:
        return "주의", "평소보다 유입량 또는 예상 방류 증가 폭이 큽니다. 강수량과 하류 수위를 함께 확인하세요.", "#d97706", [217, 119, 6, 195]
    if observed_rain > 0 or rain_3h > 0 or inflow_watch:
        return "관심", "비가 내리거나 유입량이 평소보다 늘고 있습니다. 다음 관측값에서 변화가 이어지는지 확인하세요.", "#ca8a04", [202, 138, 4, 185]
    return "정상", "현재 확인이 필요한 뚜렷한 변화가 없습니다. 정기적으로 최신 관측값을 확인하세요.", "#16a34a", [22, 163, 74, 175]


def render_signal_banner(level: str, reason: str, color: str) -> None:
    st.markdown(
        f"""
        <div class="signal-banner" style="border-color:{color}; background:{color}12;">
            <div class="signal-title" style="color:{color};">내부 운영 참고 신호: {level}</div>
            <div class="signal-reason">{reason}</div>
            <div class="signal-caption">공식 기상특보 또는 방류 명령이 아닙니다. 관리자가 수문값, 기상예보, 하류 상황을 함께 확인하기 위한 내부 참고 지표입니다.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def horizon_score(row: pd.Series) -> int:
    hit = to_num(row.get("변화신호_적중률"), 0) or 0
    improve = to_num(row.get("MAE_개선율(%)"), 0) or 0
    return int(max(0, min(100, hit * 70 + max(improve, 0) * 0.3)))


def build_horizon_table(
    selected_name: str,
    selected_weather: pd.DataFrame,
    five_day: pd.DataFrame,
    horizon_by_dam: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for hours, label, role in [(3, "3시간 뒤", "단기 운영 판단"), (6, "6시간 뒤", "예비 운영 검토")]:
        part = horizon_by_dam[
            (horizon_by_dam.get("댐이름") == selected_name)
            & (pd.to_numeric(horizon_by_dam.get("예측시간"), errors="coerce") == hours)
        ]
        if part.empty:
            rows.append(
                {
                    "예측 구간": label,
                    "역할": role,
                    "예상 강수량": fmt_num(rain_until(selected_weather, hours), " mm"),
                    "방류 예측/판단": "해당 구간 실험 지표 없음",
                    "근거": "-",
                    "참고 강도": "-",
                }
            )
            continue
        item = part.iloc[0]
        rows.append(
            {
                "예측 구간": label,
                "역할": role,
                "예상 강수량": fmt_num(rain_until(selected_weather, hours), " mm"),
                "방류 예측/판단": f"변화신호 적중률 {fmt_pct(item.get('변화신호_적중률'))}",
                "근거": f"MAE {fmt_num(item.get('모델_MAE'), ' ㎥/s')}, R2 {fmt_num(item.get('모델_R2'), '', 3)}, 기준선 대비 {fmt_pct(item.get('MAE_개선율(%)'))}",
                "참고 강도": f"{horizon_score(item)}/100",
            }
        )

    long_part = five_day[five_day.get("댐이름") == selected_name].copy() if "댐이름" in five_day.columns else pd.DataFrame()
    long_by_hour: dict[int, pd.Series] = {}
    if not long_part.empty:
        long_part["예측시간"] = pd.to_numeric(long_part["예측시간"], errors="coerce")
        for _, row in long_part.iterrows():
            if pd.notna(row.get("예측시간")):
                long_by_hour[int(row["예측시간"])] = row

    for hours, label in [(24, "1일 뒤"), (48, "2일 뒤"), (72, "3일 뒤"), (96, "4일 뒤"), (120, "5일 뒤")]:
        base_row = long_by_hour.get(hours)
        interpolated = False
        if base_row is None and hours in (48, 96):
            before = long_by_hour.get(24 if hours == 48 else 72)
            after = long_by_hour.get(72 if hours == 48 else 120)
            if before is not None and after is not None:
                ratio = 0.5
                data = before.copy()
                for col in ["예상강수량", "예상누적방류_하한", "예상누적방류_중앙", "예상누적방류_상한", "구간포함률"]:
                    data[col] = (to_num(before.get(col), 0) or 0) * (1 - ratio) + (to_num(after.get(col), 0) or 0) * ratio
                data["방류증가가능성"] = after.get("방류증가가능성", before.get("방류증가가능성", "-"))
                base_row = data
                interpolated = True

        if base_row is None:
            rows.append(
                {
                    "예측 구간": label,
                    "역할": "장기 강수 위험 참고",
                    "예상 강수량": fmt_num(rain_until(selected_weather, hours), " mm"),
                    "방류 예측/판단": "장기 구간 데이터 없음",
                    "근거": "-",
                    "참고 강도": "-",
                }
            )
            continue

        lower = fmt_num(base_row.get("예상누적방류_하한"), "", 0)
        upper = fmt_num(base_row.get("예상누적방류_상한"), "", 0)
        possibility = base_row.get("방류증가가능성", "-")
        reason = "누적 방류 범위와 강수 전망을 함께 본 장기 참고값입니다."
        if interpolated:
            reason = "인접한 1/3일 또는 3/5일 결과를 보간한 참고값입니다."
        rows.append(
            {
                "예측 구간": label,
                "역할": "장기 강수 위험 참고" if hours >= 24 else "운영 판단",
                "예상 강수량": fmt_num(base_row.get("예상강수량", rain_until(selected_weather, hours)), " mm"),
                "방류 예측/판단": f"방류 증가 가능성 {possibility}",
                "근거": f"누적 예상 방류 범위 {lower} ~ {upper}",
                "참고 강도": f"{int((to_num(base_row.get('구간포함률'), 0) or 0) * 100)}/100",
                "해석": reason,
            }
        )

    return pd.DataFrame(rows)


summary = read_csv("latest_summary.csv", parse_dates=["obsrdt", "pred_base_time", "pred_target_time"])
history = read_csv("observation_history_72h.csv", parse_dates=["obsrdt"])
weather = read_csv("weather_forecast_latest.csv", parse_dates=["base_datetime", "forecast_datetime"])
validation = read_csv("prediction_validation.csv", parse_dates=["pred_base_time", "pred_target_time"])
downstream = read_csv("downstream_representative.csv", parse_dates=["observed_at"])
downstream_candidates = read_csv("downstream_candidates.csv")
five_day = read_csv("five_day_range.csv")
horizon_summary = read_csv("release_horizon_summary.csv")
horizon_by_dam = read_csv("release_horizon_by_dam.csv")
interest_history = read_csv("interest_period_history.csv", parse_dates=["obsrdt"])
risk_thresholds = read_csv("risk_thresholds.csv")

validation = add_validation_metrics(validation, history)

if summary.empty:
    st.error("공유용 CSV 데이터가 없습니다. data 폴더를 확인하세요.")
    st.stop()

st.markdown(
    """
    <style>
    .main { background:#f4f7fb; }
    h1 {
        color:#0f2747;
        font-size:2rem !important;
        font-weight:850 !important;
        letter-spacing:0 !important;
    }
    h2, h3 {
        color:#16365f;
        font-weight:800 !important;
        letter-spacing:0 !important;
    }
    h3 { font-size:1.24rem !important; }
    p, li, label, div[data-testid="stCaptionContainer"] {
        color:#52647a;
    }
    button[data-baseweb="tab"] p {
        color:#52647a;
        font-size:.92rem;
        font-weight:750;
    }
    button[data-baseweb="tab"][aria-selected="true"] p {
        color:#1d4ed8;
    }
    .metric-card {
        background:#fff;
        border:1px solid #d8e0ea;
        border-left:4px solid #2563eb;
        border-radius:8px;
        padding:14px 16px;
        min-height:104px;
        box-shadow:0 1px 2px rgba(15,23,42,.04);
    }
    .metric-title { color:#52647a; font-size:.88rem; font-weight:700; margin-bottom:8px; }
    .metric-value { color:#0f2747; font-size:1.44rem; font-weight:850; line-height:1.25; word-break:keep-all; }
    .metric-caption { color:#66758a; font-size:.82rem; margin-top:6px; }
    .notice {
        border:1px solid #bfdbfe;
        background:#eff6ff;
        color:#1e3a8a;
        border-radius:8px;
        padding:12px 14px;
        margin:10px 0 14px 0;
    }
    .legend {
        display:flex;
        flex-wrap:wrap;
        gap:8px 14px;
        color:#52647a;
        font-size:.84rem;
        margin:8px 0 14px 0;
    }
    .legend-item { display:flex; align-items:center; gap:6px; }
    .legend-dot {
        width:10px;
        height:10px;
        border-radius:50%;
        display:inline-block;
    }
    .signal-banner {
        border:2px solid #16a34a;
        border-radius:8px;
        padding:14px 16px;
        margin:10px 0 16px;
    }
    .signal-title { font-size:1.08rem; font-weight:850; }
    .signal-reason { color:#334155; font-size:.92rem; font-weight:700; margin-top:5px; }
    .signal-caption { color:#64748b; font-size:.8rem; margin-top:6px; }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.title("AI 댐 관리")
    st.caption("공유용 CSV 스냅샷 대시보드입니다. API 키와 로컬 DB 없이 실행됩니다.")
    dam_names = summary["dam_name"].dropna().tolist()
    selected_name = st.selectbox("댐 선택", dam_names, index=dam_names.index("남강") if "남강" in dam_names else 0)
    st.divider()
    latest_obs = summary["obsrdt"].max()
    st.caption(f"스냅샷 기준: {fmt_dt(latest_obs)}")
    st.caption("김천부항댐 제외 20개 다목적댐 기준")

selected = summary[summary["dam_name"] == selected_name].iloc[0]
selected_code = int(selected["dam_code"])
selected_history = history[history["dam_code"] == selected_code].copy()
selected_weather = weather[weather["dam_code"] == selected_code].copy()
selected_validation = validation[validation["dam_code"] == selected_code].copy()
selected_downstream = downstream[downstream["dam_code"] == selected_code].copy()
selected_candidates = downstream_candidates[downstream_candidates["dam_code"] == selected_code].head(3).copy()
selected_interest = interest_history[interest_history["dam_code"] == selected_code].copy()
selected_threshold = risk_thresholds[risk_thresholds["dam_code"] == selected_code]
selected_threshold_row = selected_threshold.iloc[0] if not selected_threshold.empty else None
status_label, status_reason, status_color, status_rgba = status_info(selected)
signal_level, signal_reason, signal_color, signal_rgba = internal_signal(selected, selected_weather, selected_threshold_row)

st.title("AI 댐 관리 대시보드")
st.caption("수문 운영 정보, 기상예보, 하류 수위, 3시간/6시간/1~5일 방류 판단을 CSV 스냅샷으로 재현한 공유용 대시보드")
render_signal_banner(signal_level, signal_reason, signal_color)

top = st.columns(4)
with top[0]:
    card("현재 시각", fmt_dt(pd.Timestamp.now()), "대시보드 표시 시각")
with top[1]:
    card("최신 수문 관측", fmt_dt(selected.get("obsrdt")), "수문 운영 정보")
with top[2]:
    card("최신 예측 기준", fmt_dt(selected.get("pred_base_time")), "3시간 뒤 예측")
with top[3]:
    card("3시간 뒤 목표시각", fmt_dt(selected.get("pred_target_time")), "예측 목표")

section(f"{selected_name}댐 상세 현황", "선택한 댐의 현재 수문값과 예측 참고값입니다.")
detail = st.columns(6)
with detail[0]:
    card("현재 유입량", fmt_num(selected.get("inflowqy"), " ㎥/s"), "수문 관측값", "#0284c7")
with detail[1]:
    card("현재 방류량", fmt_num(selected.get("totdcwtrqy"), " ㎥/s"), "수문 관측값", "#0284c7")
with detail[2]:
    card("저수율", fmt_num(selected.get("rsvwtrt"), "%"), "현재 저수 상태", "#16a34a")
with detail[3]:
    card("수위", fmt_num(selected.get("lowlevel"), " m"), "현재 수위", "#16a34a")
with detail[4]:
    card("3시간 뒤 유입량", fmt_num(selected.get("predicted_inflow_3h"), " ㎥/s"), "모델 예측", "#d97706")
with detail[5]:
    card("3시간 뒤 방류량", fmt_num(selected.get("predicted_discharge_3h"), " ㎥/s"), "운영 참고값", "#d97706")

tab_summary, tab_weather, tab_operation, tab_interest, tab_prediction, tab_validation, tab_downstream, tab_map, tab_five = st.tabs(
    ["요약", "기상예보", "수문 운영 정보", "강수·고유입 관심구간", "3시간 뒤 방류 판단", "실시간 검증", "하류 수위", "지도", "5일 예측 해석"]
)

with tab_summary:
    left, right = st.columns([1.1, 1])
    with left:
        section("최근 72시간 변화", "유입량, 방류량, 저수율 추세")
        if selected_history.empty:
            st.info("선택 댐의 72시간 이력이 없습니다.")
        else:
            chart = selected_history.set_index("obsrdt")[["inflowqy", "totdcwtrqy", "rsvwtrt"]]
            st.line_chart(chart, height=320)
    with right:
        section("판단 요약", "실제 방류 명령이 아닌 운영 판단 보조 지표입니다.")
        card("3시간 뒤 조치", status_label, status_reason, status_color)
        st.markdown(
            f"""
            <div class="notice">
            현재 방류량은 {fmt_num(selected.get("totdcwtrqy"), " ㎥/s")}이고,
            3시간 뒤 예상 방류량은 {fmt_num(selected.get("predicted_discharge_3h"), " ㎥/s")}입니다.
            이 값은 자동 방류 명령이 아니라 관리자 판단을 돕는 참고값입니다.
            </div>
            """,
            unsafe_allow_html=True,
        )

with tab_weather:
    section("기상예보", "댐 좌표 격자와 결합한 초단기/단기 예보 스냅샷")
    daily = daily_weather_summary(selected_weather)
    if not daily.empty:
        st.bar_chart(daily.set_index("날짜")[["예상강수량_mm"]], height=240)
        st.dataframe(daily, width="stretch", hide_index=True)
    st.dataframe(selected_weather, width="stretch", hide_index=True, height=360)

with tab_operation:
    section("현재 수문 운영 정보", "가장 최근 수문 관측값")
    operation = summary[
        ["dam_name", "dam_code", "obsrdt", "inflowqy", "lowlevel", "rf", "rsvwtqy", "rsvwtrt", "totdcwtrqy"]
    ].rename(
        columns={
            "dam_name": "댐 이름(dam_name)",
            "dam_code": "댐 코드(dam_code)",
            "obsrdt": "관측시각(obsrdt)",
            "inflowqy": "유입량(inflowqy)",
            "lowlevel": "수위(lowlevel)",
            "rf": "강수량(rf)",
            "rsvwtqy": "저수량(rsvwtqy)",
            "rsvwtrt": "저수율(rsvwtrt)",
            "totdcwtrqy": "방류량(totdcwtrqy)",
        }
    )
    st.dataframe(operation, width="stretch", hide_index=True)

with tab_interest:
    section("강수·고유입 관심구간", "과거 학습 데이터 중 운영상 우선 확인이 필요한 시점만 모았습니다.")
    st.markdown(
        """
        <div class="notice">
        이 화면은 공식 호우특보 발령 이력이 아닙니다.
        관측 강수량이 있거나, 최근 24시간 누적 강수량이 있거나,
        선택 댐의 유입량이 과거 상위 25% 이상인 시점을 데이터 기반 관심구간으로 분류했습니다.
        </div>
        """,
        unsafe_allow_html=True,
    )
    if selected_interest.empty:
        st.info("선택 댐의 강수·고유입 관심구간 데이터가 없습니다.")
    else:
        selected_interest = selected_interest.sort_values("obsrdt", ascending=False)
        min_date = selected_interest["obsrdt"].min().date()
        max_date = selected_interest["obsrdt"].max().date()
        date_range = st.date_input(
            "조회 기간",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="선택한 기간에 포함되는 과거 관심구간만 표시합니다.",
        )
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
            selected_interest = selected_interest[
                selected_interest["obsrdt"].dt.date.between(start_date, end_date)
            ]
        cols = st.columns(4)
        with cols[0]:
            card("관심구간 기록", f"{len(selected_interest):,}건", "선택 댐 기준", "#2563eb")
        with cols[1]:
            card("최대 시간강수", fmt_num(selected_interest["rain"].max(), " mm"), "관측 rf/rain 기준", "#0284c7")
        with cols[2]:
            card("최대 유입량", fmt_num(selected_interest["inflowqy"].max(), " ㎥/s"), "관심구간 관측값", "#d97706")
        with cols[3]:
            card("최대 방류량", fmt_num(selected_interest["totdcwtrqy"].max(), " ㎥/s"), "관심구간 관측값", "#dc2626")

        chart = (
            selected_interest.sort_values("obsrdt")
            .set_index("obsrdt")[["inflowqy", "totdcwtrqy", "rain_sum_24h"]]
            .rename(
                columns={
                    "inflowqy": "유입량",
                    "totdcwtrqy": "방류량",
                    "rain_sum_24h": "24시간 누적강수",
                }
            )
        )
        st.line_chart(chart, height=280)
        interest_view = selected_interest[
            ["obsrdt", "rain", "rain_sum_24h", "inflowqy", "totdcwtrqy", "lowlevel", "rsvwtrt", "interest_reason"]
        ].rename(
            columns={
                "obsrdt": "관측시각",
                "rain": "시간강수(mm)",
                "rain_sum_24h": "24시간 누적강수(mm)",
                "inflowqy": "유입량(㎥/s)",
                "totdcwtrqy": "방류량(㎥/s)",
                "lowlevel": "수위(m)",
                "rsvwtrt": "저수율(%)",
                "interest_reason": "선정 근거",
            }
        )
        st.dataframe(interest_view, width="stretch", hide_index=True, height=380)

with tab_prediction:
    section("3시간 뒤 방류 판단", "방류량 직접 명령이 아니라 방류 변화 가능성 판단입니다.")
    cols = st.columns(3)
    with cols[0]:
        card("운영 판단", status_label, status_reason, status_color)
    with cols[1]:
        card("예상 방류량", fmt_num(selected.get("predicted_discharge_3h"), " ㎥/s"), "3시간 뒤 참고값")
    with cols[2]:
        card("관리자 안내", review_text(selected.get("release_review")), "모델 판단 문구")
    comparison = summary[
        ["dam_name", "dam_code", "obsrdt", "totdcwtrqy", "pred_target_time", "predicted_discharge_3h", "discharge_change_level", "release_review"]
    ].copy()
    statuses = comparison.apply(status_info, axis=1, result_type="expand")
    comparison.insert(1, "3시간 뒤 조치", statuses[0])
    comparison["release_review"] = comparison["release_review"].apply(review_text)
    st.dataframe(
        comparison.rename(
            columns={
                "dam_name": "댐 이름",
                "dam_code": "댐 코드",
                "obsrdt": "관측시각",
                "totdcwtrqy": "현재 방류량",
                "pred_target_time": "예측 목표시각",
                "predicted_discharge_3h": "예상 방류량",
                "discharge_change_level": "변화 가능성",
                "release_review": "관리자 안내",
            }
        ),
        width="stretch",
        hide_index=True,
        height=420,
    )

with tab_validation:
    section("실시간 예측 검증", "예측 목표시각의 실제 수문값과 매칭된 결과")
    if validation.empty:
        st.info("검증 데이터가 없습니다.")
    else:
        cols = st.columns(3)
        with cols[0]:
            card("검증 건수", f"{len(validation):,}건", "전체 댐 기준")
        with cols[1]:
            card("방류량 MAE", fmt_num(validation["방류량_절대오차"].mean(), " ㎥/s"), "낮을수록 좋음")
        with cols[2]:
            card("변화신호 적중률", fmt_num(validation["변화신호_적중"].mean() * 100, "%"), "댐별 변화 기준")
        show = selected_validation[
            [
                "dam_name",
                "pred_base_time",
                "pred_target_time",
                "base_discharge",
                "댐별_변화기준",
                "predicted_discharge_3h",
                "actual_discharge_3h",
                "방류량_절대오차",
                "실제_방류변화",
                "discharge_change_level",
                "변화신호_적중",
            ]
        ].rename(
            columns={
                "dam_name": "댐 이름",
                "pred_base_time": "예측 기준",
                "pred_target_time": "검증 시각",
                "base_discharge": "기준 방류량",
                "predicted_discharge_3h": "예측 방류량",
                "actual_discharge_3h": "실제 방류량",
                "discharge_change_level": "변화 가능성",
            }
        )
        st.dataframe(show, width="stretch", hide_index=True, height=360)

with tab_downstream:
    section("대표 하류 수위관측소", "방류 판단 보조를 위한 하류 또는 인접 수위관측소")
    if selected_downstream.empty:
        st.info("대표 하류 수위관측소 데이터가 없습니다.")
    else:
        ds = selected_downstream.iloc[0]
        cols = st.columns(4)
        with cols[0]:
            card("대표 관측소", str(ds.get("station_name")), f"코드 {ds.get('station_code')}")
        with cols[1]:
            card("댐과 거리", fmt_num(ds.get("distance_km"), " km"), "좌표 기준 직선거리")
        with cols[2]:
            card("최근 하류 수위", fmt_num(ds.get("water_level"), " m"), fmt_dt(ds.get("observed_at")))
        with cols[3]:
            card("최근 하류 유량", fmt_num(ds.get("flow_rate"), " ㎥/s"), "시자료 API 스냅샷")
        st.markdown(f'<div class="notice">선정 기준: {ds.get("selection_reason", "-")}</div>', unsafe_allow_html=True)
        with st.expander("후보 관측소 보기", expanded=False):
            st.dataframe(selected_candidates.fillna("-"), width="stretch", hide_index=True)

with tab_map:
    section("20개 댐 위치", "전체 댐을 표시하고 선택한 댐은 큰 외곽 원으로 강조했습니다. 색상은 내부 운영 참고 신호입니다.")
    map_df = summary.copy()
    signal_rows = []
    for _, map_row in map_df.iterrows():
        map_weather = weather[weather["dam_code"] == int(map_row["dam_code"])].copy()
        threshold = risk_thresholds[risk_thresholds["dam_code"] == int(map_row["dam_code"])]
        threshold_row = threshold.iloc[0] if not threshold.empty else None
        signal_rows.append(internal_signal(map_row, map_weather, threshold_row))
    map_df["status_label"] = [item[0] for item in signal_rows]
    map_df["status_reason"] = [item[1] for item in signal_rows]
    map_df["color"] = [item[3] for item in signal_rows]
    selected_map_df = map_df[map_df["dam_name"] == selected_name].copy()
    st.pydeck_chart(
        pdk.Deck(
            initial_view_state=pdk.ViewState(
                latitude=36.15,
                longitude=127.9,
                zoom=6.2,
                pitch=0,
            ),
            layers=[
                pdk.Layer(
                    "ScatterplotLayer",
                    data=map_df,
                    get_position="[longitude, latitude]",
                    get_radius=9000,
                    get_fill_color="color",
                    pickable=True,
                ),
                pdk.Layer(
                    "ScatterplotLayer",
                    data=selected_map_df,
                    get_position="[longitude, latitude]",
                    get_radius=17500,
                    get_fill_color=[37, 99, 235, 55],
                    get_line_color=[29, 78, 216, 255],
                    line_width_min_pixels=4,
                    stroked=True,
                    filled=True,
                    pickable=False,
                ),
            ],
            tooltip={
                "text": "{dam_name}댐\n내부 참고 신호: {status_label}\n근거: {status_reason}\n현재 방류량: {totdcwtrqy}\n예상 방류량: {predicted_discharge_3h}"
            },
        )
    )
    st.markdown(
        """
        <div class="legend">
            <span class="legend-item"><span class="legend-dot" style="background:#16a34a;"></span>초록: 정상</span>
            <span class="legend-item"><span class="legend-dot" style="background:#ca8a04;"></span>노랑: 관심</span>
            <span class="legend-item"><span class="legend-dot" style="background:#d97706;"></span>주황: 주의</span>
            <span class="legend-item"><span class="legend-dot" style="background:#dc2626;"></span>빨강: 긴급 검토</span>
            <span class="legend-item"><span class="legend-dot" style="background:#64748b;"></span>회색: 판단 보류</span>
            <span class="legend-item"><span class="legend-dot" style="background:#2563eb;"></span>파란 외곽 원: 선택한 댐</span>
        </div>
        <div class="notice">
            정상은 뚜렷한 변화가 없는 상태, 관심은 다음 관측값을 확인할 상태, 주의는 강수량과 하류 수위를 함께 확인할 상태입니다.
            긴급 검토는 담당자가 즉시 확인할 상태이며, 판단 보류는 최신 예측값이 부족해 수집 상태부터 확인할 상태입니다.
        </div>
        """,
        unsafe_allow_html=True,
    )

with tab_five:
    section("5일 예측 해석", "3시간/6시간은 단기 판단, 1~5일은 누적 강수와 누적 방류 범위를 보는 장기 참고값입니다.")
    horizon_table = build_horizon_table(selected_name, selected_weather, five_day, horizon_by_dam)
    if horizon_table.empty:
        st.info("선택 댐의 예측 해석 데이터가 없습니다.")
    else:
        cols = st.columns(3)
        with cols[0]:
            first = horizon_table.iloc[0]
            card("단기 판단", str(first.get("방류 예측/판단", "-")), "3시간 뒤")
        with cols[1]:
            rain5 = horizon_table.loc[horizon_table["예측 구간"] == "5일 뒤", "예상 강수량"]
            card("5일 누적 강수", rain5.iloc[0] if not rain5.empty else "-", "단기/초단기 예보 스냅샷 기준")
        with cols[2]:
            row6 = horizon_table[horizon_table["예측 구간"] == "6시간 뒤"]
            card("6시간 참고", row6.iloc[0]["참고 강도"] if not row6.empty else "-", "댐별 실험 지표")
        st.dataframe(horizon_table.fillna("-"), width="stretch", hide_index=True)

    if not horizon_summary.empty:
        with st.expander("전체 예측 구간별 실험 지표 보기", expanded=False):
            st.dataframe(horizon_summary, width="stretch", hide_index=True)

st.caption("공유용 버전은 DB/API를 직접 호출하지 않고 CSV 스냅샷만 사용합니다. 실시간 API 수집은 로컬 실험용 대시보드와 자동수집 스크립트에서 수행합니다.")
