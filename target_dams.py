# target_dams.py

"""
AI 기반 댐 관리 시스템 MVP 대상 댐 목록

기준:
- 김천부항댐 제외
- 20개 댐만 사용
- 다목적댐 운영 정보, 수문 운영 정보, 기상 예보 수집, 대시보드, AI 학습에 공통 적용
"""

TARGET_DAMS_20 = [
    "소양강",
    "충주",
    "횡성",
    "안동",
    "임하",
    "성덕",
    "영주",
    "군위",
    "보현산",
    "대청",
    "용담",
    "섬진강",
    "주암(본)",
    "주암(조)",
    "합천",
    "남강",
    "밀양",
    "보령",
    "부안",
    "장흥",
]


# 기존 dam_location 또는 과거 코드에서 영문명이 섞여 있을 가능성까지 대비
DAM_NAME_ALIASES = {
    "소양강": ["소양강", "소양강댐", "Soyang River", "Soyang"],
    "충주": ["충주", "충주댐", "Chungju"],
    "횡성": ["횡성", "횡성댐", "Hoengseong"],
    "안동": ["안동", "안동댐", "Andong"],
    "임하": ["임하", "임하댐", "Imha"],
    "성덕": ["성덕", "성덕댐", "Seongdeok"],
    "영주": ["영주", "영주댐", "Yeongju"],
    "군위": ["군위", "군위댐", "Gunwi"],
    "보현산": ["보현산", "보현산댐", "Bohyeon Mountain", "Bohyeonsan"],
    "대청": ["대청", "대청댐", "Daecheong"],
    "용담": ["용담", "용담댐", "Yongdam"],
    "섬진강": ["섬진강", "섬진강댐", "Seomjin River", "Seomjingang"],
    "주암(본)": ["주암(본)", "주암본", "주암본댐", "주암(본댐)", "Juam (main)", "Juam main"],
    "주암(조)": ["주암(조)", "주암조", "주암조절지", "주암(조)", "Juam (Jo)", "Juam Jo"],
    "합천": ["합천", "합천댐", "Hapcheon"],
    "남강": ["남강", "남강댐", "Nam River", "Namgang"],
    "밀양": ["밀양", "밀양댐", "Miryang"],
    "보령": ["보령", "보령댐", "Boryeong"],
    "부안": ["부안", "부안댐", "Buan"],
    "장흥": ["장흥", "장흥댐", "Jangheung"],
}


EXCLUDED_DAMS = [
    "김천부항",
    "김천부항댐",
    "Gimcheon Buhang",
    "Gimcheon Buhang Dam",
]


def normalize_name(name):
    """
    댐 이름 비교를 위한 문자열 정리
    """
    if name is None:
        return ""

    return str(name).strip()


def get_standard_dam_name(name):
    """
    한글/영문/별칭으로 들어온 댐 이름을 표준 한글명으로 변환한다.
    대상 20개 댐이 아니면 None을 반환한다.
    """
    name = normalize_name(name)

    if name in EXCLUDED_DAMS:
        return None

    for standard_name, aliases in DAM_NAME_ALIASES.items():
        if name in aliases:
            return standard_name

    return None


def is_target_dam(name):
    """
    김천부항 제외 20개 댐에 해당하는지 확인한다.
    """
    return get_standard_dam_name(name) is not None


def filter_target_dams(df, dam_col="dam_name", standardize=True):
    """
    DataFrame에서 김천부항 제외 20개 댐만 남긴다.

    Parameters
    ----------
    df : pandas.DataFrame
        필터링할 DataFrame
    dam_col : str
        댐 이름 컬럼명
    standardize : bool
        True이면 댐 이름을 표준 한글명으로 바꾼다.

    Returns
    -------
    pandas.DataFrame
        20개 댐만 남긴 DataFrame
    """
    if df is None or df.empty:
        return df

    if dam_col not in df.columns:
        raise ValueError(f"DataFrame에 {dam_col} 컬럼이 없습니다.")

    result = df.copy()
    result["_standard_dam_name"] = result[dam_col].apply(get_standard_dam_name)
    result = result[result["_standard_dam_name"].notna()].copy()

    if standardize:
        result[dam_col] = result["_standard_dam_name"]

    result = result.drop(columns=["_standard_dam_name"])

    return result