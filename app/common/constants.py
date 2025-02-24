import pytz


KR_EXCLUDE_DATES = ["2024-12-30", "2025-01-27"]
US_EXCLUDE_DATES = ["2025-01-09"]

KST = pytz.timezone("Asia/Seoul")
UTC = pytz.timezone("UTC")
USE = pytz.timezone("America/New_York")

SECTOR_MAP = {
    "건강관리": "건강관리",
    "자동차": "자동차",
    "화장품,의류,완구": "화장품,의류,완구",
    "화학": "화학",
    "필수소비재": "필수소비재",
    "운송": "운송",
    "상사,자본재": "상사,자본재",
    "비철,목재등": "비철,목재등",
    "건설,건축관련": "건설,건축관련",
    "보험": "보험",
    "에너지": "에너지",
    "기계": "기계",
    "철강": "철강",
    "반도체": "반도체",
    "IT하드웨어": "IT하드웨어",
    "증권": "증권",
    "디스플레이": "디스플레이",
    "IT가전": "IT가전",
    "소매(유통)": "소매(유통)",
    "유틸리티": "유틸리티",
    "은행": "은행",
    "통신서비스": "통신서비스",
    "호텔,레저서비스": "호텔,레저서비스",
    "소프트웨어": "소프트웨어",
    "조선": "조선",
    "미디어,교육": "미디어,교육",
    "real_estate": "부동산",  # TODO
    "finance": "금융",
    "construction": "건설,건축관련",
    "consumer_goods": "필수소비재",
    "hospitality": "호텔,레저서비스",
    "software": "소프트웨어",
    "professional": "전문 서비스",  # TODO
    "energy": "에너지",
    "electronics": "전자",  # TODO
    "aerospace": "항공",  # TODO
    "industrial": "기계",
    "media": "미디어,교육",
    "other": "기타",
    None: "기타",
}

DEFAULT_SCREENER_COLUMNS = ["Code", "Name", "ExchMnem", "WI26업종명(대)"]

NEED_TO_MULTIPLY_100 = ["vol", "vol_60", "momentum_1", "momentum_3", "momentum_6", "momentum_12", "current_ratio"]
