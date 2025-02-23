import pytz


KR_EXCLUDE_DATES = ["2024-12-30", "2025-01-27"]
US_EXCLUDE_DATES = ["2025-01-09"]

KST = pytz.timezone("Asia/Seoul")
UTC = pytz.timezone("UTC")
USE = pytz.timezone("America/New_York")

FACTOR_RENAME_MAP = {
    "ExchMnem": "market",
    "WI26업종명(대)": "sector",
    "Name": "name",
    "거래대금": "trade_volume",
    "수정주가수익률": "price_change_rate",
}

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

DEFAULT_SCREENER_COLUMNS = ["Code", "name", "market", "sector", "close", "price_change_rate", "trade_volume"]

NEED_TO_MULTIPLY_100 = ["vol", "vol_60", "momentum_1", "momentum_3", "momentum_6", "momentum_12", "current_ratio"]

FACTOR_CONFIGS = {
    "close": {
        "direction": 1,  # 높을수록 좋음  # TODO
        "range": (0, None),  # 음수 불가
    },
    "price_change_rate": {
        "direction": -1,  # 높을수록 좋음  # TODO
        "range": None,
    },
    "trade_volume": {
        "direction": 1,  # 높을수록 좋음  # TODO
        "range": (0, None),  # 음수 불가
    },
    "median_trade": {
        "direction": None,  # 중간 값이 좋음 # TODO
        "range": (0, None),  # 음수 불가
    },
    "disparity_5": {
        "direction": None,  # 100%에 가까울수록 좋음 # TODO
        "range": (0, None),  # 음수 불가
    },
    "disparity_10": {
        "direction": None,  # 100%에 가까울수록 좋음 # TODO
        "range": (0, None),  # 음수 불가
    },
    "disparity_20": {
        "direction": None,  # 100%에 가까울수록 좋음 # TODO
        "range": (0, None),  # 음수 불가
    },
    "disparity_50": {
        "direction": None,  # 100%에 가까울수록 좋음 # TODO
        "range": (0, None),  # 음수 불가
    },
    "disparity_100": {
        "direction": None,  # 100%에 가까울수록 좋음 # TODO
        "range": (0, None),  # 음수 불가
    },
    "disparity_200": {
        "direction": None,  # 100%에 가까울수록 좋음 # TODO
        "range": (0, None),  # 음수 불가
    },
    "momentum_6": {
        "direction": 1,
        "range": None,
    },
    "momentum_12": {
        "direction": 1,
        "range": None,
    },
    "rsi_9": {
        "direction": None,  # 중간값이 좋음 # TODO
        "range": (0, 100),  # 0-100 사이
    },
    "rsi_14": {
        "direction": None,  # 중간값이 좋음 # TODO
        "range": (0, 100),  # 0-100 사이
    },
    "rsi_25": {
        "direction": None,  # 중간값이 좋음 # TODO
        "range": (0, 100),  # 0-100 사이
    },
    "sharpe": {
        "direction": 1,  # 높을수록 좋음
        "range": None,  # 제한 없음
    },
    "sortino": {
        "direction": 1,  # 높을수록 좋음
        "range": None,  # 제한 없음
    },
    "vol": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, None),  # 음수 불가
    },
    "vol_60": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, None),  # 음수 불가
    },
    "beta": {
        "direction": None,  # 1에 가까울수록 좋음 # TODO
        "range": None,  # 제한 없음
    },
    "beta_60": {
        "direction": None,  # 1에 가까울수록 좋음 # TODO
        "range": None,  # 제한 없음
    },
    "abs_beta": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, None),  # 음수 불가
    },
    "abs_beta_60": {
        "direction": -1,
        "range": (0, None),
    },
    # 펀더멘털 지표
    "deptRatio": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 400),  # 400% 초과는 위험 # TODO
    },
    "borrow_rate": {
        "direction": -1,
        "range": (0, 200),
    },
    "current_ratio": {
        "direction": 1,  # 높을수록 좋음
        "range": (0, 500),  # 500% 초과는 비효율 # TODO
    },
    "reserve_ratio": {
        "direction": 1,  # 높을수록 좋음
        "range": (0, None),  # 음수 불가
    },
    "rev": {
        "direction": 1,  # 높을수록 좋음
        "range": (0, None),  # 음수 불가
    },
    "rev_ttm": {
        "direction": 1,  # 높을수록 좋음
        "range": (0, None),  # 음수 불가
    },
    "gross_profit": {
        "direction": 1,  # 높을수록 좋음
        "range": (0, None),  # 음수 불가
    },
    "gross_profit_ttm": {
        "direction": 1,  # 높을수록 좋음
        "range": (0, None),  # 음수 불가
    },
    "operating_income": {
        "direction": 1,  # 높을수록 좋음
        "range": None,  # 음수 가능
    },
    "operating_income_ttm": {
        "direction": 1,
        "range": None,
    },
    "roa": {
        "direction": 1,  # 높을수록 좋음
        "range": (-100, 100),  # 극단값 제한 # TODO
    },
    "roa_ttm": {
        "direction": 1,  # 높을수록 좋음
        "range": (-100, 100),  # 극단값 제한 # TODO
    },
    "roe": {
        "direction": 1,  # 높을수록 좋음
        "range": (-100, 100),  # 극단값 제한 # TODO
    },
    "roe_ttm": {
        "direction": 1,  # 높을수록 좋음
        "range": (-100, 100),  # 극단값 제한 # TODO
    },
    "gpa": {
        "direction": 1,  # 높을수록 좋음
        "range": (-100, 100),  # 극단값 제한 # TODO
    },
    "gpa_ttm": {
        "direction": 1,  # 높을수록 좋음
        "range": (-100, 100),  # 극단값 제한 # TODO
    },
    "assetturnover": {
        "direction": 1,  # 높을수록 좋음
        "range": (0, None),  # 음수 가능
    },
    "assetturnover_ttm": {
        "direction": 1,  # 높을수록 좋음
        "range": (0, None),  # 음수 가능
    },
    "fscore": {
        "direction": 1,  # 높을수록 좋음
        "range": (0, 9),  # 0-9점
    },
    "z_score": {
        "direction": 1,  # 높을수록 좋음
        "range": None,  # 제한 없음
    },
    # 가치 평가 지표
    "per": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 100),
    },
    "per_ttm": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 100),
    },
    "pbr": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 10),
    },
    "psr": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 10),
    },
    "psr_ttm": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 10),
    },
    "por": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 50),
    },
    "por_ttm": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 50),
    },
    "pcr": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 20),
    },
    "pcr_ttm": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 20),
    },
    "pgpr": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 20),
    },
    "pgpr_ttm": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 20),
    },
}
