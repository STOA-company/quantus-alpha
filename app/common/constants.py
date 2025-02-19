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
    "real_estate": "부동산",
    "finance": "금융",
    "construction": "건설, 건축",
    "consumer_goods": "필수소비재",
    "hospitality": "호텔, 레저",
    "software": "소프트웨어",
    "professional": "전문 서비스",
    "energy": "에너지",
    "electronics": "전자",
    "aerospace": "항공",
    "industrial": "기계",
    "media": "미디어,교육",
    "other": "기타",
    None: "기타",
}

DEFAULT_SCREENER_COLUMNS = ["Code", "name", "market", "sector", "close", "price_change_rate", "trade_volume"]


FACTOR_CONFIGS = {
    # 기술 분석 지표
    "marketCap": {
        "direction": 1,  # 높을수록 좋음  # TODO
        "range": (0, None),  # 음수 불가
    },
    "median_trade": {
        "direction": None,  # 중간 값이 좋음 # TODO
        "range": (0, None),  # 음수 불가
        "optimal_percentile": 50,  # 중앙값에 가까울수록 좋음
    },
    "disparity_5": {
        "direction": None,  # 100%에 가까울수록 좋음 # TODO
        "range": (0, None),  # 음수 불가
        "optimal_value": 100,
    },
    "disparity_10": {
        "direction": None,  # 100%에 가까울수록 좋음 # TODO
        "range": (0, None),  # 음수 불가
        "optimal_value": 100,
    },
    "disparity_20": {
        "direction": None,  # 100%에 가까울수록 좋음 # TODO
        "range": (0, None),  # 음수 불가
        "optimal_value": 100,
    },
    "disparity_50": {
        "direction": None,  # 100%에 가까울수록 좋음 # TODO
        "range": (0, None),  # 음수 불가
        "optimal_value": 100,
    },
    "disparity_100": {
        "direction": None,  # 100%에 가까울수록 좋음 # TODO
        "range": (0, None),  # 음수 불가
        "optimal_value": 100,
    },
    "disparity_200": {
        "direction": None,  # 100%에 가까울수록 좋음 # TODO
        "range": (0, None),  # 음수 불가
        "optimal_value": 100,
    },
    "momentum_6": {"direction": 1, "range": None},
    "momentum_12": {"direction": 1, "range": None},
    "rsi_9": {
        "direction": None,  # 중간값이 좋음 # TODO
        "range": (0, 100),  # 0-100 사이
        "optimal_range": (30, 70),
    },
    "rsi_14": {
        "direction": None,  # 중간값이 좋음 # TODO
        "range": (0, 100),  # 0-100 사이
        "optimal_range": (30, 70),
    },
    "rsi_25": {
        "direction": None,  # 중간값이 좋음 # TODO
        "range": (0, 100),  # 0-100 사이
        "optimal_range": (30, 70),
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
        "optimal_value": 1,
    },
    "beta_60": {
        "direction": None,  # 1에 가까울수록 좋음 # TODO
        "range": None,  # 제한 없음
        "optimal_value": 1,
    },
    "abs_beta": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, None),  # 음수 불가
    },
    "abs_beta_60": {"direction": -1, "range": (0, None)},
    # 펀더멘털 지표
    "deptRatio": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 400),  # 400% 초과는 위험 # TODO
        "warning_threshold": 200,  # 200% 초과시 주의
    },
    "borrow_rate": {"direction": -1, "range": (0, 200)},
    "current_ratio": {
        "direction": 1,  # 높을수록 좋음
        "range": (0, 500),  # 500% 초과는 비효율 # TODO
        "warning_threshold": 50,  # 50% 미만은 위험
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
    "operating_income_ttm": {"direction": 1, "range": None},
    "roa": {
        "direction": 1,  # 높을수록 좋음
        "range": (-100, 100),  # 극단값 제한 # TODO
        "warning_threshold": -20,  # -20% 미만은 위험  # TODO
    },
    "roa_ttm": {
        "direction": 1,  # 높을수록 좋음
        "range": (-100, 100),  # 극단값 제한 # TODO
        "warning_threshold": -20,  # -20% 미만은 위험  # TODO
    },
    "roe": {
        "direction": 1,  # 높을수록 좋음
        "range": (-100, 100),  # 극단값 제한 # TODO
        "warning_threshold": -20,  # -20% 미만은 위험  # TODO
    },
    "roe_ttm": {
        "direction": 1,  # 높을수록 좋음
        "range": (-100, 100),  # 극단값 제한 # TODO
        "warning_threshold": -20,  # -20% 미만은 위험  # TODO
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
        "warning_threshold": 3,  # 3점 미만은 위험 # TODO
    },
    "z_score": {
        "direction": 1,  # 높을수록 좋음
        "range": None,  # 제한 없음
        "warning_threshold": 1.81,  # 1.81 미만은 위험 # TODO
    },
    # 가치 평가 지표
    "per": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 100),  # 음수는 제외, 100 초과는 100으로
        "warning_threshold": 0,  # 음수는 적자
    },
    "per_ttm": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 100),  # 음수는 제외, 100 초과는 100으로
        "warning_threshold": 0,  # 음수는 적자
    },
    "pbr": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 10),  # 음수는 제외, 10 초과는 10으로
        "warning_threshold": 0,  # 음수는 자본잠식
    },
    "psr": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 10),  # 음수는 제외, 10 초과는 10으로
    },
    "psr_ttm": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 10),  # 음수는 제외, 10 초과는 10으로
    },
    "por": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 50),  # 음수는 제외, 50 초과는 50으로
    },
    "por_ttm": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 50),  # 음수는 제외, 50 초과는 50으로
    },
    "pcr": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 20),  # 음수는 제외, 20 초과는 20으로
    },
    "pcr_ttm": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 20),  # 음수는 제외, 20 초과는 20으로
    },
    "pgpr": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 20),  # 음수는 제외, 20 초과는 20으로
    },
    "pgpr_ttm": {
        "direction": -1,  # 낮을수록 좋음
        "range": (0, 20),  # 음수는 제외, 20 초과는 20으로
    },
}
