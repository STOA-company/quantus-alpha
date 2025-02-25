import pytz


KR_EXCLUDE_DATES = ["2024-12-30", "2025-01-27"]
US_EXCLUDE_DATES = ["2025-01-09"]

KST = pytz.timezone("Asia/Seoul")
UTC = pytz.timezone("UTC")
USE = pytz.timezone("America/New_York")

DEFAULT_SCREENER_COLUMNS = ["Code", "Name", "country", "ExchMnem", "sector"]

NEED_TO_MULTIPLY_100 = ["vol", "vol_60", "momentum_1", "momentum_3", "momentum_6", "momentum_12", "current_ratio"]

FACTOR_MAP = {
    "abs_beta": "절대값 베타 (52주)",
    "abs_beta_60": "절대값 베타 (60일)",
    "beta": "베타 (52주)",
    "beta_60": "베타 (60일)",
    "close": "종가 (수정 전)",
    "disparity_10": "이격도 (10일)",
    "disparity_100": "이격도 (100일)",
    "disparity_20": "이격도 (20일)",
    "disparity_200": "이격도 (200일)",
    "disparity_5": "이격도 (5일)",
    "disparity_50": "이격도 (50일)",
    "marketCap": "시가총액",
    "median_trade": "중위 월간 거래량",
    "momentum_1": "모멘텀 (1개월)",
    "momentum_12": "모멘텀 (12개월)",
    "momentum_3": "모멘텀 (3개월)",
    "momentum_6": "모멘텀 (6개월)",
    "rsi_14": "RSI (14일)",
    "rsi_25": "RSI (25일)",
    "rsi_9": "RSI (9일)",
    "sharpe": "샤프 비율 (52주)",
    "sortino": "Sortino 비율 (52주)",
    "vol": "변동성 (52주)",
    "vol_60": "변동성 (60일)",
    "assetturnover": "Asset Turnover",
    "assetturnover_ttm": "Asset Turnover (TTM)",
    "borrow_rate": "차입 비율",
    "current_ratio": "유동 비율",
    "deptRatio": "부채 비율",
    "fscore": "F-score",
    "gpa": "GP/A",
    "gpa_ttm": "GP/A (TTM)",
    "gross_profit": "매출 총 이익",
    "gross_profit_ttm": "매출 총 이익 (TTM)",
    "operating_income": "영업 이익",
    "operating_income_ttm": "영업 이익 (TTM)",
    "reserve_ratio": "유보율",
    "rev": "매출액",
    "rev_ttm": "매출액 (TTM)",
    "roa": "ROA",
    "roa_ttm": "ROA (TTM)",
    "roe": "ROE",
    "roe_ttm": "ROE (TTM)",
    "z_score": "Altman Z-score",
    "pbr": "PBR",
    "pcr": "PCR",
    "pcr_ttm": "PCR (TTM)",
    "per": "PER",
    "per_ttm": "PER (TTM)",
    "pgpr": "PGPR",
    "pgpr_ttm": "PGPR (TTM)",
    "por": "POR",
    "por_ttm": "POR (TTM)",
    "psr": "PSR",
    "psr_ttm": "PSR (TTM)",
}

REVERSE_FACTOR_MAP = {v: k for k, v in FACTOR_MAP.items()}
