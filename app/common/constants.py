import pytz


KR_EXCLUDE_DATES = ["2024-12-30", "2025-01-27"]
US_EXCLUDE_DATES = ["2025-01-09"]

KST = pytz.timezone("Asia/Seoul")
UTC = pytz.timezone("UTC")
USE = pytz.timezone("America/New_York")

NEED_TO_MULTIPLY_100 = ["vol", "vol_60", "momentum_1", "momentum_3", "momentum_6", "momentum_12", "current_ratio"]

DEFAULT_COLUMNS = ["Code", "Name", "country"]

NON_NUMERIC_COLUMNS = ["Code", "Name", "market", "sector", "country", "score"]

UNIT_MAP = {"percentage": "%", "times": "회", "score": "점", "multiple": "배", "ratio": ""}

MARKET_MAP = {
    "KOSPI": "코스피",
    "KOSDAQ": "코스닥",
    "NAS": "나스닥",
    "NYS": "뉴욕 증권 거래소",
    "AMS": "아멕스",
}

FACTOR_MAP = {
    "Code": "티커",
    "Name": "종목명",
    "country": "국가",
    "market": "시장",
    "sector": "산업",
    "score": "스코어",
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

########################################################
# ETF
########################################################

ETF_DATA_DIR = "check_data/etf"
KRX_DIR = "check_data/etf_krx"
PARQUET_DIR = "parquet"
MORNINGSTAR_DIR = "check_data/etf_morningstar"

ETF_FACTOR_LIST = [
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "bid",
    "ask",
    "num_shrs",
    "trade_amount",
    "market_cap",
    "momentum_1",
    "momentum_3",
    "momentum_6",
    "momentum_12",
    "ba_absolute_spread",
    "ba_relative_spread",
    "ba_spread_20d_avg",
    "ba_spread_20d_std",
    "ba_spread_1d_change",
    "ba_spread_20d_change",
    "disparity_5",
    "disparity_10",
    "disparity_20",
    "disparity_50",
    "disparity_100",
    "disparity_200",
    "vol_60",
    "vol",
    "rsi_9",
    "rsi_14",
    "rsi_25",
    "sharpe",
    "sortino",
    "return_1m",
    "return_3m",
    "return_6m",
    "return_1y",
    "week_52_high",
    "week_52_low",
    "drawdown_1y",
    "median_trade",
    "dividend_count",
    "last_dividend_date",
    "last_dividend_per_share",
    "recent_dividend_yield",
    "dividend_growth_rate_3y",
    "dividend_growth_rate_5y",
    "ctry",
    "market",
    "kr_name",
    "en_name",
    "listing_date",
    "base_index_name",
    "tracking_multiplier",
    "replication_method",
    "base_asset_classification",
    "manager",
    "total_fee",
    "tax_type",
    "tracking_error",
    "disparity",
    "volatility",
    "is_hedge",
]
FACTOR_MAP_ETF = {v: k for k, v in FACTOR_MAP.items() if k in ETF_FACTOR_LIST}
