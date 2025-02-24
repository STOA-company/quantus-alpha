import pytz


KR_EXCLUDE_DATES = ["2024-12-30", "2025-01-27"]
US_EXCLUDE_DATES = ["2025-01-09"]

KST = pytz.timezone("Asia/Seoul")
UTC = pytz.timezone("UTC")
USE = pytz.timezone("America/New_York")

DEFAULT_SCREENER_COLUMNS = ["Code", "Name", "ExchMnem", "sector"]

NEED_TO_MULTIPLY_100 = ["vol", "vol_60", "momentum_1", "momentum_3", "momentum_6", "momentum_12", "current_ratio"]
