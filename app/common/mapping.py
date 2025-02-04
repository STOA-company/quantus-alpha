import pytz

timezone_map = {
    "KR": pytz.timezone("Asia/Seoul"),
    "US": pytz.timezone("America/New_York"),
    "JP": pytz.timezone("Asia/Tokyo"),
    "HK": pytz.timezone("Asia/Hong_Kong"),
}

market_close_times_map = {
    "KR": {"hour": 15, "minute": 30, "second": 0},  # 한국 장 마감: 15:30
    "US": {"hour": 16, "minute": 0, "second": 0},  # 미국 장 마감: 16:00
    "JP": {"hour": 14, "minute": 50, "second": 0},  # 일본 장 마감: 14:50
    "HK": {"hour": 16, "minute": 0, "second": 0},  # 홍콩 장 마감: 16:00
}
