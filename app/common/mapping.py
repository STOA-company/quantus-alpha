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

# krx
multiplier_map = {"일반": 1, "2X 레버리지": 2, "2X 인버스": -2, "1X 인버스": -1, "1.5X 레버리지": 1.5}

replication_map = {
    "실물(액티브)": "active",
    "실물(패시브)": "passive",
    "합성(패시브)": "passive",
    "합성(액티브)": "active",
}

base_asset_classification_map = {
    "채권": "bond",
    "주식": "stock",
    "기타": "others",
    "혼합자산": "blend",
    "부동산": "real_estate",
    "통화": "currency",
    "원자재": "commodity",
}

etf_risk_map = {"매우 높음": 1, "높음": 2, "보통": 3, "낮음": 4, "매우 낮음": 5}

# refinitiv
etf_market_map = {
    177: "KRX",
    244: "NYSE",
    135: "NASDAQ",
    278: "BATS",
    147: "OTC",
    145: "NYSE",
}

etf_column_mapping = {
    "Ticker": "ticker",
    "MarketDate": "date",
    "Open_": "open",
    "High": "high",
    "Low": "low",
    "Close_": "close",
    "Volume": "volume",
    "ExchIntCode": "market",
    "Bid": "bid",
    "Ask": "ask",
    "NumShrs": "num_shrs",
    "거래대금": "trade_amount",
}
