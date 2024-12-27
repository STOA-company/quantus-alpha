def calculate_change_rate(current_price: float, previous_price: float) -> float:
    """가격 변동률 계산"""
    if not previous_price:
        return 0.0
    return ((current_price - previous_price) / previous_price) * 100


def run_stock_trend_realtime_batch():
    pass


def run_stock_trend_batch():
    pass
