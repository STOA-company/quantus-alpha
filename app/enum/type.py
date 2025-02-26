from enum import Enum


class StockType(str, Enum):
    STOCK = "stock"  # 주식
    ETF = "etf"  # ETF
    COIN = "coin"  # 코인
