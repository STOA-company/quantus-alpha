from typing import List
from pydantic import BaseModel


class TrendingStock(BaseModel):
    ticker: str
    rank: int
    name_kr: str
    name_en: str
    current_price: float
    change: float
    volume: float
    volume_change: float


class TrendingStockPeriodResponse(BaseModel):
    realtime_trending_stock: TrendingStock
    day_trending_stock: TrendingStock
    week_trending_stock: TrendingStock
    month_trending_stock: TrendingStock
    six_month_trending_stock: TrendingStock
    year_trending_stock: TrendingStock


class TrendingStockTyperesponse(BaseModel):
    up: List[TrendingStockPeriodResponse]
    down: List[TrendingStockPeriodResponse]
    vol: List[TrendingStockPeriodResponse]
    amt: List[TrendingStockPeriodResponse]


class TrendingStockResponse(BaseModel):
    kr: TrendingStockTyperesponse
    us: TrendingStockTyperesponse
