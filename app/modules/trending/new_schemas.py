from pydantic import BaseModel
from app.modules.common.enum import TrendingPeriod, TrendingType


class TrendingStockRequest(BaseModel):
    ctry: str = "us"
    type: TrendingType = TrendingType.UP
    period: TrendingPeriod = TrendingPeriod.REALTIME


class TrendingStock(BaseModel):
    num: int
    ticker: str
    name: str = "Temp_name"
    current_price: float = 0.0
    current_price_rate: float = 0.0
    volume: float = 0.0
    amount: float = 0.0
