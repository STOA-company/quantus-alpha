from pydantic import BaseModel
from app.modules.common.enum import TrendingPeriod, TrendingType


class TrendingStockRequest(BaseModel):
    ctry: str = "us"
    type: TrendingType = TrendingType.UP
    period: TrendingPeriod = TrendingPeriod.REALTIME


class TrendingStock(BaseModel):
    num: int
    ticker: str
    name: str | None = ""
    current_price: float | None = 0.0
    current_price_rate: float | None = 0.0
    volume: float | None = 0.0
    amount: float | None = 0.0
