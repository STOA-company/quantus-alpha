from fastapi import Query
from pydantic import BaseModel

from app.modules.common.enum import TrendingCountry, TrendingPeriod, TrendingType


class TrendingStockRequest(BaseModel):
    ctry: TrendingCountry = Query(TrendingCountry.US, description="국가 코드")
    type: TrendingType = Query(TrendingType.UP, description="트렌딩 타입")
    period: TrendingPeriod = Query(TrendingPeriod.REALTIME, description="기간")


class TrendingStock(BaseModel):
    num: int
    ticker: str
    name: str = "Temp_name"
    volume: float = 0.0
    amount: float = 0.0
    current_price: float = 0.0
    current_price_rate: float = 0.0
