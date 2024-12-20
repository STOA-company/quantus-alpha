from typing import List, Optional
from pydantic import BaseModel


class TrendingStockKr(BaseModel):
    num: int
    ticker: str
    name: str
    volume: float
    current_price: Optional[float]
    current_price_rate: Optional[float]


class TrendingStockEn(BaseModel):
    num: int
    ticker: str
    name: str
    volume: float
    current_price: Optional[float]
    current_price_rate: Optional[float]


class TrendingStock(BaseModel):
    kr: List[TrendingStockKr]
    en: List[TrendingStockEn]
