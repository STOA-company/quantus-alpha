from datetime import datetime
from pydantic import BaseModel, Field
from typing import List, Dict, Any


class PriceDataResponse(BaseModel):
    data: List[Dict[str, Any]] = Field(
        ...,
        example=[
            {"date": "2023-01-01", "open": 100, "high": 105, "low": 98, "close": 102, "volume": 1000000},
            {"date": "2023-01-02", "open": 102, "high": 107, "low": 101, "close": 106, "volume": 1200000},
        ],
    )


class PriceDataItem(BaseModel):
    ticker: str
    date: datetime
    name: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    daily_price_change_rate: float


class StockKrFactorItem(BaseModel):
    ticker: str
    name: str
    prev_close: float
    week_52_high: float
    week_52_low: float
    all_time_high: float
    all_time_low: float
    momentum_1m: float
    momentum_3m: float
    momentum_6m: float
    momentum_12m: float
    rate_of_change_10d: float
    rate_of_change_30d: float
    rate_of_change_60d: float
