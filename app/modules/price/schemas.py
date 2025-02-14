from datetime import datetime, date
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional


class PriceDataResponse(BaseModel):
    data: List[Dict[str, Any]] = Field(
        ...,
        example=[
            {"date": "2023-01-01", "open": 100, "high": 105, "low": 98, "close": 102, "volume": 1000000},
            {"date": "2023-01-02", "open": 102, "high": 107, "low": 101, "close": 106, "volume": 1200000},
        ],
    )


class PriceDataItem(BaseModel):
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    daily_price_change_rate: float


class ResponsePriceDataItem(BaseModel):
    name: str
    ticker: str
    market: str
    market_cap: Optional[float] = None
    week52_highest: float
    week52_lowest: float
    last_day_close: float = 0.0
    price_data: List[PriceDataItem]


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


# v2
class PriceSummaryItem(BaseModel):
    name: Optional[str] = None
    ticker: Optional[str] = None
    ctry: Optional[str] = None
    logo_url: Optional[str] = None
    market: Optional[str] = None
    sector: Optional[str] = None
    market_cap: Optional[float] = None
    last_day_close: Optional[float] = None
    week_52_low: Optional[float] = None
    week_52_high: Optional[float] = None
    is_market_close: Optional[bool] = None


class PriceDailyItem(BaseModel):
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    price_change_rate: float


class PriceMinuteItem(BaseModel):
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    price_change_rate: float


class RealTimePriceDataItem(BaseModel):
    ctry: str
    price: float
    price_change: float
    price_change_rate: float
    is_trading_stopped: bool
    is_cared: bool
    is_warned: bool
