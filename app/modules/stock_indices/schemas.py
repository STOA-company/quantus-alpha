from pydantic import BaseModel
from typing import Dict, Optional


class TimeData(BaseModel):
    open: float
    high: float
    low: float
    close: float
    volume: float


class IndexSummary(BaseModel):
    prev_close: float = 0.00
    change: float = 0.00
    change_percent: float = 0.00
    rise_ratio: float = 0.00
    fall_ratio: float = 0.00
    unchanged_ratio: float = 0.00
    is_open: bool = False


class IndicesResponse(BaseModel):
    kospi: Dict[str, TimeData]
    kosdaq: Dict[str, TimeData]
    nasdaq: Dict[str, TimeData]
    sp500: Dict[str, TimeData]


class IndicesData(BaseModel):
    status_code: int
    message: str
    kospi: IndexSummary
    kosdaq: IndexSummary
    nasdaq: IndexSummary
    sp500: IndexSummary
    data: Optional[IndicesResponse] = None
