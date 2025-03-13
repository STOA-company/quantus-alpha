from pydantic import BaseModel
from app.modules.screener.stock.schemas import FilterCondition
from .enum import ETFMarketEnum
from typing import Optional, List


class FilteredETF(BaseModel):
    market_filter: Optional[ETFMarketEnum] = ETFMarketEnum.US
    custom_filters: Optional[List[FilterCondition]] = []
    factor_filters: Optional[List[str]] = []
    limit: Optional[int] = 50
    offset: Optional[int] = 0
    sort_by: Optional[str] = None
    ascending: Optional[bool] = False
    lang: Optional[str] = "kr"
