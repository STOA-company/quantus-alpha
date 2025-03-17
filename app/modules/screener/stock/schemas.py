from pydantic import BaseModel
from app.models.models_factors import CategoryEnum
from app.models.models_screener import StockType
from typing import Optional, List, Dict
from enum import Enum


class MarketEnum(str, Enum):
    US = "us"
    KR = "kr"
    SNP500 = "S&P 500"
    NASDAQ = "나스닥"
    KOSPI = "코스피"
    KOSDAQ = "코스닥"
    ALL = "all"


class FactorResponse(BaseModel):
    factor: str
    description: str
    unit: str
    category: CategoryEnum
    direction: str
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    type: Optional[str] = None

    class Config:
        from_attributes = True


class FilterCondition(BaseModel):
    factor: str
    above: Optional[float] = None
    below: Optional[float] = None


class GroupMetaData(BaseModel):
    id: int
    name: str
    type: Optional[StockType] = StockType.STOCK


class SortInfo(BaseModel):
    sort_by: Optional[str] = None
    ascending: Optional[bool] = None


class GroupFilter(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    type: Optional[StockType] = StockType.STOCK
    market_filter: Optional[MarketEnum] = MarketEnum.US
    sector_filter: Optional[List[str]] = None
    category: Optional[CategoryEnum] = None
    custom_filters: Optional[List[FilterCondition]] = None
    factor_filters: Optional[Dict[str, List[str]]] = None
    sort_info: Optional[Dict[CategoryEnum, SortInfo]] = None


class GroupFilterResponse(GroupFilter):
    has_custom: bool = False


class FilteredStocks(BaseModel):
    market_filter: Optional[MarketEnum] = MarketEnum.ALL
    sector_filter: Optional[List[str]] = None
    custom_filters: Optional[List[FilterCondition]] = None
    factor_filters: Optional[List[str]] = None
    limit: Optional[int] = 50
    offset: Optional[int] = 0
    sort_info: Optional[SortInfo] = None
    lang: Optional[str] = "kr"


class ColumnSet(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    columns: Optional[List[str]] = None


class ColumnsResponse(BaseModel):
    columns: List[str]
