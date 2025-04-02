from pydantic import BaseModel
from app.models.models_factors import CategoryEnum, FactorTypeEnum
from app.models.models_screener import StockType
from app.modules.screener.etf.enum import ETFMarketEnum
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


class ExcludeEnum(str, Enum):
    FINANCIAL = "financial"  # 금융주
    HOLDING = "holding"  # 지주사
    WARNED = "warned"  # 관리종목
    DEFICIT = "deficit"  # 적자기업(분기)
    ANNUAL_DEFICIT = "annual_deficit"  # 적자기업(연간)
    CHINESE = "chinese"  # 중국기업
    PTP = "ptp"  # PTP(Penny Stock) 기업


class FilterValue(BaseModel):
    value: Optional[str] = None
    above: Optional[float] = None
    below: Optional[float] = None


class FactorResponse(BaseModel):
    factor: str
    description: str
    unit: str
    category: CategoryEnum
    direction: str
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    type: Optional[str] = None
    presets: List[Dict] = []

    class Config:
        from_attributes = True


class FilterCondition(BaseModel):
    factor: str
    type: Optional[FactorTypeEnum] = None
    value: Optional[str] = None
    above: Optional[float] = None
    below: Optional[float] = None
    values: Optional[List[FilterValue]] = None


class GroupMetaData(BaseModel):
    id: int
    name: str
    type: Optional[StockType] = StockType.STOCK


class SortInfo(BaseModel):
    sort_by: str
    ascending: bool = False


class StockFilter(BaseModel):
    factor: str
    type: Optional[FactorTypeEnum] = None
    value: Optional[str] = None
    above: Optional[float] = None
    below: Optional[float] = None
    values: Optional[List[FilterValue]] = None


class GroupFilter(BaseModel):
    id: Optional[int] = None
    name: str
    market_filter: Optional[MarketEnum] = None
    sector_filter: Optional[List[str]] = None
    exclude_filters: Optional[List[ExcludeEnum]] = None
    category: Optional[CategoryEnum] = None
    custom_filters: Optional[List[FilterCondition]] = None
    factor_filters: Optional[Dict[str, List[str]]] = None
    sort_info: Optional[Dict[CategoryEnum, SortInfo]] = None
    type: Optional[StockType] = StockType.STOCK


class GroupFilterResponse(BaseModel):
    id: int
    name: str
    market_filter: Optional[str] = None
    sector_filter: List[str] = []
    exclude_filters: List[str] = []
    custom_filters: List[StockFilter] = []
    factor_filters: Dict[str, List[str]] = {}
    sort_info: Dict[CategoryEnum, SortInfo] = {}
    has_custom: bool = False
    type: Optional[StockType] = StockType.STOCK


class ETFGroupFilter(GroupFilter):
    market_filter: Optional[ETFMarketEnum] = ETFMarketEnum.ALL
    type: Optional[StockType] = StockType.ETF


class ETFGroupFilterResponse(ETFGroupFilter):
    has_custom: bool = False


class FilteredStocks(BaseModel):
    market_filter: Optional[MarketEnum] = None
    sector_filter: Optional[List[str]] = None
    exclude_filters: Optional[List[ExcludeEnum]] = None
    custom_filters: Optional[List[FilterCondition]] = None
    factor_filters: List[str] = []
    sort_info: Optional[SortInfo] = None
    limit: int = 20
    offset: int = 0
    lang: str = "kr"


class ColumnSet(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    columns: Optional[List[str]] = None


class ColumnsResponse(BaseModel):
    columns: List[str]
