from pydantic import BaseModel
from app.models.models_factors import UnitEnum, CategoryEnum
from typing import Optional, List
from enum import Enum


class MarketEnum(str, Enum):
    US = "us"
    KR = "kr"
    SNP500 = "S&P 500"
    NASDAQ = "NASDAQ"
    KOSPI = "KOSPI"
    KOSDAQ = "KOSDAQ"


class FactorResponse(BaseModel):
    factor: str
    description: str
    unit: UnitEnum
    category: CategoryEnum

    class Config:
        from_attributes = True


class FilterCondition(BaseModel):
    factor: str
    above: Optional[float] = None
    below: Optional[float] = None


class FilterGroup(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    market_filter: Optional[MarketEnum] = MarketEnum.US
    sector_filter: Optional[List[str]] = None
    custom_filters: Optional[List[FilterCondition]] = None


class FilterInfo(BaseModel):
    id: int
    name: str

    class Config:
        from_attributes = True


class FilteredStocks(BaseModel):
    market_filter: Optional[MarketEnum] = MarketEnum.US
    sector_filter: Optional[List[str]] = None
    custom_filters: Optional[List[FilterCondition]] = None
    columns: Optional[List[str]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None


class ColumnSet(BaseModel):
    id: int
    name: str
    columns: List[str]


class ColumnSetCreate(BaseModel):
    name: str
    columns: List[str]


class ColumnSetUpdate(BaseModel):
    column_set_id: int
    name: str
    columns: List[str]


class ColumnsResponse(BaseModel):
    columns: List[str]

    class Config:
        from_attributes = True
