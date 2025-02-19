from pydantic import BaseModel
from app.models.models_factors import UnitEnum, CategoryEnum
from typing import Optional, List


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


class Filter(BaseModel):
    name: Optional[str] = None
    conditions: Optional[List[FilterCondition]] = None


class FilterUpdate(BaseModel):
    filter_id: int
    filter: Filter


class FilterInfo(BaseModel):
    id: int
    name: str

    class Config:
        from_attributes = True


class FilteredStocks(BaseModel):
    market_filter: Optional[List[str]] = None
    sector_filter: Optional[List[str]] = None
    custom_filters: Optional[List[FilterCondition]] = None
    columns: Optional[List[str]] = None


class ColumnSet(BaseModel):
    id: int
    name: str
    columns: List[str]


class ColumnSetCreate(BaseModel):
    name: str
    columns: List[str]


class ColumnUpdate(BaseModel):
    column_set_id: int
    name: str
    columns: List[str]
