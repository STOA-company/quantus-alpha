from pydantic import BaseModel
from app.models.models_factors import UnitEnum, SortDirectionEnum, CategoryEnum
from typing import Optional, List


class FactorResponse(BaseModel):
    factor: str
    description: str
    unit: UnitEnum
    sort_direction: SortDirectionEnum
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


class FilterInfo(BaseModel):
    id: int
    name: str

    class Config:
        from_attributes = True
