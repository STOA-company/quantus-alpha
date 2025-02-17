from pydantic import BaseModel
from app.models.models_factors import UnitEnum, SortDirectionEnum, CategoryEnum
from typing import Optional


class FactorResponse(BaseModel):
    factor: str
    description: str
    unit: UnitEnum
    sort_direction: SortDirectionEnum
    category: CategoryEnum

    class Config:
        from_attributes = True


class FilterRequest(BaseModel):
    factor: str
    above: Optional[float] = None
    below: Optional[float] = None
