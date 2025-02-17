from pydantic import BaseModel
from app.models.models_factors import UnitEnum, SortDirectionEnum, CategoryEnum


class FactorResponse(BaseModel):
    factor: str
    description: str
    unit: UnitEnum
    sort_direction: SortDirectionEnum
    category: CategoryEnum

    class Config:
        from_attributes = True
