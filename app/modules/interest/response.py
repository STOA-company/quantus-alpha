from pydantic import BaseModel
from typing import Literal, Optional, Dict, Any


class InterestItem(BaseModel):
    value: float
    unit: str
    sign: Optional[Literal["plus", "minus"]] = None


class InterestResponse(BaseModel):
    ticker: str
    name: str
    price: InterestItem
    change: InterestItem
    amount: InterestItem
    volume: InterestItem

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "InterestResponse":
        return cls(**data)
