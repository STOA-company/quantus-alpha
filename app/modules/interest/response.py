from pydantic import BaseModel
from typing import Literal, Optional, List


class InterestItem(BaseModel):
    value: float
    unit: str
    sign: Optional[Literal["plus", "minus"]] = None


class InterestTable(BaseModel):
    ticker: str
    name: str
    price: InterestItem
    change: InterestItem
    amount: InterestItem
    volume: InterestItem

    @classmethod
    def from_dict(cls, data: dict):
        # name이 항상 문자열인지 확인
        if data.get("name") is None:
            data["name"] = ""
        return cls(**data)


class InterestResponse(BaseModel):
    has_next: bool
    data: List[InterestTable]
