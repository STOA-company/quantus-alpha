from typing import List, Literal, Optional

from pydantic import BaseModel


class InterestItem(BaseModel):
    value: float
    unit: str
    sign: Optional[Literal["plus", "minus"]] = None


class InterestTable(BaseModel):
    ticker: str
    country: str
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


class InterestStock(BaseModel):
    ticker: str
    name: str | None = None
    ctry: str | None = None


class InterestGroupResponse(BaseModel):
    id: int
    name: str
    stocks: List[InterestStock]
    order: int
    is_editable: bool


class InterestPriceResponse(BaseModel):
    ctry: str
    name: str
    ticker: str
    current_price: float
    change_rt: float
