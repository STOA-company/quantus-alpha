from typing import Any, Dict, List

from pydantic import BaseModel


class AddInterestRequest(BaseModel):
    group_id: int
    ticker: str

    def to_dict(self) -> Dict[str, Any]:
        return {"group_id": self.group_id, "ticker": self.ticker}


class DeleteInterestRequest(BaseModel):
    group_id: int
    tickers: List[str]


class UpdateInterestRequest(BaseModel):
    group_ids: List[int]
    ticker: str

    def to_dict(self) -> Dict[str, Any]:
        return {"group_ids": self.group_ids, "ticker": self.ticker}


class UpdateInterestOrderRequest(BaseModel):
    group_id: int | None = None
    order: List[int] | List[str]


class MoveInterestRequest(BaseModel):
    from_group_id: int
    to_group_id: int
    tickers: List[str]
