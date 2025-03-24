from typing import Dict, Any, List
from pydantic import BaseModel


class AddInterestRequest(BaseModel):
    group_id: int
    ticker: str

    def to_dict(self) -> Dict[str, Any]:
        return {"group_id": self.group_id, "ticker": self.ticker}


class DeleteInterestRequest(BaseModel):
    group_id: int
    tickers: List[str]
