from typing import Dict, Any
from pydantic import BaseModel


class AddInterestRequest(BaseModel):
    group_id: int
    ticker: str

    def to_dict(self) -> Dict[str, Any]:
        return {"group_id": self.group_id, "ticker": self.ticker}
