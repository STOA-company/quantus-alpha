from typing import List, Optional

from pydantic import BaseModel


class SearchItem(BaseModel):
    ticker: str
    name: Optional[str] = None
    language: Optional[str] = None
    current_price: Optional[float] = None
    current_price_rate: Optional[float] = None


class CommunitySearchItem(BaseModel):
    ticker: str
    name: str


class SearchResponse(BaseModel):
    status_code: int
    message: str
    has_more: bool
    data: List[SearchItem]
