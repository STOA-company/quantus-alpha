from datetime import datetime
from pydantic import BaseModel

from app.modules.common.schemas import PaginationBaseResponse


class NewsResponse(PaginationBaseResponse):
    positive_count: int
    negative_count: int
    neutral_count: int
    ctry: str


class NewsItem(BaseModel):
    date: datetime
    title: str
    summary: str | None
    emotion: str | None
    name: str | None
    change_rate: float | None


class NewsRenewalItem(BaseModel):
    id: int
    date: datetime
    ctry: str | None
    ticker: str | None
    title: str
    summary: str | None
    emotion: str | None
    name: str | None
    change_rate: float | None
    price_impact: float | None


class DisclosureRenewalItem(BaseModel):
    id: int
    date: datetime
    ctry: str | None
    ticker: str | None
    title: str
    summary: str | None
    impact_reason: str | None
    key_points: str | None
    emotion: str | None
    name: str | None
    change_rate: float | None
    price_impact: float | None
    document_url: str | None


class NewsRenewalResponse(BaseModel):
    news: list[NewsRenewalItem]
    disclosure: list[DisclosureRenewalItem]


class LatestNewsResponse(BaseModel):
    date: datetime
    content: str
    type: str


class TopStoriesItem(BaseModel):
    price_impact: float
    date: datetime
    title: str
    summary: str | None
    emotion: str | None
    type: str


class TopStoriesResponse(BaseModel):
    name: str
    ticker: str
    logo_image: str
    ctry: str
    current_price: float
    change_rate: float
    items_count: int
    news: list[TopStoriesItem]
