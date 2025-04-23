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
    impact_reason: str | None
    key_points: str | None
    emotion: str | None
    name: str | None


class NewsDetailItem(BaseModel):
    id: int
    ctry: str | None
    date: datetime
    title: str
    summary1: str | None
    summary2: str | None
    emotion: str | None
    price_impact: float | None


class NewsDetailItemV2(BaseModel):
    id: int
    ctry: str | None
    date: datetime
    title: str
    summary: str | None
    impact_reason: str | None
    key_points: str | None
    emotion: str | None
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


class InterestNewsResponse(BaseModel):
    news: list[NewsRenewalItem]
    has_next: bool


class InterestDisclosureResponse(BaseModel):
    disclosure: list[DisclosureRenewalItem]
    has_next: bool


class LatestNewsResponse(BaseModel):
    date: datetime
    content: str
    type: str


class TopStoriesItem(BaseModel):
    id: int
    price_impact: float
    date: datetime
    title: str
    summary: str | None
    emotion: str | None
    type: str
    is_viewed: bool


class TopStoriesResponse(BaseModel):
    name: str
    ticker: str
    ctry: str
    current_price: float
    change_rate: float
    items_count: int
    is_viewed: bool
    news: list[TopStoriesItem]
