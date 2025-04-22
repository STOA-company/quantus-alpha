from pydantic import BaseModel

from app.modules.common.schemas import PaginationBaseResponse


class DisclosureResponse(PaginationBaseResponse):
    positive_count: int
    negative_count: int
    neutral_count: int


class DisclosureItem(BaseModel):
    title: str
    date: str
    emotion: str | None
    impact_reason: str | None
    key_points: str | None
    summary: str | None = None
    document_url: str
    name: str | None
    price_change: str | None
