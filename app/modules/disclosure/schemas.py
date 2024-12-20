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
    key_points_1: str | None
    key_points_2: str | None
    key_points_3: str | None
    key_points_4: str | None
    key_points_5: str | None
    summary: str | None = None
    document_url: str
    name: str | None
    price_change: str | None
