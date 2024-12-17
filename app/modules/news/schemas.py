from datetime import datetime
from pydantic import BaseModel

from app.modules.common.schemas import PaginationBaseResponse


class NewsResponse(PaginationBaseResponse):
    positive_count: int
    negative_count: int
    neutral_count: int


class NewsItem(BaseModel):
    date: datetime
    title: str
    summary: str | None
    emotion: str | None
