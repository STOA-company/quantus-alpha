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
    content: str
    summary: str
    emotion: str | None
    image_url: str | None
    
