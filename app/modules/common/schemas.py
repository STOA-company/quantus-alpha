from pydantic import BaseModel
from typing import List, Generic, Optional, TypeVar

T = TypeVar("T")


class ResponseSchema(BaseModel):
    status_code: int
    message: str


class ListResponseSchema(ResponseSchema, Generic[T]):
    data: List[T]


class BaseResponse(BaseModel, Generic[T]):
    status_code: int
    message: str
    data: Optional[T] = None


class PandasStatistics(BaseModel, Generic[T]):
    status_code: int
    message: str
    data: Optional[T] = None
    statistics: Optional[dict] = None


class PaginationBaseResponse(BaseResponse):
    total_count: int
    total_pages: int
    current_page: int
    offset: int
    size: int


class InfiniteScrollResponse(BaseModel, Generic[T]):
    status_code: int
    message: str
    has_more: bool
    data: List[T]
