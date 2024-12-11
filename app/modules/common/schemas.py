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
    total: int
    page: int
    size: int