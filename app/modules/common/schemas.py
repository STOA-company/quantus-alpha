from pydantic import BaseModel
from typing import List, Generic, Optional, TypeVar

T = TypeVar("T")


class ResponseSchema(BaseModel):
    status: str
    message: str


class PaginationSchema(BaseModel):
    total: int
    page: int
    size: int


class ListResponseSchema(ResponseSchema, Generic[T]):
    data: List[T]


class BaseResponse(BaseModel, Generic[T]):
    status: str
    message: str
    data: Optional[T] = None


class PandasStatistics(BaseModel, Generic[T]):
    status: str
    message: str
    data: Optional[T] = None
    statistics: Optional[dict] = None
