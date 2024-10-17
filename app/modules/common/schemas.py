from pydantic import BaseModel, Field
from typing import List, Dict, Any, Generic, TypeVar

T = TypeVar('T')

class ResponseSchema(BaseModel):
    status: str
    message: str

class PaginationSchema(BaseModel):
    total: int
    page: int
    size: int

class ListResponseSchema(ResponseSchema, Generic[T]):
    data: List[T]
    pagination: PaginationSchema