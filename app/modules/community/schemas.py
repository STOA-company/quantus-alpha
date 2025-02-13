from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field

### 게시글 스키마 ###


class PostCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    content: str = Field(..., min_length=1)
    category_id: int
    image_url: Optional[str] = None
    stock_tickers: List[str] = Field(default=[], max_items=3)

    class Config:
        json_schema_extra = {
            "example": {
                "title": "삼성전자 실적 분석",
                "content": "2024년 1분기 실적 분석입니다...",
                "category_id": 1,
                "image_url": "https://example.com/image.jpg",
                "stock_tickers": ["A005930", "A035720"],
            }
        }


class UserInfo(BaseModel):
    id: int
    nickname: str
    profile_image: Optional[str] = None


class ResponsePost(BaseModel):
    id: int
    title: str
    content: str
    category_name: str
    image_url: Optional[str] = None
    like_count: int
    comment_count: int
    is_changed: bool
    is_bookmarked: bool
    created_at: datetime
    stock_tickers: List[str]
    user_info: UserInfo


class PostListResponse(BaseModel):
    status_code: int
    message: str
    has_more: bool
    data: List[ResponsePost]


class PostUpdate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    content: str = Field(..., min_length=1)
    category_id: int
    image_url: Optional[str] = None
    stock_tickers: List[str] = Field(default=[], max_items=3)

    class Config:
        json_schema_extra = {
            "example": {
                "title": "삼성전자 실적 분석 수정",
                "content": "2024년 1분기 실적 분석 수정입니다...",
                "category_id": 1,
                "image_url": "https://example.com/image.jpg",
                "stock_tickers": ["A005930", "A035720"],
            }
        }


### 댓글 스키마 ###


class CommentCreate(BaseModel):
    content: str = Field(..., min_length=1)
    parent_id: Optional[int] = None

    class Config:
        json_schema_extra = {
            "example": {
                "content": "댓글 내용입니다.",
                "parent_id": None,  # 대댓글인 경우 부모 댓글 ID
            }
        }


class CommentItem(BaseModel):
    id: int
    content: str
    like_count: int
    depth: int
    parent_id: Optional[int] = None
    created_at: datetime
    is_changed: bool
    is_liked: bool
    user_info: UserInfo
    sub_comments: List["CommentItem"] = Field(default=list)

    class Config:
        from_attributes = True


class CommentListResponse(BaseModel):
    status_code: int
    message: str
    has_more: bool
    data: List[CommentItem]


class CommentUpdate(BaseModel):
    content: str = Field(..., min_length=1)

    class Config:
        json_schema_extra = {"example": {"content": "댓글 내용 수정입니다."}}
