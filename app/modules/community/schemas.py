from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

### 게시글 스키마 ###


class PostCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    content: str = Field(..., min_length=1)
    category_id: int
    image_url: Optional[str] = None
    image_format: Optional[str] = None
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
    image_format: Optional[str] = None


class StockInfo(BaseModel):
    ticker: str
    name: Optional[str] = None
    ctry: Optional[str] = None


class ResponsePost(BaseModel):
    id: int
    title: str
    content: str
    category_name: str
    image_url: Optional[str] = None
    image_format: Optional[str] = None
    like_count: int
    comment_count: int
    is_changed: bool
    is_bookmarked: bool
    is_liked: bool
    created_at: datetime
    stock_tickers: List[StockInfo]
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
    image_format: Optional[str] = None
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


class PostInfo(BaseModel):
    id: int
    title: str


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
    is_mine: bool
    user_info: UserInfo
    sub_comments: List["CommentItem"] = Field(default=list)

    class Config:
        from_attributes = True


class CommentItemWithPostInfo(BaseModel):
    id: int
    content: str
    like_count: int
    depth: int
    parent_id: Optional[int] = None
    created_at: datetime
    is_changed: bool
    is_liked: bool
    is_mine: bool
    user_info: UserInfo
    sub_comments: List["CommentItem"] = Field(default=list)
    post_info: PostInfo

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


##### 좋아요 스키마 #####


class LikeRequest(BaseModel):
    is_liked: bool  # 좋아요 추가(True) 또는 제거(False)


class LikeResponse(BaseModel):
    is_liked: bool  # 현재 좋아요 상태
    like_count: int


### 북마크 스키마


class BookmarkItem(BaseModel):
    is_bookmarked: bool  # 북마크 추가(True) 또는 제거(False)


class TrendingPostResponse(BaseModel):
    id: int
    rank: int
    title: str
    created_at: datetime
    user_info: UserInfo


class TrendingStockResponse(BaseModel):
    rank: int
    ticker: str
    name: str
    ctry: str


### 카테고리 스키마 ###


class CategoryResponse(BaseModel):
    id: int
    name: str


class PresignedUrlRequest(BaseModel):
    """Presigned URL 요청 스키마"""

    content_type: str = Field(..., description="파일의 Content-Type (예: image/jpeg, image/png, image/gif)")
    file_size: int = Field(..., description="파일 크기 (바이트)", ge=0, le=5 * 1024 * 1024)  # 최대 5MB

    class Config:
        json_schema_extra = {
            "example": {
                "content_type": "image/jpeg",
                "file_size": 1024000,  # 1MB
            }
        }


class PresignedUrlResponse(BaseModel):
    """Presigned URL 응답 스키마"""

    upload_url: str = Field(..., description="S3 업로드용 presigned URL")
    image_key: str = Field(..., description="S3에 저장될 이미지 키")
    expires_in: int = Field(..., description="URL 유효기간 (초)")
