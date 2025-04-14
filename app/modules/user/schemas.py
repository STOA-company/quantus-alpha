from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class UserInfoResponse(BaseModel):
    id: int
    email: str
    nickname: Optional[str] = None
    profile_image: Optional[str] = None
    image_format: Optional[str] = None
    is_subscribed: bool = False
    subscription_end: Optional[datetime] = None
    level: Optional[str] = None
    period_days: Optional[int] = None
    product_type: Optional[str] = None


class RefreshTokenResponse(BaseModel):
    new_access_token: str


class UserProfileResponse(BaseModel):
    id: int
    nickname: Optional[str] = None
    profile_image: Optional[str] = None
    post_count: int
    comment_count: int


class DataDownloadHistory(BaseModel):
    user_id: int
    data_type: str
    data_detail: str
    download_datetime: datetime
