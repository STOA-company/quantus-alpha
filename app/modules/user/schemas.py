from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class UserInfoResponse(BaseModel):
    id: int
    email: str
    nickname: Optional[str] = None
    profile_image: Optional[str] = None
    image_format: Optional[str] = None
    is_subscribed: bool = False
    subscription_end: Optional[datetime] = None


class RefreshTokenResponse(BaseModel):
    new_access_token: str


class UserProfileResponse(BaseModel):
    id: int
    nickname: Optional[str] = None
    profile_image: Optional[str] = None
    post_count: int
    comment_count: int
