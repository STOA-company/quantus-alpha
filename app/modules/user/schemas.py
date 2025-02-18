from pydantic import BaseModel
from typing import Optional


class UserInfoResponse(BaseModel):
    id: int
    email: str
    nickname: Optional[str] = None
    profile_image: Optional[str] = None
    image_format: Optional[str] = None


class RefreshTokenResponse(BaseModel):
    new_access_token: str


class UserProfileResponse(BaseModel):
    id: int
    nickname: Optional[str] = None
    profile_image: Optional[str] = None
    post_count: int
    comment_count: int
