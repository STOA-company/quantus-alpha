from pydantic import BaseModel
from typing import Optional


class UserInfoResponse(BaseModel):
    id: int
    email: str
    nickname: Optional[str] = None


class RefreshTokenResponse(BaseModel):
    new_access_token: str
