from pydantic import BaseModel
from typing import Optional, List


class SignupRequest(BaseModel):
    email_token: str
    nickname: str
    favorite_stock: List[str] = []


class UserInfoResponse(BaseModel):
    id: int
    email: str
    nickname: Optional[str] = None


class RefreshTokenResponse(BaseModel):
    new_access_token: str
