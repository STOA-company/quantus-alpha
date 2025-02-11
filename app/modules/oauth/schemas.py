from pydantic import BaseModel


class GoogleLoginResponse(BaseModel):
    oauth_url: str


class GoogleCallbackResponse(BaseModel):
    message: str
    user: dict
    access_token: str
    token_type: str
    refresh_token: str


class CancelResponse(BaseModel):
    message: str
