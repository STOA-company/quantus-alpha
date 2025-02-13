from pydantic import BaseModel


class GoogleLoginResponse(BaseModel):
    oauth_url: str


class CancelResponse(BaseModel):
    message: str
