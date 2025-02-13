from fastapi import APIRouter, Depends, File, UploadFile, Form
from app.models.models_users import AlphafinderUser
from app.utils.oauth_utils import get_current_user
from app.modules.user.service import delete_user
from app.modules.user.schemas import UserInfoResponse, RefreshTokenResponse
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose.exceptions import JWTError
from app.utils.oauth_utils import refresh_access_token, decode_email_token, create_jwt_token, create_refresh_token
from app.modules.user.service import create_user, add_favorite_stock
import json
from typing import Optional

router = APIRouter()

security = HTTPBearer()


@router.post("/signup")
async def signup(
    email_token: str = Form(...),
    provider: str = Form(default="google"),
    nickname: str = Form(...),
    favorite_stocks: Optional[str] = Form(None),
    profile_image: UploadFile = File(...),
):
    email = decode_email_token(email_token)["sub"]
    user = create_user(email, nickname, provider)  # profile image 활성화 필요
    favorite_stock_list = json.loads(favorite_stocks)
    if favorite_stock_list:
        for ticker in favorite_stock_list:
            add_favorite_stock(user.id, ticker)
    access_token = create_jwt_token(user.id)
    refresh_token = create_refresh_token(user.id)

    return {"message": "Signup successful", "access_token": access_token, "refresh_token": refresh_token}


@router.get("/me", response_model=UserInfoResponse)
def get_user_info(current_user: AlphafinderUser = Depends(get_current_user)):
    """현재 인증된 사용자 정보 반환"""
    return UserInfoResponse(id=current_user.id, email=current_user.email, nickname=current_user.nickname)


@router.post("/refresh", response_model=RefreshTokenResponse)
def refresh_token(
    credentials: HTTPAuthorizationCredentials = Security(security),
):
    """리프레시 토큰을 사용하여 새로운 액세스 토큰 발급"""
    try:
        new_access_token = refresh_access_token(credentials)
        return RefreshTokenResponse(new_access_token=new_access_token)

    except JWTError:
        raise HTTPException(
            status_code=401,
            detail="Invalid refresh token",
            headers={"WWW-Authenticate": "Bearer"},
        )


@router.get("/cancel")
def signup_cancel(current_user: AlphafinderUser = Depends(get_current_user)):
    try:
        delete_user(current_user.id)
        return {"message": "User deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
