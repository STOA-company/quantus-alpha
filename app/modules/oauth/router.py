from fastapi import APIRouter, HTTPException, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from google.oauth2 import id_token
from google.auth.transport import requests
import httpx
from jose import JWTError
import os
from app.models.models_users import AlphafinderUser
import logging
from app.utils.oauth_utils import create_jwt_token, create_refresh_token, get_current_user, refresh_access_token
from app.modules.oauth.service import get_user_by_email, create_user, delete_user

logger = logging.getLogger(__name__)

router = APIRouter()

security = HTTPBearer()

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI")
GOOGLE_TOKEN_ENDPOINT = os.getenv("GOOGLE_TOKEN_ENDPOINT")
GOOGLE_AUTH_URL = os.getenv("GOOGLE_AUTH_URL")


@router.get("/google/login")
def google_login():
    """Google 로그인 페이지로 리다이렉트"""
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "response_type": "code",
        "scope": "email profile",
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "prompt": "select_account",
    }

    query_string = "&".join(f"{k}={v}" for k, v in params.items())
    logger.info(f"{GOOGLE_AUTH_URL}?{query_string}")
    return {"oauth_url": f"{GOOGLE_AUTH_URL}?{query_string}"}


@router.get("/google/callback")
def google_callback(code: str):
    """Google OAuth 콜백 처리"""
    try:
        logger.info(f"code: {code}")
        token_params = {
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": GOOGLE_REDIRECT_URI,
        }

        logger.info(f"token_params: {token_params}")
        with httpx.Client() as client:
            token_response = client.post(GOOGLE_TOKEN_ENDPOINT, data=token_params)
            token_data = token_response.json()
            logger.info(f"token_data: {token_data}")
            if "error" in token_data:
                raise HTTPException(status_code=400, detail=token_data["error"])

            google_user = id_token.verify_oauth2_token(
                token_data["id_token"],
                requests.Request(),
                GOOGLE_CLIENT_ID,
                clock_skew_in_seconds=10,  # 시간 오차 허용
            )

            email = google_user["email"]

            user = get_user_by_email(email)

            if not user:
                logger.info("user not found, create user")
                create_user(email)

            access_token = create_jwt_token(user.id)
            refresh_token = create_refresh_token(user.id)

            return {
                "message": "Login successful",
                "user": {
                    "id": user.id,
                    "email": user.email,
                },
                **access_token,
                "refresh_token": refresh_token,
            }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/refresh")
def refresh_token(
    credentials: HTTPAuthorizationCredentials = Security(security),
):
    """리프레시 토큰을 사용하여 새로운 액세스 토큰 발급"""
    try:
        new_access_token = refresh_access_token(credentials)
        return new_access_token

    except JWTError:
        raise HTTPException(
            status_code=401,
            detail="Invalid refresh token",
            headers={"WWW-Authenticate": "Bearer"},
        )


@router.get("/me")
def get_user_info(current_user: AlphafinderUser = Depends(get_current_user)):
    """현재 인증된 사용자 정보 반환"""
    return {
        "id": current_user.id,
        "email": current_user.email,
        "nickname": current_user.nickname,
    }


@router.get("/cancel")
def google_join_cancel(current_user: AlphafinderUser = Depends(get_current_user)):
    try:
        delete_user(current_user.id)
        return {"message": "User deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
