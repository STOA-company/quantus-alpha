from fastapi import APIRouter, HTTPException
from google.oauth2 import id_token
from google.auth.transport import requests
import httpx
import os
import logging
from app.utils.oauth_utils import create_email_token, create_jwt_token, create_refresh_token, store_token
from app.modules.oauth.schemas import (
    GoogleLoginResponse,
)
from app.modules.user.service import get_user_by_email
from fastapi.responses import RedirectResponse
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

router = APIRouter()

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI")
GOOGLE_TOKEN_ENDPOINT = os.getenv("GOOGLE_TOKEN_ENDPOINT")
GOOGLE_AUTH_URL = os.getenv("GOOGLE_AUTH_URL")
TEST_GOOGLE_REDIRECT_URI = os.getenv("TEST_GOOGLE_REDIRECT_URI")


@router.get("/google/login", response_model=GoogleLoginResponse)
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
    return GoogleLoginResponse(oauth_url=f"{GOOGLE_AUTH_URL}?{query_string}")


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
                email_token = create_email_token(email)

                params = {
                    "is_login": False,
                    "email_token": email_token,
                }
            else:
                access_token = create_jwt_token(user.id)
                refresh_token = create_refresh_token(user.id)
                access_token_hash = store_token(access_token, refresh_token)

                params = {
                    "is_login": True,
                    "access_token_hash": access_token_hash,
                }

            FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

            redirect_url = f"{FRONTEND_URL}/oauth/callback?{urlencode(params)}"
            logger.info(f"redirect_url: {redirect_url}")
            return RedirectResponse(url=redirect_url)

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/google/login/test", response_model=GoogleLoginResponse)
def google_login_test():
    """Google 로그인 페이지로 리다이렉트"""
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "response_type": "code",
        "scope": "email profile",
        "redirect_uri": TEST_GOOGLE_REDIRECT_URI,
        "prompt": "select_account",
    }

    query_string = "&".join(f"{k}={v}" for k, v in params.items())
    logger.info(f"{GOOGLE_AUTH_URL}?{query_string}")
    return GoogleLoginResponse(oauth_url=f"{GOOGLE_AUTH_URL}?{query_string}")


@router.get("/google/callback/test")
def google_callback_test(code: str):
    """Google OAuth 콜백 처리"""
    try:
        logger.info(f"code: {code}")
        token_params = {
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": TEST_GOOGLE_REDIRECT_URI,
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
                email_token = create_email_token(email)

                params = {
                    "is_login": False,
                    "email_token": email_token,
                }
            else:
                access_token = create_jwt_token(user.id)
                refresh_token = create_refresh_token(user.id)
                access_token_hash = store_token(access_token, refresh_token)

                params = {
                    "is_login": True,
                    "access_token_hash": access_token_hash,
                }

            TEST_FRONTEND_URL = os.getenv("TEST_FRONTEND_URL", "http://localhost:3000")

            redirect_url = f"{TEST_FRONTEND_URL}/oauth/callback?{urlencode(params)}"
            logger.info(f"redirect_url: {redirect_url}")
            return RedirectResponse(url=redirect_url)

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
