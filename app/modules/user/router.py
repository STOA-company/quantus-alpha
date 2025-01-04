# import os
# from fastapi import APIRouter, logger, status, Request, Depends
# from fastapi.responses import JSONResponse, Response, RedirectResponse
# from app.database.crud import dml
# from app.database.schemas import Token, RefreshToken
# from app.utils.logger import set_logging
# from app.utils.oauth import delete_token, refresh_access_token, request_google_token
# from app.common.configs import BASE_URL, API_ENV
# from app.common.consts import CLIENTS
# from app.common.auth_configs import GOOGLE_CLIENT_INFO
# from app.common.auth_consts import CLIENT_URI
# from app.core.exception.custom import (
#     AuthException,
#     InvalidAuthCodeException,
#     TokenRefreshFailedException,
#     InvalidTokenException,
#     TokenExpiredException
# )

# router = APIRouter(
#     prefix="/oauth",
#     tags=["Auth"]
# )

# @router.get("/login/google")
# async def login_google():
#     REDIRECT_URI = os.path.join(BASE_URL, "oauth/callback/google")
#     SCOPE = 'openid email profile'
#     google_client_info = GOOGLE_CLIENT_INFO["web"]

#     authorization_url = (
#         "https://accounts.google.com/o/oauth2/v2/auth?"
#         f"response_type=code&"
#         f"client_id={google_client_info['client_id']}&"
#         f"redirect_uri={REDIRECT_URI}&"
#         f"scope={SCOPE}&"
#         f"access_type=offline&"
#         f"prompt=consent"
#     )
#     return RedirectResponse(url=authorization_url)

# @router.get("/callback/google")
# async def callback_google(request: Request):
#     code = request.query_params.get("code")
#     if not code:
#         raise InvalidAuthCodeException()

#     try:
#         access_token = await request_google_token(
#             code=code,
#             client_type="alphafinder"
#         )

#         client_uri = CLIENT_URI["alphafinder"][API_ENV]
#         redirect_uri = os.path.join(client_uri, f'oauth/callback?access_token={access_token}')

#         return RedirectResponse(redirect_uri)
#     except Exception as e:
#         logger.error(f"Google OAuth callback failed: {str(e)}")
#         raise AuthException(message="Google 로그인 처리 중 오류가 발생했습니다")

# @router.post("/refresh_token")
# async def refresh_token(token: Token):
#     if not token.token:
#         raise InvalidTokenException()

#     try:
#         res = await refresh_access_token(
#             access_token=token.token,
#             sns_type="google",
#             client_type="alphafinder",
#             is_app=False
#         )

#         status_code = res["status_code"]
#         if status_code != 200:
#             raise TokenRefreshFailedException()

#         token_data = res["token_data"]
#         return JSONResponse(
#             status_code=status_code,
#             content={"access_token": token_data.get("access_token")}
#         )
#     except TokenExpiredException:
#         raise
#     except Exception as e:
#         logger.error(f"Token refresh failed: {str(e)}")
#         raise TokenRefreshFailedException()

# @router.post("/logout")
# async def logout(token: Token):
#     if not token.token:
#         raise InvalidTokenException()

#     try:
#         delete_token(access_token=token.token)
#         return Response(status_code=status.HTTP_200_OK)
#     except Exception as e:
#         logger.error(f"Logout failed: {str(e)}")
#         raise AuthException(message="로그아웃 처리 중 오류가 발생했습니다")
