# import os
# import json
# import httpx
# from httpx import Response
# from app.database.crud import dml
# from app.utils.exceptions import get_exceptions
# from app.utils.date_utils import now_utc
# from app.utils.utils import generate_sha256_hash
# from app.utils.logger import set_logging
# from app.common.configs import BASE_URL
# from app.common.consts import CLIENTS
# from app.common.auth_configs import (
#     # KAKAO_CLIENT_INFO,
#     GOOGLE_CLIENT_INFO
# )
# from tenacity import retry, wait_fixed, stop_after_attempt

# # @retry(wait=wait_fixed(1.5), stop=stop_after_attempt(3), reraise=True)
# # async def request_kakao_token(
# #     code: str,
# #     client_type: CLIENTS,
# #     **kwargs
# # ):
# #     is_local, is_app \
# #         = kwargs.get("is_local", False), kwargs.get("is_app", False)
# #     CLIENT_INFO = KAKAO_CLIENT_INFO[client_type]
# #     REDIRECT_URI = os.path.join(BASE_URL, f"oauth/callback/kakao/local") if is_local else os.path.join(BASE_URL, f"oauth/callback/kakao/{client_type}")

# #     ## httpx
# #     async with httpx.AsyncClient() as client:
# #         response_data = await client.post(
# #             "https://kauth.kakao.com/oauth/token",
# #             data={
# #                 "grant_type": "authorization_code",
# #                 "client_id": CLIENT_INFO['client_id'],
# #                 "code": code,
# #                 "client_secret": CLIENT_INFO['client_secret'],
# #                 "redirect_uri": REDIRECT_URI,
# #             },
# #         )
# #     status_code = response_data.status_code
# #     token_data: dict = response_data.json()
# #     set_logging(action="request_kakao_token", log_data=token_data)
# #     if status_code == 200:
# #         insert_token(token_data, is_app=is_app)
# #     else:
# #         token_data.update(
# #             dict(
# #                 msg="Failed callback kakao access token",
# #                 status_code=status_code,
# #                 code=code,
# #                 client_type=client_type,
# #                 is_app=is_app
# #             )
# #         )
# #         e = Exception(token_data)
# #         get_exceptions(e, action=f"kakao_callback_{client_type}", dml=dml, slack_method="user")

# #     return token_data.get("access_token")

# @retry(wait=wait_fixed(1.5), stop=stop_after_attempt(3), reraise=True)
# async def request_google_token(
#     code: str,
#     client_type: CLIENTS,
#     **kwargs
# ):
#     is_local, is_app \
#         = kwargs.get("is_local", False), kwargs.get("is_app", False)
#     REDIRECT_URI = os.path.join(BASE_URL, f"oauth/callback/google/local") if is_local else os.path.join(BASE_URL, f"oauth/callback/google/{client_type}")
#     google_client_info = GOOGLE_CLIENT_INFO["app"][client_type] if is_app else GOOGLE_CLIENT_INFO["web"]
#     async with httpx.AsyncClient() as client:
#         response_data = await client.post(
#             "https://oauth2.googleapis.com/token",
#             data={
#                 "client_id": google_client_info["client_id"],
#                 "client_secret": google_client_info["client_secret"],
#                 "code": code,
#                 "grant_type": "authorization_code",
#                 "redirect_uri": REDIRECT_URI,
#             },
#         )
#     status_code = response_data.status_code
#     token_data: dict = response_data.json()
#     set_logging(action="request_google_token", log_data=token_data)
#     if status_code == 200:
#         insert_token(token_data, is_app=is_app)
#     else:
#         token_data.update(
#             dict(
#                 msg="Failed callback google access token",
#                 status_code=status_code,
#                 code=code,
#                 client_type=client_type,
#                 is_app=is_app,
#                 client_id=google_client_info["client_id"],
#                 client_secret=google_client_info["client_secret"],
#                 REDIRECT_URI=REDIRECT_URI,
#             )
#         )
#         e = Exception(token_data)
#         # get_exceptions(e, action=f"google_callback_{client_type}", dml=dml, slack_method="user")

#     return token_data.get("access_token")

# def insert_token(token_data: dict, is_app: bool = False):
#     try:
#         access_token, refresh_token \
#             = token_data["access_token"], token_data["refresh_token"]
#         access_token_hash = generate_sha256_hash(access_token)
#         data = dict(
#             access_token_hash=access_token_hash,
#             access_token=access_token,
#             refresh_token=refresh_token,
#             token_data=json.dumps(token_data),
#             is_app=is_app
#         )
#         dml._insert(
#             table="quantus_oauth_token",
#             sets=data
#         )
#         set_logging(action="insert_token", log_data=data)
#     except Exception as e:
#         # get_exceptions(e, action="insert_token", slack_method="user", **token_data)
#         raise e

# def delete_token(access_token: str):
#     access_token_hash = generate_sha256_hash(access_token)
#     dml._delete(
#         table="quantus_oauth_token",
#         **dict(access_token_hash=access_token_hash)
#     )
#     log_data = dict(access_token=access_token, access_token_hash=access_token_hash)
#     set_logging(action="delete_token", log_data=log_data)

# async def refresh_access_token(
#     access_token: str,
#     sns_type: str,
#     client_type: CLIENTS,
#     is_app: bool
# ):
#     access_token_hash = generate_sha256_hash(access_token)
#     log_data = dict(
#         access_token=access_token,
#         access_token_hash=access_token_hash,
#         sns_type=sns_type,
#         client_type=client_type,
#         is_app=is_app
#     )
#     res = dml._select(
#         table="quantus_oauth_token",
#         columns=["refresh_token", "is_app", "updated_at"],
#         **dict(access_token_hash=access_token_hash)
#     )
#     if len(res) == 0:
#         msg="Empty access token"
#         status_code = 403
#         token_data = {}
#     else:
#         r = res[0]
#         refresh_token = r["refresh_token"]
#         log_data.update(dict(r))

#         # if sns_type == "kakao":
#         #     res_data: Response = await refresh_access_token_kakao(
#         #         refresh_token=refresh_token,
#         #         client_type=client_type
#         #     )
#         if sns_type == "google":
#             res_data: Response = await refresh_access_token_google(
#                 refresh_token=refresh_token,
#                 client_type=client_type,
#                 is_app=is_app
#             )

#         status_code, token_data \
#             = res_data.status_code, res_data.json()
#         if status_code == 200:
#             # Success
#             access_token_updated = token_data["access_token"]
#             access_token_updated_hash = generate_sha256_hash(access_token_updated)
#             dml._update(
#                 table="quantus_oauth_token",
#                 sets=dict(
#                     access_token_hash=access_token_updated_hash,
#                     access_token=access_token_updated,
#                     updated_at=now_utc(),
#                 ),
#                 **dict(access_token_hash=access_token_hash)
#             )
#             msg = "Success refresh access token"
#         else:
#             # Failed
#             # 세션만료 처리: delete token
#             status_code = 403
#             msg = "Failed refresh access token"
#             delete_token(access_token)

#     # update log data
#     log_data.update(dict(token_data=json.dumps(token_data), msg=msg))
#     # set logging
#     set_logging(action="refresh_access_token", log_data=log_data)

#     return dict(
#         status_code=status_code,
#         token_data=token_data
#     )

# # async def refresh_access_token_kakao(
# #     refresh_token: str,
# #     client_type: CLIENTS
# # ):
# #     CLIENT_INFO = KAKAO_CLIENT_INFO[client_type]
# #     async with httpx.AsyncClient() as client:
# #         response_data = await client.post(
# #             "https://kauth.kakao.com/oauth/token",
# #             data={
# #                 "client_id": CLIENT_INFO["client_id"],
# #                 "client_secret": CLIENT_INFO["client_secret"],
# #                 "refresh_token": refresh_token,
# #                 "grant_type": "refresh_token",
# #             },
# #         )
# #     return response_data

# async def refresh_access_token_google(
#     refresh_token: str,
#     client_type: CLIENTS,
#     is_app: bool = False,
# ):
#     google_client_info = GOOGLE_CLIENT_INFO["app"][client_type] if is_app else GOOGLE_CLIENT_INFO["web"]
#     async with httpx.AsyncClient() as client:
#         response_data = await client.post(
#             "https://oauth2.googleapis.com/token",
#             data={
#                 "client_id": google_client_info["client_id"],
#                 "client_secret": google_client_info["client_secret"],
#                 "refresh_token": refresh_token,
#                 "grant_type": "refresh_token",
#             },
#         )
#     return response_data
