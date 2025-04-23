######################################## IMPORT ##########################################
import base64
import os
from typing import Dict, Optional
from urllib.parse import urljoin

import requests
from fastapi import HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

PRIVATE_PASSWORD = os.getenv("PRIVATE_PASSWORD")

######################################### LOGIC ###########################################
BASE_URL = "https://devbackfast.quantus.kr"

security = HTTPBearer()


# @retry(
#     stop=stop_after_attempt(2),
#     wait=wait_exponential(multiplier=1, min=3, max=20),
#     # before_sleep=lambda retry_state: asyncio.create_task(slack_sender("coin_error", f"retry count : {retry_state.attempt_number}")), # NOTE :: slack_sender 함수는 코루틴 함수가 아님 -> asyncio.create_task 사용 불가
#     reraise=True
# )
def validate_token(
    token,
    sns_type,
    client_type,
):
    ## headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Sns-Type": sns_type,
        "Client-Type": client_type,
    }
    ## httpx
    # async with httpx.AsyncClient() as client:
    #     response_data = await client.post(
    #         urljoin(BASE_URL, "user_info"),
    #         headers=headers,
    #         json={ #temporary legacy : latest code includes auth information headers only inside headers
    #             "token": token,
    #             "sns_type": sns_type,
    #             "client_type": client_type,
    #         },
    #         timeout=httpx.Timeout(
    #             connect=10.0,   # connection timeout
    #             read=30,        # read timeout
    #             write=10.0,     # write timeout
    #             pool=None       # pool timeout
    #         )
    #     )
    with requests.Session() as session:
        response_data = session.post(
            urljoin(BASE_URL, "user_info"),
            headers=headers,
            timeout=(5.0, 15),  # (connect_timeout, read_timeout)
        )

        # 응답 반환 (status_code와 JSON 응답 확인 필요할 수 있음)
        return response_data

    return response_data


# def get_private_key():
#     # 암호화된 개인 키 파일을 읽기
#     with open(PRIVATE_KEY_PATH, "rb") as key_file:
#         encrypted_private_key = key_file.read()

#     # 암호를 사용하여 개인 키 로딩
#     private_key = serialization.load_pem_private_key(
#         encrypted_private_key,
#         password=PRIVATE_PASSWORD.encode('utf-8'),
#         backend=default_backend()
#     )
#     return private_key

# def get_public_key():
#     with open(PUBLIC_KEY_PATH, "rb") as f:
#         public_key = serialization.load_pem_public_key(
#             f.read(),
#             backend=default_backend()
#         )
#     return public_key

# 암호화 로직
# def encrypt(plaintext):
#     public_key = get_public_key()
#     ciphertext = public_key.encrypt(
#         plaintext.encode(),
#         padding.OAEP(
#             mgf=padding.MGF1(algorithm=hashes.SHA256()),
#             algorithm=hashes.SHA256(),
#             label=None
#         )
#     )
#     return ciphertext

# 복호화 로직
# private_key = get_private_key()
# def decrypt(ciphertext):
#     plaintext = private_key.decrypt(
#         ciphertext,
#         padding.OAEP(
#             mgf=padding.MGF1(algorithm=hashes.SHA256()),
#             algorithm=hashes.SHA256(),
#             label=None
#         )
#     )
#     return plaintext.decode()


def decoder_base64(encoded_str):
    # 디코딩
    decoded_bytes = base64.b64decode(encoded_str)
    return decoded_bytes

    # # 바이트를 문자열로 변환 (옵션)
    # return decoded_bytes.decode('utf-8')


# def decrypts(data: dict):
#     data_decrypted = {}
#     for key, val in data.items():
#         decrypted = decrypt(ciphertext=val)
#         data_decrypted[key] = decrypted

#     return data_decrypted

# def encrypts(data: dict):
#     data_encrypted = {}
#     for key, val in data.items():
#         encrypted = encrypt(plaintext=val)
#         data_encrypted[key] = encrypted

#     return data_encrypted


def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security),
) -> Optional[Dict]:
    """Get current user from token validation"""
    if not credentials:
        return None

    try:
        token = credentials.credentials
        sns_type = request.headers.get("Sns-Type")
        client_type = request.headers.get("Client-Type")

        # exception for validation
        exempt_paths = [
            "/open",
            "/docs",
            "/openapi.json",
            "/coin/backtest/get_supported_ticker",
            "/coin/backtest/get_supported_indicator",
            "/coin/backtest/get_supported_bar",
            "/coin/backtest/check_task_status",
            "/coin/strategy/get_rec_strategy",
            "/coin/admin/coin_trader_reassignment",
        ]
        if request.url.path in exempt_paths:
            return None

        # token chk
        res = validate_token(token=token, sns_type=sns_type, client_type=client_type)
        status_code = res.status_code
        if status_code != 200:
            raise HTTPException(status_code=status_code)

        return res.json()["userInfo"]

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
