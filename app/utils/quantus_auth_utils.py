######################################## IMPORT ##########################################
import base64
import os
import httpx
import json
from datetime import datetime, timedelta
from typing import Dict, Optional
from urllib.parse import urljoin

import requests
from fastapi import HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from opentelemetry import trace
# from opentelemetry.instrumentation.requests import RequestsInstrumentor

from app.core.redis import redis_client
from app.models.models_users import AlphafinderUser

PRIVATE_PASSWORD = os.getenv("PRIVATE_PASSWORD")

######################################### LOGIC ###########################################
BASE_URL = os.getenv("QUANTUS_BASE_URL")

security = HTTPBearer(auto_error=False)

TOKEN_CACHE_PREFIX = "auth_token:"
TOKEN_CACHE_TTL = 30  # 1시간 (초 단위)

async def validate_token_async(token, sns_type, client_type):
    try:
        # None 값 체크
        if not all([token, sns_type, client_type]):
            return None

        # tracer = trace.get_tracer(__name__)
        # with tracer.start_as_current_span("validate_token_external_async") as span:
        #     span.set_attribute("http.url", urljoin(BASE_URL, "user_info"))
        #     span.set_attribute("http.method", "POST")
        #     span.set_attribute("http.target", "/user_info")
        #     span.set_attribute("service.name", "quantus-alpha")
        #     span.set_attribute("operation", "token_validation")

        headers = {
            "Authorization": f"Bearer {token}",
            "Sns-Type": sns_type,
            "Client-Type": client_type,
        }


        async with httpx.AsyncClient(
            limits=httpx.Limits(
                max_keepalive_connections=30,      # keep-alive 연결 수만 설정
                keepalive_expiry=45.0             # 연결 유지 시간
            )
        ) as client:
            response_data = await client.post(
                urljoin(BASE_URL, "user_info"),
                headers=headers,
                timeout=30.0 
                # timeout=httpx.Timeout(
                #     connect=5.0,    # 연결 타임아웃
                #     read=15.0,      # 읽기 타임아웃
                #     write=5.0,      # 쓰기 타임아웃
                #     pool=30.0       # 풀 타임아웃
                # )
            )
                
                # span.set_attribute("http.status_code", response_data.status_code)
            return response_data
                
    except Exception as e:
        print(f"비동기 함수 상세 오류: {type(e).__name__}: {e}")  # 더 자세한 오류
        import traceback
        traceback.print_exc()  # 스택 트레이스 출력
        return None

def validate_token(
    token,
    sns_type,
    client_type,
):
    tracer = trace.get_tracer(__name__)
    
    with tracer.start_as_current_span("validate_token_external") as span:
        # span에 메타데이터 추가
        span.set_attribute("http.url", urljoin(BASE_URL, "user_info"))
        span.set_attribute("http.method", "POST")
        span.set_attribute("http.target", "/user_info")
        span.set_attribute("service.name", "quantus-alpha")
        span.set_attribute("operation", "token_validation")
        
        ## headers
        headers = {
            "Authorization": f"Bearer {token}",
            "Sns-Type": sns_type,
            "Client-Type": client_type,
        }
        
        with requests.Session() as session:
            response_data = session.post(
                urljoin(BASE_URL, "user_info"),
                headers=headers,
                timeout=(5.0, 15),
            )
            
            # 응답 정보도 span에 추가
            span.set_attribute("http.status_code", response_data.status_code)
            
            return response_data

# @retry(
#     stop=stop_after_attempt(2),
#     wait=wait_exponential(multiplier=1, min=3, max=20),
#     # before_sleep=lambda retry_state: asyncio.create_task(slack_sender("coin_error", f"retry count : {retry_state.attempt_number}")), # NOTE :: slack_sender 함수는 코루틴 함수가 아님 -> asyncio.create_task 사용 불가
#     reraise=True
# )


# def validate_token(
#     token,
#     sns_type,
#     client_type,
# ):
#     ## headers
#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Sns-Type": sns_type,
#         "Client-Type": client_type,
#     }
#     ## httpx
#     # async with httpx.AsyncClient() as client:
#     #     response_data = await client.post(
#     #         urljoin(BASE_URL, "user_info"),
#     #         headers=headers,
#     #         json={ #temporary legacy : latest code includes auth information headers only inside headers
#     #             "token": token,
#     #             "sns_type": sns_type,
#     #             "client_type": client_type,
#     #         },
#     #         timeout=httpx.Timeout(
#     #             connect=10.0,   # connection timeout
#     #             read=30,        # read timeout
#     #             write=10.0,     # write timeout
#     #             pool=None       # pool timeout
#     #         )
#     #     )
#     with requests.Session() as session:
#         response_data = session.post(
#             urljoin(BASE_URL, "user_info"),
#             headers=headers,
#             timeout=(5.0, 15),  # (connect_timeout, read_timeout)
#         )

#         # 응답 반환 (status_code와 JSON 응답 확인 필요할 수 있음)
#         return response_data

#     return response_data


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
        if token is None:
            return None

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

async def get_current_user_async(
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
        if token is None:
            return None

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
        # res = validate_token(token=token, sns_type=sns_type, client_type=client_type)
        res = await validate_token_async(token=token, sns_type=sns_type, client_type=client_type)
        status_code = res.status_code
        if status_code != 200:
            raise HTTPException(status_code=status_code)
        return res.json()["userInfo"]

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def get_current_user_redis(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security),
) -> Optional[Dict]:
    """Redis를 사용한 토큰 검증으로 현재 사용자 정보 조회"""
    if not credentials:
        return None

    try:
        token = credentials.credentials
        sns_type = request.headers.get("Sns-Type")
        client_type = request.headers.get("Client-Type")
        
        if token is None:
            return None

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

        # 1. Redis에서 캐시된 토큰 검증 결과 확인
        cached_user_info = _get_cached_token_validation(token, sns_type, client_type)
        if cached_user_info:
            return cached_user_info

        # 2. 캐시에 없으면 외부 API로 검증
        print(f"캐시 미스, 외부 API 호출: {sns_type}:{client_type}")
        res = await validate_token_async(token=token, sns_type=sns_type, client_type=client_type)
        
        if res is None:
            raise HTTPException(status_code=500, detail="Token validation failed")
            
        status_code = res.status_code
        if status_code != 200:
            raise HTTPException(status_code=status_code)

        user_info = res.json()["userInfo"]
        
        # 3. 검증 성공 시 Redis에 캐시
        _cache_token_validation(token, sns_type, client_type, user_info)
        
        return user_info

    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Redis 토큰 검증 오류: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

def _get_redis_key(token: str, sns_type: str, client_type: str) -> str:
    """Redis 키 생성"""
    return f"{TOKEN_CACHE_PREFIX}{sns_type}:{client_type}:{token}"

def _cache_token_validation(token: str, sns_type: str, client_type: str, user_info: Dict, ttl: int = None):
    """토큰 검증 결과를 Redis에 캐시"""
    try:
        redis_conn = redis_client()
        cache_key = _get_redis_key(token, sns_type, client_type)
        cache_data = {
            "user_info": user_info,
            "cached_at": datetime.utcnow().isoformat(),
            "expires_at": (datetime.utcnow() + timedelta(seconds=ttl or TOKEN_CACHE_TTL)).isoformat()
        }
        
        redis_conn.setex(
            cache_key, 
            ttl or TOKEN_CACHE_TTL, 
            json.dumps(cache_data, ensure_ascii=False)
        )
        print(f"토큰 캐시 저장 완료: {cache_key}")
        return True
    except Exception as e:
        print(f"토큰 캐시 저장 실패: {e}")
        return False

def _get_cached_token_validation(token: str, sns_type: str, client_type: str) -> Optional[Dict]:
    """Redis에서 캐시된 토큰 검증 결과 조회"""
    try:
        redis_conn = redis_client()
        cache_key = _get_redis_key(token, sns_type, client_type)
        cached_data = redis_conn.get(cache_key)
        
        if cached_data:
            data = json.loads(cached_data)
            # 만료 시간 확인
            expires_at = datetime.fromisoformat(data["expires_at"])
            if datetime.utcnow() < expires_at:
                print(f"캐시된 토큰 사용: {cache_key}")
                return data["user_info"]
            else:
                # 만료된 캐시 삭제
                redis_conn.delete(cache_key)
                print(f"만료된 캐시 삭제: {cache_key}")
        
        return None
    except Exception as e:
        print(f"캐시된 토큰 조회 실패: {e}")
        return None

def is_staff(user: AlphafinderUser):
    email = user.get("email")
    return email.split("@")[1] in ["stoa-investment.com"]