import os
from datetime import datetime, timedelta
from jose import jwt, JWTError
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from app.models.models_users import AlphafinderUser
from app.database.crud import database
from typing import Optional

ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM")

# Security configurations
security = HTTPBearer()


def create_jwt_token(user_id: int, expires_delta: timedelta = None) -> str:
    """JWT 토큰 생성"""
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode = {"exp": expire, "sub": str(user_id), "iat": datetime.utcnow()}

    return jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def create_refresh_token(user_id: int) -> str:
    """리프레시 토큰 생성"""
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode = {"exp": expire, "sub": str(user_id), "iat": datetime.utcnow(), "type": "refresh"}
    return jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def refresh_access_token(credentials):
    try:
        # JWT 토큰 디코딩 및 검증
        payload = decode_jwt_token(credentials.credentials)

        # 리프레시 토큰 검증
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=400, detail="Invalid token type")

        # 사용자 조회
        user_id = int(payload.get("sub"))
        user = database._select(table="alphafinder_user", id=user_id)

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        return create_jwt_token(user.id)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def decode_jwt_token(token: str):
    try:
        return jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security),
) -> Optional[AlphafinderUser]:
    """현재 인증된 사용자 정보 조회"""
    if not credentials:
        return None

    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        user_id = int(payload.get("sub"))
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = database._select(table="alphafinder_user", id=user_id)
    if user is None:
        raise credentials_exception

    return user
