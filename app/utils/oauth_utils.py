import os
import hashlib
from datetime import datetime, timedelta
from jose import jwt, JWTError
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from app.models.models_users import AlphafinderUser
from app.database.crud import database
from typing import Optional
import logging

logger = logging.getLogger(__name__)

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


def create_email_token(email: str) -> str:
    """이메일 토큰 생성"""
    expire = datetime.utcnow() + timedelta(days=1)
    to_encode = {"exp": expire, "sub": email, "iat": datetime.utcnow(), "type": "email"}
    return jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def decode_email_token(token: str):
    try:
        return jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


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
        hashed_token = credentials.credentials

        token_record = database._select(table="alphafinder_oauth_token", access_token_hash=hashed_token)

        if not token_record:
            raise credentials_exception

        token_data = token_record[0]

        try:
            payload = jwt.decode(token_data.access_token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
            user_id = int(payload.get("sub"))
            if user_id is None:
                raise credentials_exception

        except jwt.ExpiredSignatureError:
            try:
                refresh_payload = jwt.decode(token_data.refresh_token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])

                user_id = int(refresh_payload.get("sub"))
                if user_id is None:
                    raise credentials_exception
                new_access_token = create_jwt_token(user_id)
                new_hashed_token = hashlib.sha256(new_access_token.encode()).hexdigest()

                database._update(
                    table="alphafinder_oauth_token",
                    where={"access_token_hash": hashed_token},
                    access_token=new_access_token,
                    access_token_hash=new_hashed_token,
                )

            except jwt.ExpiredSignatureError:
                database._delete(table="alphafinder_oauth_token", access_token_hash=hashed_token)
                raise HTTPException(
                    status_code=401,
                    detail="Token has expired. Please log in again",
                    headers={"WWW-Authenticate": "Bearer"},
                )

    except JWTError:
        raise credentials_exception

    user = database._select(table="alphafinder_user", id=user_id)
    if user is None:
        raise credentials_exception

    return user[0]


def store_token(access_token: str, refresh_token: str):
    try:
        access_token_hash = hashlib.sha256(access_token.encode()).hexdigest()

        existing_token = database._select(table="alphafinder_oauth_token", access_token_hash=access_token_hash)

        if existing_token:
            database._delete(table="alphafinder_oauth_token", access_token_hash=access_token_hash)

        database._insert(
            table="alphafinder_oauth_token",
            sets={
                "access_token": access_token,
                "refresh_token": refresh_token,
                "access_token_hash": access_token_hash,
            },
        )
        logger.info(f"Token stored: {access_token} {refresh_token}")
        return access_token_hash
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def delete_token(access_token_hash: str):
    try:
        database._delete(table="alphafinder_oauth_token", access_token_hash=access_token_hash)
        logger.info(f"Token deleted: {access_token_hash}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
