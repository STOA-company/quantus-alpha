import os
import hashlib
from datetime import datetime, timedelta, timezone

from jose import jwt, JWTError, ExpiredSignatureError
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
security = HTTPBearer(auto_error=False)


def create_jwt_token(user_id: int, expires_delta: timedelta = None) -> str:
    """JWT 토큰 생성"""
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(seconds=30)

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


def refresh_access_token(access_token_hash: str):
    token_record = database._select(table="alphafinder_oauth_token", access_token_hash=access_token_hash)
    if not token_record:
        raise HTTPException(
            status_code=401,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token_data = token_record[0]

    try:
        try:
            jwt.decode(token_data.access_token, JWT_SECRET_KEY, algorithms=JWT_ALGORITHM)
            return token_data.access_token_hash

        except ExpiredSignatureError:
            current_time = datetime.now(timezone.utc)
            refresh_token = token_data.refresh_token
            refresh_payload = jwt.decode(refresh_token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])

            if current_time > datetime.fromtimestamp(refresh_payload.get("exp"), tz=timezone.utc):
                raise HTTPException(
                    status_code=401,
                    detail="Refresh Token Expired",
                    headers={"WWW-Authenticate": "Bearer"},
                )

            user_id = int(refresh_payload.get("sub"))

            new_payload = {
                "sub": user_id,
                "exp": (current_time + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)).timestamp(),
                "iat": current_time.timestamp(),
            }
            new_access_token = jwt.encode(new_payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
            new_access_token_hash = hashlib.sha256(new_access_token.encode()).hexdigest()

            database._update(
                table="alphafinder_oauth_token",
                sets={"access_token": new_access_token, "access_token_hash": new_access_token_hash},
                refresh_token=refresh_token,
            )

            return new_access_token_hash

        except JWTError:
            raise HTTPException(
                status_code=401,
                detail="Invalid token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    except JWTError:
        raise HTTPException(
            status_code=401,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )


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


def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security),
) -> Optional[AlphafinderUser]:
    """현재 인증된 사용자 정보 조회"""
    if not credentials:
        return None

    try:
        hashed_token = credentials.credentials

        try:
            token_record = database._select(table="alphafinder_oauth_token", access_token_hash=hashed_token)
            if not token_record:
                raise HTTPException(
                    status_code=401,
                    detail="Invalid token",
                    headers={"WWW-Authenticate": "Bearer"},
                )

            token_data = token_record[0]

            try:
                payload = jwt.decode(token_data.access_token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
                user_id = int(payload.get("sub"))

            except JWTError:
                raise HTTPException(
                    status_code=401,
                    detail="Access Token Expired",
                    headers={"WWW-Authenticate": "Bearer"},
                )

            user = database._select(table="alphafinder_user", id=user_id)
            if not user:
                raise HTTPException(
                    status_code=401,
                    detail="User not found",
                    headers={"WWW-Authenticate": "Bearer"},
                )

            return user[0]

        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Internal server error",
            )

    except JWTError as e:
        logger.error(f"JWT verification failed: {str(e)}")
        raise HTTPException(
            status_code=401,
            detail="Invalid token format",
            headers={"WWW-Authenticate": "Bearer"},
        )


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
