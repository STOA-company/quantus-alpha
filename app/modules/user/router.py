from fastapi import APIRouter, Depends, File, Query, UploadFile, Form
from app.models.models_users import AlphafinderUser
from app.modules.common.schemas import BaseResponse, InfiniteScrollResponse
from app.modules.community.schemas import CommentItemWithPostInfo, ResponsePost
from app.utils.oauth_utils import get_current_user
from app.modules.user.service import UserProfileService, delete_user, get_user_profile_service
from app.modules.user.schemas import UserInfoResponse, UserProfileResponse
from fastapi import HTTPException
from fastapi.security import HTTPBearer
from app.utils.oauth_utils import (
    decode_email_token,
    create_jwt_token,
    create_refresh_token,
    store_token,
    refresh_access_token,
)
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
    access_token_hash = store_token(access_token, refresh_token)

    return {"message": "Signup successful", "access_token_hash": access_token_hash}


@router.get("/me", response_model=UserInfoResponse)
async def get_user_info(current_user: AlphafinderUser = Depends(get_current_user)):
    """현재 인증된 사용자 정보 반환"""
    if not current_user:
        raise HTTPException(
            status_code=401,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return UserInfoResponse(id=current_user.id, email=current_user.email, nickname=current_user.nickname)


@router.get("/cancel")
def signup_cancel(current_user: AlphafinderUser = Depends(get_current_user)):
    try:
        delete_user(current_user.id)
        return {"message": "User deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/profile")
def get_profile(
    user_id: Optional[int] = Query(None, description="보여질 페이지의 사용자 ID, default: 현재 로그인한 사용자"),
    current_user: AlphafinderUser = Depends(get_current_user),
    service: UserProfileService = Depends(get_user_profile_service),
) -> BaseResponse[UserProfileResponse]:
    data = service.get_user_profile(current_user, user_id)
    return BaseResponse(status_code=200, message="Profile retrieved successfully", data=data)


@router.get("/refresh")
def get_new_access_token(access_token_hash: str):
    new_access_token = refresh_access_token(access_token_hash=access_token_hash)
    return {"new_access_token": new_access_token}


@router.get("/users/posts", response_model=InfiniteScrollResponse[ResponsePost], summary="사용자 게시글 목록 조회")
async def get_user_posts(
    offset: int = Query(0, description="검색 시작 위치"),
    limit: int = Query(10, description="검색 결과 수"),
    user_id: Optional[int] = Query(None, description="보여질 페이지의 사용자 ID, default: 현재 로그인한 사용자"),
    current_user: AlphafinderUser = Depends(get_current_user),
    service: UserProfileService = Depends(get_user_profile_service),
):
    """사용자가 작성한 게시글 목록 조회"""
    posts, has_more = await service.get_user_posts(current_user=current_user, user_id=user_id, offset=offset, limit=limit)

    return InfiniteScrollResponse(
        status_code=200, message="사용자 게시글 목록을 조회하였습니다.", has_more=has_more, data=posts
    )


@router.get(
    "/users/comments", response_model=InfiniteScrollResponse[CommentItemWithPostInfo], summary="사용자 댓글 목록 조회"
)
async def get_user_comments(
    offset: int = Query(0, description="검색 시작 위치"),
    limit: int = Query(10, description="검색 결과 수"),
    user_id: Optional[int] = Query(None, description="보여질 페이지의 사용자 ID, default: 현재 로그인한 사용자"),
    current_user: AlphafinderUser = Depends(get_current_user),
    service: UserProfileService = Depends(get_user_profile_service),
):
    """사용자가 작성한 댓글 목록 조회"""
    comments, has_more = await service.get_user_comments(
        current_user=current_user, user_id=user_id, offset=offset, limit=limit
    )

    return InfiniteScrollResponse(
        status_code=200, message="사용자 댓글 목록을 조회하였습니다.", has_more=has_more, data=comments
    )
