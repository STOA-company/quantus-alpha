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
from app.modules.user.service import create_user, add_favorite_stock, check_nickname_available, update_profile
from app.utils.image_utils import convert_file_to_base64
import json
from typing import Optional

router = APIRouter()

security = HTTPBearer()


@router.post("/signup")
def signup(
    email_token: str = Form(...),
    provider: str = Form(default="google"),
    nickname: str = Form(...),
    favorite_stocks: Optional[str] = Form(None),
    profile_image: UploadFile = File(...),
    image_format: Optional[str] = Form(None),
):
    email = decode_email_token(email_token)["sub"]
    base64 = None
    if profile_image:
        base64 = convert_file_to_base64(profile_image)
    user = create_user(email, nickname, provider, base64, image_format)
    favorite_stock_list = json.loads(favorite_stocks)
    if favorite_stock_list:
        for ticker in favorite_stock_list:
            add_favorite_stock(user.id, ticker)

    access_token = create_jwt_token(user.id)
    refresh_token = create_refresh_token(user.id)
    access_token_hash = store_token(access_token, refresh_token)

    return {"message": "Signup successful", "access_token_hash": access_token_hash}


@router.patch("/nickname")
def update_nickname(nickname: str, current_user: AlphafinderUser = Depends(get_current_user)):
    update_profile(current_user.id, nickname)

    return BaseResponse(status_code=200, message="Nickname updated successfully")


@router.patch("/profile-image")
def update_profile_image(
    profile_image: UploadFile = File(...),
    image_format: Optional[str] = Form(None),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    base64 = convert_file_to_base64(profile_image)
    update_profile(current_user.id, base64, image_format)

    return BaseResponse(status_code=200, message="Profile image updated successfully")


@router.delete("/delete")
def user_delete(current_user: AlphafinderUser = Depends(get_current_user)):
    delete_user(current_user.id)

    return BaseResponse(status_code=200, message="User deleted successfully")


@router.get("/me", response_model=UserInfoResponse)
def get_user_info(current_user: AlphafinderUser = Depends(get_current_user)):
    """현재 인증된 사용자 정보 반환"""
    if not current_user:
        raise HTTPException(
            status_code=401,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return UserInfoResponse(
        id=current_user.id,
        email=current_user.email,
        nickname=current_user.nickname,
        profile_image=current_user.profile_image,
    )


@router.get("/duplicate")
def check_nickname_duplicate(nickname: str):
    is_available = check_nickname_available(nickname)
    return {"is_available": is_available}


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
    new_access_token_hash = refresh_access_token(access_token_hash=access_token_hash)
    return {"new_access_token_hash": new_access_token_hash}


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
