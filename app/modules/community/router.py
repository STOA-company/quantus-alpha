from typing import Optional
from app.modules.common.schemas import BaseResponse
from app.modules.community.enum import PostOrderBy
from app.modules.community.schemas import (
    CommentCreate,
    CommentListResponse,
    CommentUpdate,
    PostCreate,
    PostListResponse,
    PostUpdate,
    ResponsePost,
)
from app.modules.community.services import CommunityService, get_community_service
from app.utils.oauth_utils import get_current_user
from app.models.models_users import AlphafinderUser
from fastapi import APIRouter, Depends, Query

router = APIRouter()


##### 게시글 CRUD #####
@router.post("/posts", response_model=BaseResponse[bool], summary="게시글 생성")
async def create_new_post(
    post_create: PostCreate,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    post = await community_service.create_post(current_user=current_user, post_create=post_create)
    return BaseResponse(status_code=200, message="게시글을 생성하였습니다.", data=post)


@router.get("/posts/{post_id}", response_model=BaseResponse[ResponsePost], summary="게시글 상세 조회")
async def get_post(
    post_id: int,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    post = await community_service.get_post_detail(current_user=current_user, post_id=post_id)
    return BaseResponse(status_code=200, message="게시글을 조회하였습니다.", data=post)


@router.get("/posts", response_model=PostListResponse, summary="게시글 목록 조회")
async def get_posts(
    offset: int = Query(0, description="검색 시작 위치"),
    limit: int = Query(10, description="검색 결과 수"),
    category_id: Optional[int] = Query(None, description="카테고리 ID"),
    stock_ticker: Optional[str] = Query(None, description="종목 코드"),
    order_by: Optional[PostOrderBy] = Query(PostOrderBy.created_at, description="정렬 기준 (created_at, like_count)"),
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """게시글 목록 조회"""
    posts = await community_service.get_posts(
        current_user=current_user,
        offset=offset,
        limit=limit + 1,  # 한 개 더 요청
        category_id=category_id,
        stock_ticker=stock_ticker,
        order_by=order_by,
    )

    has_more = len(posts) > limit
    if has_more:
        posts = posts[:-1]  # 마지막 항목 제거

    return PostListResponse(status_code=200, message="게시글 목록을 조회하였습니다.", has_more=has_more, data=posts)


@router.put("/posts/{post_id}", response_model=BaseResponse[bool], summary="게시글 수정")
async def update_post(
    post_id: int,
    post_update: PostUpdate,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    post = await community_service.update_post(current_user=current_user, post_id=post_id, post_update=post_update)
    return BaseResponse(status_code=200, message="게시글을 수정하였습니다.", data=post)


@router.delete("/posts/{post_id}", response_model=BaseResponse[bool], summary="게시글 삭제")
async def delete_post(
    post_id: int,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    post = await community_service.delete_post(current_user=current_user, post_id=post_id)
    return BaseResponse(status_code=200, message="게시글을 삭제하였습니다.", data=post)


##### 댓글 CRUD #####
@router.post("/posts/{post_id}/comments", response_model=BaseResponse[bool], summary="댓글 생성")
async def create_comment(
    post_id: int,
    comment_create: CommentCreate,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """댓글 생성"""
    result = await community_service.create_comment(
        current_user=current_user, post_id=post_id, comment_create=comment_create
    )
    return BaseResponse(status_code=200, message="댓글을 생성하였습니다.", data=result)


@router.get("/posts/{post_id}/comments", response_model=CommentListResponse, summary="댓글 목록 조회")
async def get_comments(
    post_id: int,
    offset: int = Query(0, description="조회 시작 위치"),
    limit: int = Query(10, description="조회할 댓글 수"),
    community_service: CommunityService = Depends(get_community_service),
    current_user: Optional[AlphafinderUser] = Depends(get_current_user),
):
    """댓글 목록 조회"""
    comments, has_more = await community_service.get_comments(
        current_user=current_user, post_id=post_id, offset=offset, limit=limit
    )

    return CommentListResponse(status_code=200, message="댓글 목록을 조회하였습니다.", has_more=has_more, data=comments)


@router.put("/comments/{comment_id}", response_model=BaseResponse[bool], summary="댓글 수정")
async def update_comment(
    comment_id: int,
    comment_update: CommentUpdate,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """댓글 수정"""
    result = await community_service.update_comment(
        current_user=current_user, comment_id=comment_id, comment_update=comment_update
    )

    return BaseResponse(status_code=200, message="댓글을 수정하였습니다.", data=result)


@router.delete("/comments/{comment_id}", response_model=BaseResponse[bool], summary="댓글 삭제")
async def delete_comment(
    comment_id: int,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """댓글 삭제"""
    result = await community_service.delete_comment(current_user=current_user, comment_id=comment_id)
    return BaseResponse(status_code=200, message="댓글을 삭제하였습니다.", data=result)


##### 좋아요 CRUD #####
