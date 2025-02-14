from typing import List, Optional
from app.modules.common.enum import TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.community.enum import PostOrderBy
from app.modules.community.schemas import (
    BookmarkItem,
    CategoryResponse,
    CommentCreate,
    CommentListResponse,
    CommentUpdate,
    LikeRequest,
    LikeResponse,
    PostCreate,
    PostListResponse,
    PostUpdate,
    ResponsePost,
    TrendingPostResponse,
    TrendingStockResponse,
)
from app.modules.community.services import CommunityService, get_community_service
from app.utils.oauth_utils import get_current_user
from app.models.models_users import AlphafinderUser
from fastapi import APIRouter, Depends, Query

router = APIRouter()


##### 게시글 CRUD #####
@router.post("/posts", response_model=BaseResponse[dict], summary="게시글 생성")
async def create_new_post(
    post_create: PostCreate,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    result, post_id = await community_service.create_post(current_user=current_user, post_create=post_create)
    result = {"success": result, "post_id": post_id}
    return BaseResponse(status_code=200, message="게시글을 생성하였습니다.", data=result)


@router.get("/posts/{post_id}", response_model=BaseResponse[ResponsePost], summary="게시글 상세 조회")
async def get_post(
    post_id: int,
    lang: Optional[TranslateCountry] = Query(TranslateCountry.KO, description="언어 설정 (ko/en)"),
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    post = await community_service.get_post_detail(current_user=current_user, post_id=post_id, lang=lang)
    return BaseResponse(status_code=200, message="게시글을 조회하였습니다.", data=post)


@router.get("/posts", response_model=PostListResponse, summary="게시글 목록 조회")
async def get_posts(
    offset: int = Query(0, description="검색 시작 위치"),
    limit: int = Query(10, description="검색 결과 수"),
    category_id: Optional[int] = Query(None, description="카테고리 ID"),
    stock_ticker: Optional[str] = Query(None, description="종목 코드"),
    lang: Optional[TranslateCountry] = Query(TranslateCountry.KO, description="언어 설정 (ko/en)"),
    order_by: Optional[PostOrderBy] = Query(PostOrderBy.created_at, description="정렬 기준 (created_at, like_count)"),
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """게시글 목록 조회"""
    posts = await community_service.get_posts(
        current_user=current_user,
        offset=offset,
        limit=limit + 1,
        category_id=category_id,
        stock_ticker=stock_ticker,
        order_by=order_by,
        lang=lang,
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
    result, post_id = await community_service.update_post(
        current_user=current_user, post_id=post_id, post_update=post_update
    )
    data = {"success": result, "post_id": post_id}
    return BaseResponse(status_code=200, message="게시글을 수정하였습니다.", data=data)


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


##### 좋아요 on/off #####
@router.put("/posts/{post_id}/like", response_model=BaseResponse[LikeResponse], summary="게시글 좋아요 상태 업데이트")
async def update_post_like(
    post_id: int,
    like_request: LikeRequest,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """게시글 좋아요 상태 업데이트"""
    is_liked, like_count = await community_service.update_post_like(
        current_user=current_user, post_id=post_id, is_liked=like_request.is_liked
    )

    return BaseResponse(
        status_code=200,
        message="좋아요 상태를 업데이트하였습니다.",
        data=LikeResponse(is_liked=is_liked, like_count=like_count),
    )


@router.put("/comments/{comment_id}/like", response_model=BaseResponse[LikeResponse], summary="댓글 좋아요 상태 업데이트")
async def update_comment_like(
    comment_id: int,
    like_request: LikeRequest,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """게시글 좋아요 상태 업데이트"""
    is_liked, like_count = await community_service.update_comment_like(
        current_user=current_user, comment_id=comment_id, is_liked=like_request.is_liked
    )

    return BaseResponse(
        status_code=200,
        message="좋아요 상태를 업데이트하였습니다.",
        data=LikeResponse(is_liked=is_liked, like_count=like_count),
    )


### 북마크 on/off ####
@router.put("/posts/{post_id}/bookmark", response_model=BaseResponse[BookmarkItem], summary="게시글 북마크 추가")
async def update_post_bookmark(
    post_id: int,
    bookmark_item: BookmarkItem,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """게시글 북마크 추가"""
    is_bookmarked = await community_service.update_post_bookmark(
        current_user=current_user, post_id=post_id, is_bookmarked=bookmark_item.is_bookmarked
    )

    return BaseResponse(
        status_code=200, message="북마크 상태를 업데이트하였습니다.", data=BookmarkItem(is_bookmarked=is_bookmarked)
    )


### 실시간 인기 게시글 조회 ###
@router.get("/trending/posts", response_model=BaseResponse[List[TrendingPostResponse]], summary="실시간 인기 게시글 조회")
async def get_trending_posts(
    limit: int = Query(5, description="조회할 게시글 수 / default: 5", ge=1, le=50),
    community_service: CommunityService = Depends(get_community_service),
):
    trending_posts = await community_service.get_trending_posts(limit=limit)
    return BaseResponse(status_code=200, message="실시간 인기 게시글을 조회하였습니다.", data=trending_posts)


@router.get("/trending/stocks", response_model=BaseResponse[List[TrendingStockResponse]], summary="실시간 인기 종목 조회")
async def get_trending_stocks(
    limit: int = Query(5, description="조회할 종목 수", ge=1, le=50),
    lang: Optional[TranslateCountry] = Query(TranslateCountry.KO, description="언어 설정 (ko/en)"),
    community_service: CommunityService = Depends(get_community_service),
):
    trending_stocks = await community_service.get_trending_stocks(limit=limit, lang=lang)
    return BaseResponse(status_code=200, message="실시간 인기 종목을 조회하였습니다.", data=trending_stocks)


### 카테고리 리스트 조회 ###
@router.get("/categories", response_model=BaseResponse[List[CategoryResponse]], summary="카테고리 리스트 조회")
async def get_categories(
    community_service: CommunityService = Depends(get_community_service),
):
    categories = await community_service.get_categories()
    return BaseResponse(status_code=200, message="카테고리 리스트를 조회하였습니다.", data=categories)
