from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.logger.logger import get_logger
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.community.v2.enum import PostOrderBy
from app.modules.community.v2.schemas import (
    CategoryResponse,
    CommentCreate,
    CommentListResponse,
    CommentUpdate,
    LikeRequest,
    LikeResponse,
    PostCreate,
    PostListResponse,
    PostUpdate,
    PresignedUrlRequest,
    PresignedUrlResponse,
    ReportItemResponse,
    ReportRequest,
    ResponsePost,
    TrendingPostResponse,
)
from app.modules.community.v2.services import CommunityService, get_community_service
from app.utils.quantus_auth_utils import get_current_user

router = APIRouter()
logger = get_logger(__name__)


##### 게시글 CRUD #####
# presign-url 발급
@router.post(
    "/presigned-url",
    response_model=BaseResponse[List[PresignedUrlResponse]],
    summary="이미지 업로드용 presigned URL 발급",
    description="""
    이미지 업로드를 위한 presigned URL을 발급받는 API입니다.

    ### 요청 데이터 (PresignedUrlRequest)
    ```json
    [
        {
            "content_type": "image/jpeg",  // 이미지 MIME 타입
            "file_size": 1024,             // 파일 크기 (바이트)
            "image_index": 0               // 이미지 인덱스 (0부터 시작)
        }
    ]
    ```

    ### 응답 데이터 (PresignedUrlResponse)
    ```json
    {
        "status_code": 200,
        "message": "presigned URL을 발급하였습니다.",
        "data": [
            {
                "upload_url": "https://...",  // 이미지 업로드 URL
                "image_key": "community/...",    // S3에 저장될 이미지 키
                "image_index": 0              // 요청한 이미지 인덱스
            }
        ]
    }
    ```

    ### 주의사항
    1. 발급받은 URL은 15분간만 유효합니다.
    2. URL 발급 후 바로 이미지를 업로드해야 합니다.
    """,
)
async def generate_presigned_url(
    request: List[PresignedUrlRequest],
    community_service: CommunityService = Depends(get_community_service),
    current_user: Optional[AlphafinderUser] = Depends(get_current_user),
):
    print(f"current_user : {current_user}")
    """이미지 업로드용 presigned URL 발급"""
    results = []
    for req in request:
        result = community_service.generate_upload_presigned_url(
            content_type=req.content_type, file_size=req.file_size, index=req.image_index
        )
        results.append(
            PresignedUrlResponse(
                upload_url=result["upload_url"], image_key=result["image_key"], image_index=req.image_index
            )
        )

    return BaseResponse(
        status_code=200,
        message="presigned URL을 발급하였습니다.",
        data=results,
    )


@router.post(
    "/posts",
    response_model=BaseResponse[dict],
    summary="게시글 생성",
    description="""
    새로운 게시글을 생성하는 API입니다.

    ### 요청 데이터 (PostCreate)
    ```json
    {
        "content": "게시글 내용",         // 선택
        "category_id": 1,                // 필수, 카테고리 ID
        "image_url": ["url1", "url2"],   // 선택, 이미지 URL 리스트
        "stock_tickers": ["AAPL", "MSFT"] // 선택, 종목 코드 리스트 (최대 3개)
    }
    ```

    ### 응답 데이터
    ```json
    {
        "status_code": 200,
        "message": "게시글을 생성하였습니다.",
        "data": {
            "success": true,
            "post_id": 123
        }
    }
    ```

    ### 에러 응답
    - 400: 종목 코드가 유효하지 않거나 최대 3개를 초과한 경우
    - 401: 로그인이 필요한 경우
    - 500: 게시글 생성 실패

    ### 주의사항
    1. 로그인이 필요합니다.
    2. 종목 코드는 최대 3개까지 입력 가능합니다.
    3. 이미지 업로드가 필요한 경우, 먼저 presigned-url API를 호출하여 URL을 발급받아야 합니다.
    """,
)
async def create_post(
    post_create: PostCreate,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """게시글 생성 API

    Args:
        post_create (PostCreate): 게시글 생성 요청 데이터
        community_service (CommunityService): 커뮤니티 서비스
        current_user (AlphafinderUser): 현재 로그인한 사용자

    Returns:
        BaseResponse[dict]: 게시글 생성 결과
            - success (bool): 생성 성공 여부
            - post_id (int): 생성된 게시글 ID

    Raises:
        PostException: 종목 코드가 유효하지 않거나 게시글 생성에 실패한 경우
    """
    try:
        result, post_id = await community_service.create_post(current_user=current_user, post_create=post_create)
        result = {"success": result, "post_id": post_id}
        return BaseResponse(status_code=200, message="게시글을 생성하였습니다.", data=result)
    except HTTPException as e:
        logger.error(f"게시글 생성 실패: {e}", exc_info=True)
        return BaseResponse(status_code=e.status_code, message=e.message)


@router.get(
    "/posts/{post_id}",
    response_model=BaseResponse[ResponsePost],
    summary="게시글 상세 조회",
    description="""
    게시글의 상세 정보를 조회하는 API입니다.

    ### URL 파라미터
    - post_id: 조회할 게시글 ID

    ### 쿼리 파라미터
    - lang: 언어 설정 (ko/en), 기본값: ko

    ### 응답 데이터 (ResponsePost)
    ```json
    {
        "status_code": 200,
        "message": "게시글을 조회하였습니다.",
        "data": {
            "id": 123,
            "content": "게시글 내용",
            "category_name": "카테고리명",
            "image_url": ["https://...", "https://..."],  // presigned URL로 변환된 이미지 URL
            "image_format": "jpg",                         // 첫 번째 이미지의 포맷
            "like_count": 42,
            "comment_count": 10,
            "is_changed": true,                           // 수정 여부
            "is_bookmarked": true,                        // 현재 사용자의 북마크 여부
            "is_liked": true,                             // 현재 사용자의 좋아요 여부
            "is_mine": true,                              // 현재 사용자의 게시글 여부
            "created_at": "2024-01-01T00:00:00+09:00",   // KST 시간대
            "depth": 0,                                   // 게시글 깊이 (0: 일반 게시글)
            "stock_tickers": [                            // 연결된 종목 정보
                {
                    "ticker": "AAPL",
                    "name": "애플",                       // 언어에 따라 한글/영문
                    "ctry": "US"
                }
            ],
            "user_info": {                                // 작성자 정보
                "id": "user123",
                "nickname": "사용자1",
                "profile_image": null,
                "image_format": null
            },
            "tagging_post_info": {                        // 태깅된 게시글 정보 (있는 경우)
                "post_id": 456,
                "content": "태깅된 게시글 내용",
                "created_at": "2024-01-01T00:00:00+09:00",
                "user_info": {
                    "id": "user456",
                    "nickname": "사용자2",
                    "profile_image": null,
                    "image_format": null
                },
                "image_url": ["https://..."],
                "image_format": "jpg"
            }
        }
    }
    ```

    ### 에러 응답
    - 404: 게시글을 찾을 수 없는 경우

    ### 주의사항
    1. 이미지 URL은 presigned URL로 자동 변환됩니다.
    2. 태깅된 게시글이 삭제된 경우, 특정 메시지로 표시됩니다.
    3. 작성자가 삭제된 경우, "알 수 없는 사용자"로 표시됩니다.
    4. 모든 시간은 KST(한국 시간)로 반환됩니다.
    5. 종목명은 요청한 언어(ko/en)에 따라 한글/영문으로 표시됩니다.₩
    """,
)
async def get_post(
    post_id: int,
    lang: Optional[TranslateCountry] = Query(TranslateCountry.KO, description="언어 설정 (ko/en)"),
    community_service: CommunityService = Depends(get_community_service),
    current_user: Optional[AlphafinderUser] = Depends(get_current_user),
):
    """게시글 상세 조회"""
    post = await community_service.get_post_detail(current_user=current_user, post_id=post_id, lang=lang)
    return BaseResponse(status_code=200, message="게시글을 조회하였습니다.", data=post)


@router.get(
    "/posts",
    response_model=PostListResponse,
    summary="게시글 목록 조회",
    description="""
    게시글 목록을 조회하는 API입니다. 페이지네이션, 필터링, 정렬 기능을 제공합니다.

    ### 쿼리 파라미터
    - offset: 검색 시작 위치 (기본값: 0)
    - limit: 검색 결과 수 (기본값: 10)
    - category_id: 카테고리 ID (선택)
    - stock_ticker: 종목 코드 (선택)
    - lang: 언어 설정 (ko/en), 기본값: ko
    - order_by: 정렬 기준 (created_at/like_count), 기본값: created_at

    ### 응답 데이터 (PostListResponse)
    ```json
    {
        "status_code": 200,
        "message": "게시글 목록을 조회하였습니다.",
        "has_more": true,  // 다음 페이지 존재 여부
        "data": [
            {
                "id": 123,
                "content": "게시글 내용",
                "category_name": "카테고리명",
                "image_url": ["https://...", "https://..."],  // presigned URL로 변환된 이미지 URL
                "image_format": "jpg",                         // 첫 번째 이미지의 포맷
                "like_count": 42,
                "comment_count": 10,
                "is_changed": true,                           // 수정 여부
                "is_bookmarked": true,                        // 현재 사용자의 북마크 여부
                "is_liked": true,                             // 현재 사용자의 좋아요 여부
                "is_mine": true,                              // 현재 사용자의 게시글 여부
                "created_at": "2024-01-01T00:00:00+09:00",   // KST 시간대
                "depth": 0,                                   // 게시글 깊이 (0: 일반 게시글)
                "stock_tickers": [                            // 연결된 종목 정보
                    {
                        "ticker": "AAPL",
                        "name": "애플",                       // 언어에 따라 한글/영문
                        "ctry": "US"
                    }
                ],
                "user_info": {                                // 작성자 정보
                    "id": "user123",
                    "nickname": "사용자1",
                    "profile_image": null,
                    "image_format": null
                },
                "tagging_post_info": {                        // 태깅된 게시글 정보 (있는 경우)
                    "post_id": 456,
                    "content": "태깅된 게시글 내용",
                    "created_at": "2024-01-01T00:00:00+09:00",
                    "user_info": {
                        "id": "user456",
                        "nickname": "사용자2",
                        "profile_image": null,
                        "image_format": null
                    },
                    "image_url": ["https://..."],
                    "image_format": "jpg"
                }
            }
        ]
    }
    ```

    ### 에러 응답
    - 400: 잘못된 파라미터 값
    - 401: 로그인이 필요한 경우

    ### 주의사항
    1. 이미지 URL은 presigned URL로 자동 변환됩니다.
    2. 태깅된 게시글이 삭제된 경우, 특정 메시지로 표시됩니다.
    3. 작성자가 삭제된 경우, "알 수 없는 사용자"로 표시됩니다.
    4. 모든 시간은 KST(한국 시간)로 반환됩니다.
    5. 종목명은 요청한 언어(ko/en)에 따라 한글/영문으로 표시됩니다.
    6. has_more가 true인 경우, 다음 페이지가 존재합니다.
    7. depth가 0인 게시글만 조회됩니다 (일반 게시글).
    """,
)
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


@router.put(
    "/posts/{post_id}",
    response_model=BaseResponse[dict],
    summary="게시글 수정",
    description="""
    게시글을 수정하는 API입니다. 작성자만 수정할 수 있습니다.

    ### URL 파라미터
    - post_id: 수정할 게시글 ID

    ### 요청 데이터 (PostUpdate)
    ```json
    {
        "content": "수정된 게시글 내용",
        "category_id": 1,
        "image_url": ["url1", "url2"],
        "stock_tickers": ["AAPL", "MSFT"],
        "tagging_post_id": 456
    }
    ```

    ### 응답 데이터
    ```json
    {
        "status_code": 200,
        "message": "게시글을 수정하였습니다.",
        "data": {
            "success": true,      // 수정 성공 여부
            "post_id": 123        // 수정된 게시글 ID
        }
    }
    ```

    ### 에러 응답
    - 400: 종목 코드가 유효하지 않거나 최대 3개를 초과한 경우
    - 401: 로그인이 필요한 경우
    - 403: 게시글 수정 권한이 없는 경우
    - 404: 게시글을 찾을 수 없는 경우
    - 500: 게시글 수정 중 오류가 발생한 경우

    ### 주의사항
    1. 로그인이 필요합니다.
    2. 작성자만 게시글을 수정할 수 있습니다.
    3. 종목 코드는 최대 3개까지 입력 가능합니다.
    4. 종목 정보가 변경된 경우, 기존 종목 정보는 모두 삭제되고 새로운 종목 정보가 추가됩니다.
    5. 태깅된 게시글이 변경된 경우, tagging_post_id가 업데이트됩니다.
    """,
)
async def update_post(
    post_id: int,
    post_update: PostUpdate,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """게시글 수정"""
    result, post_id = await community_service.update_post(
        current_user=current_user, post_id=post_id, post_update=post_update
    )
    data = {"success": result, "post_id": post_id}
    return BaseResponse(status_code=200, message="게시글을 수정하였습니다.", data=data)


@router.delete(
    "/posts/{post_id}",
    response_model=BaseResponse[bool],
    summary="게시글/댓글 삭제",
    description="""
    게시글 또는 댓글을 삭제하는 API입니다. 작성자만 삭제할 수 있습니다.

    ### URL 파라미터
    - post_id: 삭제할 게시글/댓글 ID

    ### 응답 데이터
    ```json
    {
        "status_code": 200,
        "message": "게시글/댓글을 삭제하였습니다.",
        "data": true  // 삭제 성공 여부
    }
    ```

    ### 에러 응답
    - 401: 로그인이 필요한 경우
    - 403: 삭제 권한이 없는 경우
    - 404: 게시글/댓글을 찾을 수 없는 경우
    - 500: 삭제 중 오류가 발생한 경우

    ### 주의사항
    1. 로그인이 필요합니다.
    2. 작성자만 삭제할 수 있습니다.
    3. 게시글 삭제 시 연결된 댓글도 함께 삭제됩니다 (cascade 설정).
    """,
)
async def delete_post(
    post_id: int,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """게시글/댓글 삭제"""
    result = await community_service.delete_content(current_user=current_user, content_id=post_id)
    return BaseResponse(status_code=200, message="게시글/댓글을 삭제하였습니다.", data=result)


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
    lang: Optional[TranslateCountry] = Query(TranslateCountry.KO, description="언어 설정 (ko/en)"),
    community_service: CommunityService = Depends(get_community_service),
    current_user: Optional[AlphafinderUser] = Depends(get_current_user),
):
    """댓글 목록 조회"""
    comments, has_more = await community_service.get_comments(
        current_user=current_user, post_id=post_id, offset=offset, limit=limit, lang=lang
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


##### 좋아요 on/off #####
@router.put(
    "/posts/{post_id}/like",
    response_model=BaseResponse[LikeResponse],
    summary="게시글/댓글 좋아요 상태 업데이트",
    description="""
    게시글 또는 댓글의 좋아요 상태를 업데이트하는 API입니다.

    ### 요청 데이터 (LikeRequest)
    ```json
    {
        "is_liked": true  // true: 좋아요 추가, false: 좋아요 취소
    }
    ```

    ### 응답 데이터 (LikeResponse)
    ```json
    {
        "status_code": 200,
        "message": "좋아요 상태를 업데이트하였습니다.",
        "data": {
            "is_liked": true,      // 현재 좋아요 상태
            "like_count": 42       // 현재 좋아요 수
        }
    }
    ```

    ### 에러 응답
    - 401: 로그인이 필요한 경우
    - 404: 게시글/댓글을 찾을 수 없는 경우

    ### 주의사항
    1. 로그인이 필요합니다.
    2. 게시글과 댓글 모두 동일한 엔드포인트를 사용합니다.
    3. 좋아요 상태가 현재 상태와 같으면 아무 작업도 수행하지 않습니다.
    4. 좋아요 수는 자동으로 업데이트됩니다.
    """,
)
async def update_like(
    post_id: int,
    like_request: LikeRequest,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """게시글/댓글 좋아요 상태 업데이트"""
    is_liked, like_count = await community_service.update_post_like(
        current_user=current_user, post_id=post_id, is_liked=like_request.is_liked
    )

    return BaseResponse(
        status_code=200,
        message="좋아요 상태를 업데이트하였습니다.",
        data=LikeResponse(is_liked=is_liked, like_count=like_count),
    )


### 북마크 on/off #### # 필요시 주석 해제
# @router.put("/posts/{post_id}/bookmark", response_model=BaseResponse[BookmarkItem], summary="게시글 북마크 추가")
# async def update_post_bookmark(
#     post_id: int,
#     bookmark_item: BookmarkItem,
#     community_service: CommunityService = Depends(get_community_service),
#     current_user: AlphafinderUser = Depends(get_current_user),
# ):
#     """게시글 북마크 추가"""
#     is_bookmarked = await community_service.update_post_bookmark(
#         current_user=current_user, post_id=post_id, is_bookmarked=bookmark_item.is_bookmarked
#     )

#     return BaseResponse(
#         status_code=200, message="북마크 상태를 업데이트하였습니다.", data=BookmarkItem(is_bookmarked=is_bookmarked)
#     )


### 실시간 인기 게시글 조회 ###
@router.get("/trending/posts", response_model=BaseResponse[List[TrendingPostResponse]], summary="실시간 인기 게시글 조회")
async def get_trending_posts(
    limit: int = Query(5, description="조회할 게시글 수 / default: 5", ge=1, le=50),
    community_service: CommunityService = Depends(get_community_service),
):
    trending_posts = await community_service.get_trending_posts(limit=limit)
    return BaseResponse(status_code=200, message="실시간 인기 게시글을 조회하였습니다.", data=trending_posts)


# @router.get("/trending/stocks", response_model=BaseResponse[List[TrendingStockResponse]], summary="실시간 인기 종목 조회")
# async def get_trending_stocks(
#     limit: int = Query(5, description="조회할 종목 수", ge=1, le=50),
#     lang: Optional[TranslateCountry] = Query(TranslateCountry.KO, description="언어 설정 (ko/en)"),
#     community_service: CommunityService = Depends(get_community_service),
# ):
#     trending_stocks = await community_service.get_trending_stocks(limit=limit, lang=lang)
#     return BaseResponse(status_code=200, message="실시간 인기 종목을 조회하였습니다.", data=trending_stocks)


### 카테고리 리스트 조회 ###
@router.get("/categories", response_model=BaseResponse[List[CategoryResponse]], summary="카테고리 리스트 조회")
async def get_categories(
    community_service: CommunityService = Depends(get_community_service),
):
    categories = await community_service.get_categories()
    return BaseResponse(status_code=200, message="카테고리 리스트를 조회하였습니다.", data=categories)


########################
# 신고 기능
########################
# 신고 가능 항목
@router.get("/report", response_model=BaseResponse[List[ReportItemResponse]], summary="신고 가능 항목 조회")
async def get_report_items(
    community_service: CommunityService = Depends(get_community_service),
):
    report_items = await community_service.get_report_items()
    return BaseResponse(status_code=200, message="신고 가능 항목을 조회하였습니다.", data=report_items)


# 신고 기능
@router.post("/report", response_model=BaseResponse[bool], summary="신고 기능")
async def report(
    report_request: ReportRequest,
    community_service: CommunityService = Depends(get_community_service),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    report_response = await community_service.report_post(report_request)
    return BaseResponse(status_code=200, message="신고 기능을 완료하였습니다.", data=report_response)
