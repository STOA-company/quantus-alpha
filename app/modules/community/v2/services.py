import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import text

from app.common.constants import KST, UNKNOWN_USER_EN, UNKNOWN_USER_KO, UTC
from app.core.exception.custom import PostException, TooManyStockTickersException
from app.core.logging.config import get_logger
from app.core.redis import redis_client
from app.database.crud import database, database_service, database_user
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from Aws.common.configs import s3_client

from .schemas import (
    CategoryResponse,
    CommentCreate,
    CommentItem,
    CommentUpdate,
    PostCreate,
    PostUpdate,
    ResponsePost,
    StockInfo,
    TaggingPostInfo,
    TrendingPostResponse,
    UserInfo,
)

logger = get_logger(__name__)


class CommunityService:
    def __init__(self):
        self.db = database_service
        self.db_data = database
        self.s3_bucket = "alpha-finder-image"  # S3 버킷 이름 설정 필요
        self.redis = redis_client()

        # 허용 가능한 Content-Type과 확장자 매핑
        self.ALLOWED_CONTENT_TYPES = {"image/jpeg": "jpg", "image/png": "png", "image/gif": "gif"}
        # 최대 파일 크기 (5MB)
        self.MAX_FILE_SIZE = 5 * 1024 * 1024
        # presigned URL 만료 시간 (5분)
        self.PRESIGNED_URL_EXPIRES_IN = 300
        # Redis 캐시 만료 시간 (4분 30초)
        self.REDIS_CACHE_EXPIRES_IN = 270

    def _handle_stock_tickers(self, post_id: int, stock_tickers: Optional[List[str]]) -> None:
        """종목 정보 처리"""
        if not stock_tickers:
            return

        if len(stock_tickers) > 3:
            raise TooManyStockTickersException()

        stock_data = [
            {
                "post_id": post_id,
                "stock_ticker": ticker,
            }
            for ticker in stock_tickers
        ]
        if stock_data:
            self.db._insert("af_post_stock_tags", stock_data)

    async def create_post(self, current_user: Optional[AlphafinderUser], post_create: PostCreate) -> Tuple[bool, int]:
        """게시글 생성"""
        if post_create.content is None and post_create.image_url is None:
            raise PostException(message="게시글 내용이 없습니다", status_code=400)
        if post_create.category_id is None:
            raise PostException(message="카테고리가 없습니다", status_code=400)
        current_time = datetime.now(UTC)
        is_stock_ticker = self._is_stock_ticker(post_create.stock_tickers)
        if not is_stock_ticker:
            raise PostException(message="종목 코드가 유효하지 않습니다", status_code=400)

        if not current_user or current_user["uid"] is None:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        user_id = current_user["uid"] if current_user else None

        # 이미지 URL에서 확장자 추출
        image_formats = []
        if post_create.image_url:
            for url in post_create.image_url:
                # URL에서 마지막 부분을 가져옴
                filename = url.split("/")[-1]
                # 파일명에서 확장자 추출
                extension = filename.split(".")[-1].lower()
                if extension in self.ALLOWED_CONTENT_TYPES.values():
                    image_formats.append(extension)
                else:
                    raise PostException(message=f"허용되지 않는 이미지 형식입니다: {extension}", status_code=400)

        insert_query = text("""
                INSERT INTO af_posts (
                    content, category_id, image_url,
                    like_count, comment_count, user_id, depth,
                    tagging_post_id,
                    created_at, updated_at
                ) VALUES (
                    :content, :category_id, :image_url,
                    0, 0, :user_id, 0,
                    :tagging_post_id,
                    :created_at, :updated_at
                )
            """)

        params = {
            "content": post_create.content,
            "category_id": post_create.category_id,
            "image_url": json.dumps(post_create.image_url) if post_create.image_url else None,
            "user_id": user_id,
            "tagging_post_id": post_create.tagging_post_id,
            "created_at": current_time,
            "updated_at": current_time,
        }

        result = self.db._execute(insert_query, params)
        post_id = result.lastrowid

        if not post_id:
            raise PostException(message="게시글 생성에 실패했습니다", status_code=500, post_id=post_id)

        # 종목 정보 처리
        self._handle_stock_tickers(post_id, post_create.stock_tickers)

        return True, post_id

    def _is_stock_ticker(self, stock_tickers: List[str]) -> bool:
        """종목 코드 유효성 검사"""
        if not stock_tickers:
            return True

        query = """
            SELECT COUNT(DISTINCT ticker)
            FROM stock_information
            WHERE ticker IN :stock_tickers AND is_activate = 1
        """
        result = self.db_data._execute(text(query), {"stock_tickers": stock_tickers})
        count = result.scalar()
        return count == len(stock_tickers)

    def _get_image_format(self, image_url: Optional[str]) -> Optional[str]:
        """이미지 URL에서 확장자 추출"""
        if not image_url:
            return None
        try:
            # URL에서 마지막 부분을 가져옴
            filename = image_url.split("/")[-1]
            # 파일명에서 확장자 추출
            extension = filename.split(".")[-1].lower()
            return extension
        except (IndexError, AttributeError):
            return None

    async def get_post_detail(self, current_user: AlphafinderUser, post_id: int, lang: TranslateCountry) -> ResponsePost:
        """게시글 상세 조회"""
        current_user_id = current_user["uid"] if current_user else None

        # 1. 게시글, 작성자, 카테고리 정보 조회
        query = """
            SELECT
                p.id, p.content, p.image_url, p.image_format, p.like_count, p.comment_count, p.created_at, p.updated_at, p.depth, p.tagging_post_id,
                c.name as category_name,
                p.user_id,
                CASE WHEN :current_user_id IS NOT NULL THEN
                    EXISTS(
                        SELECT 1 FROM bookmarks b
                        WHERE b.post_id = p.id AND b.user_id = :current_user_id
                    )
                ELSE false END as is_bookmarked,
                CASE WHEN :current_user_id IS NOT NULL THEN
                    EXISTS(
                        SELECT 1 FROM post_likes pl
                        WHERE pl.post_id = p.id AND pl.user_id = :current_user_id
                    )
                ELSE false END as is_liked
            FROM af_posts p
            JOIN categories c ON p.category_id = c.id
            WHERE p.id = :post_id
        """

        result = self.db._execute(text(query), {"post_id": post_id, "current_user_id": current_user_id})
        post = result.mappings().first()

        if not post:
            raise PostException(message="게시글을 찾을 수 없습니다", status_code=404, post_id=post_id)

        # 2. 연결된 종목 조회
        stock_tickers = self.db._select(table="post_stocks", columns=["stock_ticker"], post_id=post_id)
        stock_tickers = [row[0] for row in stock_tickers]

        # 종목 정보 조회
        columns = ["ticker", "ctry"]
        if lang == TranslateCountry.KO:
            columns.append("kr_name")
        else:
            columns.append("en_name")
        stock_info = self.db_data._select(table="stock_information", columns=columns, ticker__in=stock_tickers)
        stock_info = [row for row in stock_info]
        stock_information = [
            StockInfo(
                ticker=stock_info[0],
                name=stock_info[2] if lang == TranslateCountry.KO else stock_info[3],
                ctry=stock_info[1],
            )
            for stock_info in stock_info
        ]

        # 3. 태깅된 게시글 정보 조회
        tagging_post_info = None
        if post["tagging_post_id"]:
            tagging_post = self.db._select(
                table="af_posts",
                columns=["id", "content", "created_at", "user_id", "image_url"],
                id=post["tagging_post_id"],
            )
            if tagging_post:
                tagging_user = database_user._select(
                    table="quantus_user", columns=["id", "nickname"], id=tagging_post[0][3]
                )
                if tagging_user:
                    # 태깅된 게시글의 이미지 URL 처리
                    tagging_image_urls = []
                    tagging_image_format = None
                    if tagging_post[0][4]:
                        try:
                            tagging_image_urls = json.loads(tagging_post[0][4])
                            if tagging_image_urls:
                                tagging_image_format = self._get_image_format(tagging_image_urls[0])
                            for i, url in enumerate(tagging_image_urls):
                                presigned_url = self.generate_get_presigned_url(url)
                                tagging_image_urls[i] = presigned_url["get_url"]
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse image_url JSON for tagging post {post['tagging_post_id']}")

                    tagging_post_info = TaggingPostInfo(
                        post_id=tagging_post[0][0],
                        content=tagging_post[0][1],
                        created_at=tagging_post[0][2],
                        user_info=UserInfo(
                            id=tagging_user[0][0],
                            nickname=tagging_user[0][1],
                            profile_image=None,
                            image_format=None,
                        ),
                        image_url=tagging_image_urls,
                        image_format=tagging_image_format,
                    )
                else:
                    # 태깅된 게시글의 작성자가 삭제된 경우
                    tagging_image_urls = []
                    tagging_image_format = None
                    if tagging_post[0][4]:
                        try:
                            tagging_image_urls = json.loads(tagging_post[0][4])
                            if tagging_image_urls:
                                tagging_image_format = self._get_image_format(tagging_image_urls[0])
                            for i, url in enumerate(tagging_image_urls):
                                presigned_url = self.generate_get_presigned_url(url)
                                tagging_image_urls[i] = presigned_url["get_url"]
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse image_url JSON for tagging post {post['tagging_post_id']}")

                    tagging_post_info = TaggingPostInfo(
                        post_id=tagging_post[0][0],
                        content=tagging_post[0][1],
                        created_at=tagging_post[0][2],
                        user_info=UserInfo(
                            id=0,
                            nickname=self._get_unknown_user_nickname(lang),
                            profile_image=None,
                            image_format=None,
                        ),
                        image_url=tagging_image_urls,
                        image_format=tagging_image_format,
                    )
            else:
                # 태깅된 게시글이 삭제된 경우
                tagging_post_info = TaggingPostInfo(
                    post_id=post["tagging_post_id"],
                    content="해당 게시글은 작성자에 의해 삭제되어, 현재 내용을 볼 수 없습니다.",
                    created_at=post["created_at"],
                    user_info=UserInfo(
                        id=0,
                        nickname=self._get_unknown_user_nickname(lang),
                        profile_image=None,
                        image_format=None,
                    ),
                    image_url=None,
                    image_format=None,
                )

        # 4. UserInfo 조회 (user DB에서)
        user_info = None
        if post["user_id"]:
            user_result = database_user._select(table="quantus_user", columns=["id", "nickname"], id=post["user_id"])
            if user_result:
                user = user_result[0]
                user_info = UserInfo(
                    id=user.id,
                    nickname=user.nickname,
                    profile_image=None,
                    image_format=None,
                )

        if not user_info:
            user_info = UserInfo(
                id=0, nickname=self._get_unknown_user_nickname(lang), profile_image=None, image_format=None
            )

        # 5. 이미지 URL 처리
        image_urls = []
        if post["image_url"]:
            try:
                image_urls = json.loads(post["image_url"])
                # 각 이미지 URL에 대해 presigned URL 생성
                for i, url in enumerate(image_urls):
                    presigned_url = self.generate_get_presigned_url(url)
                    image_urls[i] = presigned_url["get_url"]
            except json.JSONDecodeError:
                logger.error(f"Failed to parse image_url JSON for post {post_id}")

        # 6. ResponsePost 객체 생성 및 반환
        response = ResponsePost(
            id=post["id"],
            content=post["content"],
            category_name=post["category_name"],
            image_url=image_urls,
            image_format=post["image_format"],
            like_count=post["like_count"],
            comment_count=post["comment_count"],
            is_changed=post["created_at"] != post["updated_at"],
            is_bookmarked=post["is_bookmarked"],
            is_liked=post["is_liked"],
            is_mine=post["user_id"] == current_user_id,
            created_at=post["created_at"].astimezone(KST),
            depth=post["depth"],
            stock_tickers=stock_information,
            user_info=user_info,
            tagging_post_info=tagging_post_info,
        )

        return response

    def _get_image_url(self, image_urls_json: Optional[str]) -> Optional[List[str]]:
        """이미지 URL JSON에서 모든 이미지 URL을 정렬하여 반환"""
        if not image_urls_json:
            return None

        try:
            image_urls = json.loads(image_urls_json)
            if not image_urls:
                return None

            # 이미지 URL을 숫자로 정렬
            def get_index(url):
                try:
                    # URL에서 마지막 부분을 가져옴
                    filename = url.split("/")[-1]
                    # _숫자.확장자 형식에서 숫자 추출
                    index = int(filename.split("_")[-1].split(".")[0])
                    return index
                except (IndexError, ValueError):
                    return float("inf")

            # 숫자로 정렬
            sorted_urls = sorted(image_urls, key=get_index)
            return sorted_urls
        except json.JSONDecodeError:
            logger.error(f"Failed to parse image_url JSON: {image_urls_json}")
            return None

    async def get_posts(
        self,
        current_user: AlphafinderUser,
        offset: int = 0,
        limit: int = 20,
        category_id: Optional[int] = None,
        stock_ticker: Optional[str] = None,
        lang: TranslateCountry = TranslateCountry.KO,
        order_by: str = "created_at",
    ) -> List[ResponsePost]:
        """게시글 목록 조회"""
        current_user_id = current_user["uid"] if current_user else None
        order_by = order_by.value

        base_query = """
            SELECT p.id, p.content, p.image_url, p.image_format, p.like_count, p.comment_count, p.created_at, p.updated_at, p.depth, p.tagging_post_id,
                c.name as category_name,
                p.user_id,
                CASE WHEN :current_user_id IS NOT NULL THEN
                    EXISTS(
                        SELECT 1 FROM bookmarks b
                        WHERE b.post_id = p.id AND b.user_id = :current_user_id
                    )
                ELSE false END as is_bookmarked,
                CASE WHEN :current_user_id IS NOT NULL THEN
                    EXISTS(
                        SELECT 1 FROM post_likes pl
                        WHERE pl.post_id = p.id AND pl.user_id = :current_user_id
                    )
                ELSE false END as is_liked
            FROM af_posts p
            JOIN categories c ON p.category_id = c.id
            {stock_join}  /* stock_ticker 조건 시 JOIN */
            WHERE p.depth = 0
            {category_condition}  /* category_id 조건 */
            {stock_condition}  /* stock_ticker 조건 */
            ORDER BY p.{order_by} DESC
            LIMIT :limit OFFSET :offset
        """

        conditions = {"stock_join": "", "category_condition": "", "stock_condition": ""}
        params = {"current_user_id": current_user_id, "limit": limit, "offset": offset}

        if category_id:
            conditions["category_condition"] = "AND p.category_id = :category_id"
            params["category_id"] = category_id

        if stock_ticker:
            conditions["stock_join"] = "JOIN post_stocks ps_filter ON p.id = ps_filter.post_id"
            conditions["stock_condition"] = "AND ps_filter.stock_ticker = :stock_ticker"
            params["stock_ticker"] = stock_ticker

        # 쿼리 완성
        query = base_query.format(
            stock_join=conditions["stock_join"],
            category_condition=conditions["category_condition"],
            stock_condition=conditions["stock_condition"],
            order_by=order_by,
        )

        result = self.db._execute(text(query), params)
        posts = result.mappings().all()

        # 2. stock_tickers 정보 조회
        post_ids = [post["id"] for post in posts]
        stock_query = """
            SELECT post_id, stock_ticker
            FROM post_stocks
            WHERE post_id IN :post_ids
        """
        if not post_ids:
            return []
        stock_result = self.db._execute(text(stock_query), {"post_ids": post_ids})

        # post_id별 stock_tickers 매핑
        post_stocks = {}
        for row in stock_result:
            if row[0] not in post_stocks:
                post_stocks[row[0]] = []
            post_stocks[row[0]].append(row[1])

        # 모든 게시글의 티커를 하나의 set으로 모음
        all_tickers = set()
        for post in post_stocks:
            all_tickers.update(post_stocks[post])

        # 종목 정보를 한 번에 조회
        columns = ["ticker", "ctry"]
        if lang == TranslateCountry.KO:
            columns.append("kr_name")
        else:
            columns.append("en_name")

        stock_info = self.db_data._select(table="stock_information", columns=columns, ticker__in=list(all_tickers))

        # 티커를 키로 하는 딕셔너리 생성
        stock_info_map = {
            row[0]: StockInfo(ticker=row[0], name=row[2] if lang == TranslateCountry.KO else row[3], ctry=row[1])
            for row in stock_info
        }

        # 3. 태깅된 게시글 정보 조회
        tagging_post_ids = [post["tagging_post_id"] for post in posts if post["tagging_post_id"]]
        tagging_posts = {}
        if tagging_post_ids:
            tagging_post_query = """
                SELECT id, content, created_at, user_id, image_url
                FROM af_posts
                WHERE id IN :post_ids
            """
            tagging_result = self.db._execute(text(tagging_post_query), {"post_ids": tuple(tagging_post_ids)})
            for row in tagging_result:
                tagging_posts[row[0]] = {
                    "id": row[0],
                    "content": row[1],
                    "created_at": row[2],
                    "user_id": row[3],
                    "image_url": row[4],
                }

        # 태깅된 게시글의 작성자 정보 조회
        tagging_user_ids = [post["user_id"] for post in tagging_posts.values() if post["user_id"]]
        tagging_users = {}
        if tagging_user_ids:
            user_results = database_user._select(
                table="quantus_user", columns=["id", "nickname"], id__in=list(tagging_user_ids)
            )
            for user in user_results:
                tagging_users[user.id] = UserInfo(
                    id=user.id,
                    nickname=user.nickname,
                    profile_image=None,
                    image_format=None,
                )

        # 사용자 정보 조회 (user DB에서)
        user_ids = [post["user_id"] for post in posts if post["user_id"]]
        user_info_map = {}
        if user_ids:
            user_results = database_user._select(table="quantus_user", columns=["id", "nickname"], id__in=user_ids)
            for user in user_results:
                user_info_map[user.id] = UserInfo(
                    id=user.id,
                    nickname=user.nickname,
                    profile_image=None,  # TODO :: 추후 추가해야 함
                    image_format=None,  # TODO :: 추후 추가해야 함
                )

        # 게시글 응답 생성
        response_posts = []
        for post in posts:
            # 태깅된 게시글 정보 처리
            tagging_post_info = None
            if post["tagging_post_id"]:
                if post["tagging_post_id"] in tagging_posts:
                    tagging_post = tagging_posts[post["tagging_post_id"]]
                    user_id = tagging_post["user_id"]

                    # 태깅된 게시글의 이미지 URL 처리
                    tagging_image_urls = []
                    tagging_image_format = None
                    if tagging_post["image_url"]:
                        try:
                            tagging_image_urls = json.loads(tagging_post["image_url"])
                            if tagging_image_urls:
                                tagging_image_format = self._get_image_format(tagging_image_urls[0])
                            for i, url in enumerate(tagging_image_urls):
                                presigned_url = self.generate_get_presigned_url(url)
                                tagging_image_urls[i] = presigned_url["get_url"]
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse image_url JSON for tagging post {post['tagging_post_id']}")

                    if user_id in tagging_users:
                        user = tagging_users[user_id]
                        tagging_post_info = TaggingPostInfo(
                            post_id=tagging_post["id"],
                            content=tagging_post["content"],
                            created_at=tagging_post["created_at"],
                            user_info=UserInfo(
                                id=user["id"],
                                nickname=user["nickname"],
                                profile_image=None,
                                image_format=None,
                            ),
                            image_url=tagging_image_urls,
                            image_format=tagging_image_format,
                        )
                    else:
                        # 태깅된 게시글의 작성자가 삭제된 경우
                        tagging_post_info = TaggingPostInfo(
                            post_id=tagging_post["id"],
                            content=tagging_post["content"],
                            created_at=tagging_post["created_at"],
                            user_info=UserInfo(
                                id=0,
                                nickname=self._get_unknown_user_nickname(lang),
                                profile_image=None,
                                image_format=None,
                            ),
                            image_url=tagging_image_urls,
                            image_format=tagging_image_format,
                        )
                else:
                    # 태깅된 게시글이 삭제된 경우
                    tagging_post_info = TaggingPostInfo(
                        post_id=post["tagging_post_id"],
                        content="해당 게시글은 작성자에 의해 삭제되어, 현재 내용을 볼 수 없습니다.",
                        created_at=post["created_at"],
                        user_info=UserInfo(
                            id=0,
                            nickname=self._get_unknown_user_nickname(lang),
                            profile_image=None,
                            image_format=None,
                        ),
                        image_url=None,
                        image_format=None,
                    )

            # 이미지 URL 처리
            image_urls = []
            if post["image_url"]:
                try:
                    image_urls = json.loads(post["image_url"])
                    for i, url in enumerate(image_urls):
                        presigned_url = self.generate_get_presigned_url(url)
                        image_urls[i] = presigned_url["get_url"]
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse image_url JSON for post {post['id']}")

            response_posts.append(
                ResponsePost(
                    id=post["id"],
                    content=post["content"],
                    category_name=post["category_name"],
                    image_url=image_urls,
                    image_format=post["image_format"],
                    like_count=post["like_count"],
                    comment_count=post["comment_count"],
                    is_changed=post["created_at"] != post["updated_at"],
                    is_bookmarked=post["is_bookmarked"],
                    is_liked=post["is_liked"],
                    is_mine=post["user_id"] == current_user_id,
                    created_at=post["created_at"].astimezone(KST),
                    depth=post["depth"],
                    stock_tickers=[
                        stock_info_map[ticker] for ticker in post_stocks.get(post["id"], []) if ticker in stock_info_map
                    ],
                    user_info=user_info_map.get(
                        post["user_id"],
                        UserInfo(
                            id=0, nickname=self._get_unknown_user_nickname(lang), profile_image=None, image_format=None
                        ),
                    ),
                    tagging_post_info=tagging_post_info,
                )
            )

        return response_posts

    async def update_post(self, current_user: AlphafinderUser, post_id: int, post_update: PostUpdate) -> Tuple[bool, int]:
        """게시글 수정"""
        user_id = current_user["uid"] if current_user else None
        is_stock_ticker = self._is_stock_ticker(post_update.stock_tickers)
        if not is_stock_ticker:
            raise PostException(message="종목 코드가 유효하지 않습니다", status_code=400)

        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        post = self.db._select(table="af_posts", columns=["user_id", "tagging_post_id"], id=post_id, limit=1)
        if not post:
            raise PostException(message="게시글을 찾을 수 없습니다", status_code=404, post_id=post_id)

        if user_id != post[0].user_id:
            raise PostException(message="게시글 수정 권한이 없습니다", status_code=403, post_id=post_id)

        current_time = datetime.now(UTC)
        update_data = {
            "updated_at": current_time,
        }

        # 실제로 전달된 필드만 업데이트
        if post_update.content is not None:
            update_data["content"] = post_update.content
        if post_update.category_id is not None:
            update_data["category_id"] = post_update.category_id
        if post_update.image_url is not None:
            update_data["image_url"] = json.dumps(post_update.image_url) if post_update.image_url else None
        if post_update.tagging_post_id is not None:
            update_data["tagging_post_id"] = post_update.tagging_post_id

        try:
            # 게시글 수정
            result = self.db._update(table="af_posts", sets=update_data, id=post_id)
            if not result.rowcount:
                raise PostException(message="게시글 수정에 실패했습니다", status_code=500, post_id=post_id)

            # 종목 정보 업데이트
            if post_update.stock_tickers is not None:
                if len(post_update.stock_tickers) > 3:
                    raise TooManyStockTickersException()

                # 기존 종목 정보 삭제
                self.db._delete("af_post_stock_tags", post_id=post_id)

                # 새로운 종목 정보 추가
                stock_data = [
                    {
                        "post_id": post_id,
                        "stock_ticker": ticker,
                    }
                    for ticker in post_update.stock_tickers
                ]
                if stock_data:
                    self.db._insert("af_post_stock_tags", stock_data)

            return True, post_id

        except Exception as e:
            logger.exception(f"Failed to update post {post_id}: {str(e)}")
            raise PostException(message="게시글 수정 중 오류가 발생했습니다", status_code=500, post_id=post_id)

    async def _delete_images_from_s3(self, image_urls_json: Optional[str]) -> None:
        """S3에서 이미지 삭제"""
        if not image_urls_json:
            return

        try:
            image_urls = json.loads(image_urls_json)
            for image_key in image_urls:
                try:
                    # S3에서 이미지 삭제
                    await s3_client.delete_object(Bucket=self.s3_bucket, Key=image_key)
                except Exception as e:
                    logger.error(f"Failed to delete image from S3: {image_key}, error: {e}")
        except json.JSONDecodeError:
            logger.error(f"Failed to parse image_url JSON: {image_urls_json}")
        except Exception as e:
            logger.error(f"Failed to delete images from S3: {e}")

    async def delete_content(self, current_user: AlphafinderUser, content_id: int) -> bool:
        """게시글/댓글 삭제"""
        user_id = current_user["uid"] if current_user else None
        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        # 컨텐츠 존재 여부와 작성자 확인
        content = self.db._select(table="af_posts", columns=["user_id", "image_url", "parent_id"], id=content_id, limit=1)
        if not content:
            raise PostException(message="컨텐츠를 찾을 수 없습니다", status_code=404, post_id=content_id)

        if content[0].user_id != user_id:
            raise PostException(message="삭제 권한이 없습니다", status_code=403, post_id=content_id)

        # S3에서 이미지 삭제
        if content[0].image_url:
            await self._delete_images_from_s3(content[0].image_url)

        # 컨텐츠 삭제 (cascade로 종목 정보도 함께 삭제됨)
        result = self.db._delete(table="af_posts", id=content_id)
        if not result.rowcount:
            raise PostException(message="삭제에 실패했습니다", status_code=500, post_id=content_id)

        # parent_id가 있는 경우 (댓글이거나 대댓글인 경우) 부모 게시글의 댓글 수 감소
        if content[0].parent_id:
            update_data = {
                "comment_count__inc": -1,
            }
            self.db._update(table="af_posts", sets=update_data, id=content[0].parent_id)

        return True

    ##################
    ###  댓글 CRUD  ###
    ##################

    async def create_comment(self, current_user: AlphafinderUser, post_id: int, comment_create: CommentCreate) -> bool:
        """댓글 생성"""
        current_time = datetime.now(UTC)
        user_id = current_user["uid"] if current_user else None

        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        # 1. 게시글 존재 여부 확인
        post = self.db._select(table="af_posts", columns=["id", "depth"], id=post_id, limit=1)
        if not post:
            raise PostException(message="게시글을 찾을 수 없습니다", status_code=404, post_id=post_id)

        # 2. 종목 코드 유효성 검사
        if comment_create.stock_tickers:
            is_stock_ticker = self._is_stock_ticker(comment_create.stock_tickers)
            if not is_stock_ticker:
                raise PostException(message="종목 코드가 유효하지 않습니다", status_code=400)

        # 3. 댓글 생성
        comment_data = {
            "content": comment_create.content,
            "like_count": 0,
            "comment_count": 0,
            "depth": post[0].depth + 1,  # 게시글 depth + 1
            "parent_id": post_id,  # 게시글 ID를 parent_id로 사용
            "image_url": json.dumps(comment_create.image_url) if comment_create.image_url else None,
            "tagging_post_id": comment_create.tagging_post_id,
            "user_id": user_id,
            "created_at": current_time,
            "updated_at": current_time,
        }

        result = self.db._insert(table="af_posts", sets=comment_data)
        comment_id = result.lastrowid

        if not comment_id:
            raise PostException(message="댓글 생성에 실패했습니다", status_code=500)

        # 종목 정보 처리
        self._handle_stock_tickers(comment_id, comment_create.stock_tickers)

        # 4. 게시글의 댓글 수 증가
        update_data = {
            "comment_count__inc": 1,  # increment operator 사용
        }
        self.db._update(table="af_posts", sets=update_data, id=post_id)

        return True

    def _get_stock_info_map(self, stock_tickers: Set[str], lang: TranslateCountry) -> Dict[str, StockInfo]:
        """종목 상세 정보 조회"""
        if not stock_tickers:
            return {}

        stock_info_query = """
            SELECT ticker, ctry, kr_name, en_name
            FROM stock_information
            WHERE ticker IN :tickers
        """
        stock_info_result = self.db_data._execute(text(stock_info_query), {"tickers": tuple(stock_tickers)})
        return {
            row[0]: StockInfo(
                ticker=row[0],
                name=row[2] if lang == TranslateCountry.KO else row[3],
                ctry=row[1],
            )
            for row in stock_info_result
        }

    def _get_user_info_map(self, user_ids: Set[int]) -> Dict[int, UserInfo]:
        """사용자 정보 조회"""
        if not user_ids:
            return {}

        user_results = database_user._select(table="quantus_user", columns=["id", "nickname"], id__in=list(user_ids))
        return {
            user.id: UserInfo(
                id=user.id,
                nickname=user.nickname,
                profile_image=None,
                image_format=None,
            )
            for user in user_results
        }

    def _get_tagging_posts(self, tagging_post_ids: Set[int]) -> Dict[int, Dict]:
        """태깅된 게시글 정보 조회"""
        if not tagging_post_ids:
            return {}

        tagging_query = """
            SELECT p.id, p.content, p.created_at, p.user_id, p.image_url
            FROM af_posts p
            WHERE p.id IN :post_ids
        """
        tagging_result = self.db._execute(text(tagging_query), {"post_ids": tuple(tagging_post_ids)})
        return {
            row[0]: {
                "id": row[0],
                "content": row[1],
                "created_at": row[2],
                "user_id": row[3],
                "image_url": row[4],
            }
            for row in tagging_result
        }

    def _create_tagging_post_info(
        self, comment: Dict, tagging_posts: Dict[int, Dict], tagging_users: Dict[int, UserInfo], lang: TranslateCountry
    ) -> Optional[TaggingPostInfo]:
        """태깅된 게시글 정보 생성"""
        if not comment["tagging_post_id"]:
            return None

        tagging_post = tagging_posts.get(comment["tagging_post_id"])
        if not tagging_post:
            return None

        user_info = tagging_users.get(
            tagging_post["user_id"],
            UserInfo(id=0, nickname=self._get_unknown_user_nickname(lang), profile_image=None, image_format=None),
        )

        return TaggingPostInfo(
            post_id=tagging_post["id"],
            content=tagging_post["content"],
            created_at=tagging_post["created_at"].astimezone(KST),
            user_info=user_info,
            image_url=json.loads(tagging_post["image_url"]) if tagging_post["image_url"] else None,
            image_format=None,
        )

    async def get_comments(
        self,
        current_user: Optional[AlphafinderUser],
        post_id: int,
        offset: int = 0,
        limit: int = 20,
        lang: TranslateCountry = TranslateCountry.KO,
    ) -> Tuple[List[CommentItem], bool]:
        """댓글 목록 조회"""
        current_user_id = current_user["uid"] if current_user else None

        # 1. 댓글 조회 (limit + 1개)
        query = """
            SELECT
                p.id, p.content, p.like_count, p.comment_count, p.depth, p.parent_id, p.created_at, p.updated_at,
                p.user_id, p.image_url, p.tagging_post_id,
                CASE WHEN :current_user_id IS NOT NULL THEN
                    EXISTS(
                        SELECT 1 FROM post_likes pl
                        WHERE pl.post_id = p.id AND pl.user_id = :current_user_id
                    )
                ELSE false END as is_liked
            FROM af_posts p
            WHERE p.parent_id = :post_id
            ORDER BY p.created_at ASC
            LIMIT :limit OFFSET :offset
        """

        params = {"post_id": post_id, "limit": limit + 1, "offset": offset, "current_user_id": current_user_id}

        result = self.db._execute(text(query), params)
        comments = result.mappings().all()

        has_more = len(comments) > limit
        if has_more:
            comments = comments[:-1]

        if not comments:
            return [], has_more

        # 2. 종목 정보 조회
        comment_ids = [comment["id"] for comment in comments]
        stock_query = """
            SELECT post_id, GROUP_CONCAT(stock_ticker) as stock_tickers
            FROM af_post_stock_tags
            WHERE post_id IN :comment_ids
            GROUP BY post_id
        """
        stock_result = self.db._execute(text(stock_query), {"comment_ids": tuple(comment_ids)})
        stock_map = {row[0]: row[1].split(",") if row[1] else [] for row in stock_result}

        # 종목 상세 정보 조회
        all_tickers = set()
        for tickers in stock_map.values():
            all_tickers.update(tickers)
        stock_info_map = self._get_stock_info_map(all_tickers, lang)

        # 3. 사용자 정보 조회
        user_ids = {comment["user_id"] for comment in comments if comment["user_id"]}
        user_info_map = self._get_user_info_map(user_ids)

        # 4. 태깅된 게시글 정보 조회
        tagging_post_ids = {comment["tagging_post_id"] for comment in comments if comment["tagging_post_id"]}
        tagging_posts = self._get_tagging_posts(tagging_post_ids)

        # 태깅된 게시글의 작성자 정보 조회
        tagging_user_ids = {post["user_id"] for post in tagging_posts.values() if post["user_id"]}
        tagging_users = self._get_user_info_map(tagging_user_ids)

        # 5. 최종 응답 구성
        comment_list = [
            CommentItem(
                id=comment["id"],
                content=comment["content"],
                like_count=comment["like_count"],
                comment_count=comment["comment_count"],
                depth=comment["depth"],
                parent_id=comment["parent_id"],
                created_at=comment["created_at"].astimezone(KST),
                is_changed=comment["created_at"] != comment["updated_at"],
                is_liked=comment["is_liked"],
                is_mine=comment["user_id"] == current_user_id if current_user_id else False,
                user_info=user_info_map.get(
                    comment["user_id"],
                    UserInfo(id=0, nickname=self._get_unknown_user_nickname(lang), profile_image=None, image_format=None),
                ),
                sub_comments=[],
                image_url=json.loads(comment["image_url"]) if comment["image_url"] else None,
                stock_tickers=[
                    stock_info_map[ticker] for ticker in stock_map.get(comment["id"], []) if ticker in stock_info_map
                ],
                tagging_post_info=self._create_tagging_post_info(comment, tagging_posts, tagging_users, lang),
            )
            for comment in comments
        ]

        return comment_list, has_more

    async def update_comment(self, current_user: AlphafinderUser, comment_id: int, comment_update: CommentUpdate) -> bool:
        """댓글 수정"""
        current_time = datetime.now(UTC)
        user_id = current_user["uid"] if current_user else None

        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        # 1. 댓글 존재 여부와 작성자 확인
        comment = self.db._select(table="af_posts", columns=["id", "user_id", "parent_id"], id=comment_id, limit=1)
        if not comment:
            raise PostException(message="댓글을 찾을 수 없습니다", status_code=404)

        if comment[0].user_id != user_id:
            raise PostException(message="댓글 수정 권한이 없습니다", status_code=403)

        # 2. 종목 코드 유효성 검사
        if comment_update.stock_tickers:
            is_stock_ticker = self._is_stock_ticker(comment_update.stock_tickers)
            if not is_stock_ticker:
                raise PostException(message="종목 코드가 유효하지 않습니다", status_code=400)

        # 3. 댓글 수정
        update_data = {
            "content": comment_update.content,
            "image_url": json.dumps(comment_update.image_url) if comment_update.image_url else None,
            "updated_at": current_time,
        }

        result = self.db._update(table="af_posts", sets=update_data, id=comment_id)
        if not result.rowcount:
            raise PostException(message="댓글 수정에 실패했습니다", status_code=500)

        # 4. 종목 정보 업데이트
        if comment_update.stock_tickers:
            if len(comment_update.stock_tickers) > 3:
                raise TooManyStockTickersException()

            # 기존 종목 정보 삭제
            self.db._delete("af_post_stock_tags", post_id=comment_id)

            # 새로운 종목 정보 추가
            stock_data = [
                {
                    "post_id": comment_id,
                    "stock_ticker": ticker,
                }
                for ticker in comment_update.stock_tickers
            ]
            if stock_data:
                self.db._insert("af_post_stock_tags", stock_data)

        return True

    ###################
    ###  좋아요 CRUD  ###
    ###################

    async def update_post_like(self, current_user: AlphafinderUser, post_id: int, is_liked: bool) -> Tuple[bool, int]:
        """게시글/댓글 좋아요 상태 업데이트"""
        current_time = datetime.now(UTC)
        user_id = current_user["uid"] if current_user else None

        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        # 1. 게시글/댓글 확인
        post = self.db._select(table="af_posts", columns=["id", "like_count"], id=post_id, limit=1)
        if not post:
            raise PostException(message="게시글/댓글을 찾을 수 없습니다", status_code=404)

        current_like_count = post[0].like_count

        # 2. 현재 좋아요 상태 확인
        like_exists = bool(self.db._select(table="af_post_likes", post_id=post_id, user_id=user_id))

        # 3. 상태가 같으면 아무 것도 하지 않음
        if like_exists == is_liked:
            return is_liked, current_like_count

        # 4. 상태가 다르면 업데이트
        if is_liked:
            like_data = {
                "post_id": post_id,
                "user_id": user_id,
                "is_liked": is_liked,
                "created_at": current_time,
                "updated_at": current_time,
            }
            self.db._insert("af_post_likes", like_data)
            new_like_count = current_like_count + 1
        else:
            self.db._delete(table="af_post_likes", post_id=post_id, user_id=user_id)
            new_like_count = current_like_count - 1

        # 5. 게시글/댓글 좋아요 수 업데이트
        self.db._update(table="af_posts", sets={"like_count": new_like_count}, id=post_id)

        return is_liked, new_like_count

    ###################
    ###  북마크 CRUD  ###
    ###################

    async def update_post_bookmark(
        self, current_user: AlphafinderUser, post_id: int, is_bookmarked: bool
    ) -> Tuple[bool, int]:
        """게시글 북마크 상태 업데이트"""
        current_time = datetime.now(UTC)
        user_id = current_user["uid"] if current_user else None

        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        # 1. 게시글 확인
        exists_query = text("""
            SELECT EXISTS (
                SELECT 1 FROM posts WHERE id = :post_id
            ) as exists_flag
        """)
        result = self.db._execute(exists_query, {"post_id": post_id})
        post_exists = bool(result.scalar())

        if not post_exists:
            raise PostException(message="게시글을 찾을 수 없습니다", status_code=404, post_id=post_id)

        # 2. 현재 북마크 상태 확인
        bookmark_exists = bool(self.db._select(table="bookmarks", post_id=post_id, user_id=user_id))

        # 3. 상태가 같으면 아무 것도 하지 않음
        if bookmark_exists == is_bookmarked:
            return is_bookmarked

        # 4. 상태가 다르면 업데이트
        if is_bookmarked:
            bookmark_data = {
                "post_id": post_id,
                "user_id": user_id,
                "created_at": current_time,
                "updated_at": current_time,
            }
            self.db._insert("bookmarks", bookmark_data)
        else:
            self.db._delete(table="bookmarks", post_id=post_id, user_id=user_id)

        return is_bookmarked

    async def get_trending_posts(
        self,
        limit: int = 10,
    ) -> List[TrendingPostResponse]:
        """실시간 인기 게시글 조회 (30일)"""
        query = """
            WITH post_likes_count AS (
                SELECT post_id, COUNT(*) as daily_likes
                FROM af_post_likes
                WHERE created_at >= UTC_TIMESTAMP() - INTERVAL 30 DAY
                GROUP BY post_id
            )
            SELECT
                p.id, p.created_at, p.user_id,
                ROW_NUMBER() OVER (ORDER BY COALESCE(plc.daily_likes, 0) DESC, p.created_at DESC) as rank_num
            FROM af_posts p
            LEFT JOIN post_likes_count plc ON p.id = plc.post_id
            ORDER BY COALESCE(plc.daily_likes, 0) DESC, p.created_at DESC
            LIMIT :limit
        """

        result = self.db._execute(text(query), {"limit": limit})
        posts = result.mappings().all()

        # 사용자 정보 조회 (user DB에서)
        user_ids = [post["user_id"] for post in posts if post["user_id"]]
        user_info_map = {}
        if user_ids:
            user_results = database_user._select(table="quantus_user", columns=["id", "nickname"], id__in=user_ids)
            for user in user_results:
                user_info_map[user.id] = UserInfo(
                    id=user.id,
                    nickname=user.nickname,
                    profile_image=None,
                    image_format=None,  # TODO :: 추후 추가해야 함
                )

        return [
            TrendingPostResponse(
                id=post["id"],
                rank=post["rank_num"],
                created_at=post["created_at"].astimezone(KST),
                user_info=user_info_map.get(
                    post["user_id"], UserInfo(id=0, nickname="(알 수 없는 유저)", profile_image=None, image_format=None)
                ),
            )
            for post in posts
        ]

    # async def get_trending_stocks(
    #     self, limit: int = 5, lang: TranslateCountry = TranslateCountry.KO
    # ) -> List[TrendingStockResponse]:
    #     """실시간 인기 종목 조회 (24시간)"""
    #     name_field = "si.kr_name" if lang == TranslateCountry.KO else "si.en_name"

    #     query = f"""
    #         WITH recent_stock_mentions AS (
    #             SELECT ps.stock_ticker, COUNT(*) as mentions_count
    #             FROM posts p
    #             JOIN post_stocks ps ON p.id = ps.post_id
    #             WHERE p.created_at >= UTC_TIMESTAMP() - INTERVAL 24 HOUR
    #             GROUP BY ps.stock_ticker
    #         )
    #         SELECT
    #             ROW_NUMBER() OVER (ORDER BY rsm.mention_count DESC) as rank_num,
    #             rsm.stock_ticker as ticker,
    #             {name_field} as name,
    #             si.ctry as ctry,
    #             rsm.mention_count
    #         FROM recent_stock_mentions rsm
    #         JOIN stock_information si ON rsm.stock_ticker = si.ticker
    #         ORDER BY rsm.mention_count DESC, ticker ASC
    #         LIMIT :limit
    #     """

    #     result = self.db._execute(text(query), {"limit": limit})
    #     stocks = result.mappings().all()

    #     return [
    #         TrendingStockResponse(rank=stock["rank_num"], ticker=stock["ticker"], name=stock["name"], ctry=stock["ctry"])
    #         for stock in stocks
    #     ]

    async def get_categories(self) -> List[CategoryResponse]:
        """카테고리 리스트 조회"""
        categories = self.db._select(table="categories", columns=["id", "name"])
        return [CategoryResponse(id=category[0], name=category[1]) for category in categories]

    def _get_unknown_user_nickname(self, lang: TranslateCountry) -> str:
        """언어에 따른 알 수 없는 사용자 닉네임 반환"""
        return UNKNOWN_USER_KO if lang == TranslateCountry.KO else UNKNOWN_USER_EN

    def _get_extension_from_content_type(self, content_type: str) -> str:
        """Content-Type에서 확장자 추출"""
        extension = self.ALLOWED_CONTENT_TYPES.get(content_type.lower())
        if not extension:
            raise PostException(
                message=f"허용되지 않는 Content-Type입니다. 허용되는 형식: {', '.join(self.ALLOWED_CONTENT_TYPES.keys())}",
                status_code=400,
            )
        return extension

    def _generate_image_key(self, extension: str, index: int) -> str:
        """이미지 키 생성"""
        now = datetime.now()
        date_path = now.strftime("%Y/%m/%d")
        unique_id = str(uuid.uuid4())
        return f"community/{date_path}/{unique_id}_{index}.{extension}"

    def _get_cached_presigned_url(self, image_key: str, url_type: str) -> Optional[dict]:
        """Redis에서 캐시된 presigned URL 조회"""
        cached_data = self.redis.get(f"presigned_url:{url_type}:{image_key}")
        if cached_data:
            return json.loads(cached_data)
        return None

    def _cache_presigned_url(self, image_key: str, presigned_data: dict, url_type: str) -> None:
        """Redis에 presigned URL 캐시"""
        self.redis.setex(f"presigned_url:{url_type}:{image_key}", self.REDIS_CACHE_EXPIRES_IN, json.dumps(presigned_data))

    def generate_upload_presigned_url(self, content_type: str, file_size: int, index: int = 0) -> dict:
        """S3 업로드용 presigned URL 생성"""
        if file_size > self.MAX_FILE_SIZE:
            raise PostException(
                message=f"파일 크기가 너무 큽니다. 최대 크기: {self.MAX_FILE_SIZE / (1024 * 1024)}MB", status_code=400
            )

        # Content-Type에서 확장자 추출
        extension = self._get_extension_from_content_type(content_type)
        image_key = self._generate_image_key(extension, index)

        # PUT presigned URL 생성 (업로드용)
        presigned_post = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": self.s3_bucket,
                "Key": image_key,
                "ContentType": content_type,
            },
            ExpiresIn=self.PRESIGNED_URL_EXPIRES_IN,
        )
        logger.info(f"upload_url: {presigned_post}")
        print(f"upload_url: {presigned_post}")

        presigned_data = {
            "upload_url": presigned_post,
            "image_key": image_key,
        }

        return presigned_data

    def generate_get_presigned_url(self, image_key: str) -> dict:
        """S3 조회용 presigned URL 생성"""
        # Redis에서 캐시된 URL 확인
        cached_data = self._get_cached_presigned_url(image_key, "get")
        if cached_data:
            return cached_data

        # GET presigned URL 생성 (조회용)
        get_url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.s3_bucket,
                "Key": image_key,
            },
            ExpiresIn=self.PRESIGNED_URL_EXPIRES_IN,
        )

        presigned_data = {
            "get_url": get_url,
            "image_key": image_key,
            "expires_in": self.PRESIGNED_URL_EXPIRES_IN,
        }

        # Redis에 캐시
        self._cache_presigned_url(image_key, presigned_data, "get")

        return presigned_data


def get_community_service() -> CommunityService:
    return CommunityService()
