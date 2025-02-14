from datetime import datetime

from sqlalchemy import text
from app.common.constants import UTC
from app.core.exception.custom import PostException, TooManyStockTickersException
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from .schemas import (
    CategoryResponse,
    CommentCreate,
    CommentItem,
    CommentUpdate,
    PostCreate,
    PostUpdate,
    ResponsePost,
    StockInfo,
    TrendingPostResponse,
    TrendingStockResponse,
    UserInfo,
)
from typing import List, Optional, Tuple
from app.database.crud import database


class CommunityService:
    def __init__(self):
        self.db = database

    async def create_post(self, current_user: AlphafinderUser, post_create: PostCreate) -> Tuple[bool, int]:
        """게시글 생성"""
        current_time = datetime.now(UTC)
        is_stock_ticker = self._is_stock_ticker(post_create.stock_tickers)
        if not is_stock_ticker:
            raise PostException(message="종목 코드가 유효하지 않습니다", status_code=400)

        user_id = current_user[0]

        insert_query = text("""
                INSERT INTO posts (
                    title, content, category_id, image_url,
                    like_count, comment_count, user_id,
                    created_at, updated_at
                ) VALUES (
                    :title, :content, :category_id, :image_url,
                    0, 0, :user_id,
                    :created_at, :updated_at
                )
            """)

        params = {
            "title": post_create.title,
            "content": post_create.content,
            "category_id": post_create.category_id,
            "image_url": post_create.image_url,
            "user_id": user_id,
            "created_at": current_time,
            "updated_at": current_time,
        }

        result = self.db._execute(insert_query, params)
        post_id = result.lastrowid

        if not post_id:
            raise PostException(message="게시글 생성에 실패했습니다", status_code=500, post_id=post_id)

        if post_create.stock_tickers:
            if len(post_create.stock_tickers) > 3:
                raise TooManyStockTickersException()

            stock_data = [
                {
                    "post_id": post_id,
                    "stock_ticker": ticker,
                }
                for ticker in post_create.stock_tickers
            ]
            self.db._insert("post_stocks", stock_data)

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
        result = self.db._execute(text(query), {"stock_tickers": stock_tickers})
        count = result.scalar()
        return count == len(stock_tickers)

    async def get_post_detail(self, current_user: AlphafinderUser, post_id: int, lang: TranslateCountry) -> ResponsePost:
        """게시글 상세 조회"""
        current_user_id = current_user[0] if current_user else None

        # 1. 게시글, 작성자, 카테고리 정보 조회
        query = """
            SELECT
                p.id, p.title, p.content, p.image_url, p.like_count, p.comment_count, p.created_at, p.updated_at,
                c.name as category_name,
                u.id as user_id, u.nickname, u.profile_image,
                CASE WHEN :current_user_id IS NOT NULL THEN
                    EXISTS(
                        SELECT 1 FROM bookmarks b
                        WHERE b.post_id = p.id AND b.user_id = :current_user_id
                    )
                ELSE false END as is_bookmarked
            FROM posts p
            JOIN categories c ON p.category_id = c.id
            LEFT JOIN alphafinder_user u ON p.user_id = u.id
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
        stock_info = self.db._select(table="stock_information", columns=columns, ticker__in=stock_tickers)
        stock_info = [row for row in stock_info]
        stock_information = [
            StockInfo(
                ticker=stock_info[0],
                name=stock_info[2] if lang == TranslateCountry.KO else stock_info[3],
                ctry=stock_info[1],
            )
            for stock_info in stock_info
        ]

        # 3. UserInfo 객체 생성
        user_info = (
            UserInfo(id=post["user_id"], nickname=post["nickname"], profile_image=post.get("profile_image"))
            if post["nickname"]
            else UserInfo(id=0, nickname="(알 수 없는 유저)", profile_image=None)
        )

        # 4. ResponsePost 객체 생성 및 반환
        response = ResponsePost(
            id=post["id"],
            title=post["title"],
            content=post["content"],
            category_name=post["category_name"],
            image_url=post["image_url"],
            like_count=post["like_count"],
            comment_count=post["comment_count"],
            is_changed=post["created_at"] != post["updated_at"],
            is_bookmarked=post["is_bookmarked"],
            created_at=post["created_at"],
            stock_tickers=stock_information,
            user_info=user_info,
        )

        return response

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
        current_user_id = current_user[0] if current_user else None
        order_by = order_by.value

        base_query = """
            WITH post_data AS (
                SELECT DISTINCT p.id, p.title, p.content, p.image_url, p.like_count, p.comment_count, p.created_at, p.updated_at,
                    c.name as category_name,
                    u.id as user_id, u.nickname, u.profile_image,
                    CASE WHEN :current_user_id IS NOT NULL THEN
                        EXISTS(
                            SELECT 1 FROM bookmarks b
                            WHERE b.post_id = p.id AND b.user_id = :current_user_id
                        )
                    ELSE false END as is_bookmarked
                FROM posts p
                JOIN categories c ON p.category_id = c.id
                LEFT JOIN alphafinder_user u ON p.user_id = u.id
                {stock_join}  /* stock_ticker 조건 시 JOIN */
                WHERE 1=1
                {category_condition}  /* category_id 조건 */
                {stock_condition}  /* stock_ticker 조건 */
                ORDER BY p.{order_by} DESC
                LIMIT :limit OFFSET :offset
            )
            SELECT pd.*, GROUP_CONCAT(ps.stock_ticker) as stock_tickers
            FROM post_data pd
            LEFT JOIN post_stocks ps ON pd.id = ps.post_id
            GROUP BY pd.id, pd.title, pd.content, pd.image_url,
                    pd.like_count, pd.comment_count, pd.created_at, pd.updated_at,
                    pd.category_name, pd.user_id, pd.nickname, pd.profile_image,
                    pd.is_bookmarked
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

        # 모든 게시글의 티커를 하나의 set으로 모음
        all_tickers = set()
        for post in posts:
            if post["stock_tickers"]:
                all_tickers.update(post["stock_tickers"])

        # 종목 정보를 한 번에 조회
        columns = ["ticker", "ctry"]
        if lang == TranslateCountry.KO:
            columns.append("kr_name")
        else:
            columns.append("en_name")

        stock_info = self.db._select(table="stock_information", columns=columns, ticker__in=list(all_tickers))

        # 티커를 키로 하는 딕셔너리 생성
        stock_info_map = {
            row[0]: StockInfo(ticker=row[0], name=row[2] if lang == TranslateCountry.KO else row[2], ctry=row[1])
            for row in stock_info
        }

        # 게시글 응답 생성
        return [
            ResponsePost(
                id=post["id"],
                title=post["title"],
                content=post["content"],
                category_name=post["category_name"],
                image_url=post["image_url"],
                like_count=post["like_count"],
                comment_count=post["comment_count"],
                is_changed=post["created_at"] != post["updated_at"],
                is_bookmarked=post["is_bookmarked"],
                created_at=post["created_at"],
                stock_tickers=[
                    stock_info_map[ticker] for ticker in (post["stock_tickers"] or []) if ticker in stock_info_map
                ],
                user_info=(
                    UserInfo(id=post["user_id"], nickname=post["nickname"], profile_image=post.get("profile_image"))
                    if post["nickname"]
                    else UserInfo(id=0, nickname="(알 수 없는 유저)", profile_image=None)
                ),
            )
            for post in posts
        ]

    async def update_post(self, current_user: AlphafinderUser, post_id: int, post_update: PostUpdate) -> Tuple[bool, int]:
        """게시글 수정"""
        user_id = current_user[0] if current_user else None
        is_stock_ticker = self._is_stock_ticker(post_update.stock_tickers)
        if not is_stock_ticker:
            raise PostException(message="종목 코드가 유효하지 않습니다", status_code=400)

        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        post_user_id = self.db._select(table="posts", columns=["user_id"], id=post_id)
        print(f"post_user_id: {post_user_id}###1")
        if not post_user_id:
            raise PostException(message="게시글을 찾을 수 없습니다", status_code=404, post_id=post_id)

        post_user_id = post_user_id[0][0]

        if user_id != post_user_id:
            raise PostException(message="게시글 수정 권한이 없습니다", status_code=403, post_id=post_id)

        current_time = datetime.now(UTC)
        update_date = {
            "title": post_update.title,
            "content": post_update.content,
            "category_id": post_update.category_id,
            "image_url": post_update.image_url,
            "updated_at": current_time,
        }
        result = self.db._update(table="posts", sets=update_date, id=post_id)

        if not result.rowcount:
            raise PostException(message="게시글 수정에 실패했습니다", status_code=500, post_id=post_id)

        if post_update.stock_tickers:
            if len(post_update.stock_tickers) > 3:
                raise TooManyStockTickersException()

            stock_data = [
                {
                    "post_id": post_id,
                    "stock_ticker": ticker,
                }
                for ticker in post_update.stock_tickers
            ]
            self.db._delete("post_stocks", post_id=post_id)
            self.db._insert("post_stocks", stock_data)

        return True, post_id

    async def delete_post(self, current_user: AlphafinderUser, post_id: int) -> bool:
        """게시글 삭제"""
        user_id = current_user[0] if current_user else None
        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        post_user_id = self.db._select(table="posts", columns=["user_id"], id=post_id)
        if not post_user_id:
            raise PostException(message="게시글을 찾을 수 없습니다", status_code=404, post_id=post_id)

        post_user_id = post_user_id[0][0]

        if user_id != post_user_id:
            raise PostException(message="게시글 삭제 권한이 없습니다", status_code=403, post_id=post_id)

        result = self.db._delete(table="posts", id=post_id)

        if not result.rowcount:
            raise PostException(message="게시글 삭제에 실패했습니다", status_code=500, post_id=post_id)

        return True

    ##################
    ###  댓글 CRUD  ###
    ##################

    async def create_comment(self, current_user: AlphafinderUser, post_id: int, comment_create: CommentCreate) -> bool:
        """댓글 생성"""
        current_time = datetime.now(UTC)
        user_id = current_user[0] if current_user else None

        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        # 1. 게시글 존재 여부 확인
        exists_query = text("""
            SELECT EXISTS (
                SELECT 1 FROM posts WHERE id = :post_id
            ) as exists_flag
        """)
        result = self.db._execute(exists_query, {"post_id": post_id})
        post_exists = bool(result.scalar())

        if not post_exists:
            raise PostException(message="게시글을 찾을 수 없습니다", status_code=404, post_id=post_id)

        # 2. 부모 댓글 확인 (대댓글인 경우)
        if comment_create.parent_id:
            parent_comment = self.db._select(
                table="comments",
                columns=["id", "depth"],
                id=comment_create.parent_id,
                post_id=post_id,  # 같은 게시글의 댓글인지 확인
            )
            if not parent_comment:
                raise PostException(
                    message="원댓글을 찾을 수 없습니다", status_code=404, comment_id=comment_create.parent_id
                )

            # 대댓글의 depth는 1
            if parent_comment[0][1] > 0:
                raise PostException(message="대댓글에는 답글을 달 수 없습니다", status_code=400)

        # 3. 댓글 생성
        comment_data = {
            "content": comment_create.content,
            "like_count": 0,
            "depth": 1 if comment_create.parent_id else 0,
            "parent_id": comment_create.parent_id if comment_create.parent_id else None,
            "post_id": post_id,
            "user_id": user_id,
            "created_at": current_time,
            "updated_at": current_time,
        }

        result = self.db._insert(table="comments", sets=comment_data)

        if not result.rowcount:
            raise PostException(message="댓글 생성에 실패했습니다", status_code=500)

        # 4. 게시글의 댓글 수 증가
        update_data = {
            "comment_count__inc": 1,  # increment operator 사용
        }
        self.db._update(table="posts", sets=update_data, id=post_id)

        return True

    async def get_comments(
        self, current_user: Optional[AlphafinderUser], post_id: int, offset: int = 0, limit: int = 20
    ) -> Tuple[List[CommentItem], bool]:
        """댓글 목록 조회"""
        current_user_id = current_user[0] if current_user else None

        # 1. 원댓글 조회 (limit + 1개)
        parent_query = """
            SELECT
                c.id, c.content, c.like_count, c.depth, c.parent_id, c.created_at, c.updated_at,
                u.id as user_id, u.nickname, u.profile_image,
                CASE WHEN :current_user_id IS NOT NULL THEN
                    EXISTS(
                        SELECT 1 FROM comment_likes cl
                        WHERE cl.comment_id = c.id AND cl.user_id = :current_user_id
                    )
                ELSE false END as is_liked
            FROM comments c
            LEFT JOIN alphafinder_user u ON c.user_id = u.id
            WHERE c.post_id = :post_id
            AND c.depth = 0
            ORDER BY c.created_at ASC
            LIMIT :limit OFFSET :offset
        """

        parent_params = {"post_id": post_id, "limit": limit + 1, "offset": offset, "current_user_id": current_user_id}

        parent_result = self.db._execute(text(parent_query), parent_params)
        parent_comments = parent_result.mappings().all()

        has_more = len(parent_comments) > limit
        if has_more:
            parent_comments = parent_comments[:-1]

        if not parent_comments:
            return [], has_more

        # 2. 대댓글 조회 (원댓글 ID 기준)
        parent_ids = [comment["id"] for comment in parent_comments]
        child_query = """
            SELECT
                c.id, c.content, c.like_count, c.depth, c.parent_id, c.created_at, c.updated_at,
                u.id as user_id, u.nickname, u.profile_image,
                CASE WHEN :current_user_id IS NOT NULL THEN
                    EXISTS(
                        SELECT 1 FROM comment_likes cl
                        WHERE cl.comment_id = c.id AND cl.user_id = :current_user_id
                    )
                ELSE false END as is_liked
            FROM comments c
            LEFT JOIN alphafinder_user u ON c.user_id = u.id
            WHERE c.parent_id IN :parent_ids
            ORDER BY c.created_at ASC
        """

        child_result = self.db._execute(text(child_query), {"parent_ids": parent_ids, "current_user_id": current_user_id})
        child_comments = child_result.mappings().all()

        def create_user_info(comment):
            """사용자 정보 생성 (탈퇴한 사용자 처리)"""
            if comment["user_id"] and comment["nickname"]:
                return UserInfo(
                    id=comment["user_id"], nickname=comment["nickname"], profile_image=comment.get("profile_image")
                )
            return UserInfo(
                id=0,
                nickname="(알 수 없는 유저)",
                profile_image=None,
            )

        # 3. 대댓글을 원댓글의 sub_comments에 매핑
        child_map = {}
        for child in child_comments:
            parent_id = child["parent_id"]
            if parent_id not in child_map:
                child_map[parent_id] = []
            child_map[parent_id].append(
                CommentItem(
                    id=child["id"],
                    content=child["content"],
                    like_count=child["like_count"],
                    depth=child["depth"],
                    parent_id=child["parent_id"],
                    created_at=child["created_at"],
                    is_changed=child["created_at"] != child["updated_at"],
                    is_liked=child["is_liked"],
                    is_mine=child["user_id"] == current_user_id if current_user_id else False,
                    user_info=create_user_info(child),
                    sub_comments=[],
                )
            )

        # 4. 최종 응답 구성
        comment_list = [
            CommentItem(
                id=comment["id"],
                content=comment["content"],
                like_count=comment["like_count"],
                depth=comment["depth"],
                parent_id=comment["parent_id"],
                created_at=comment["created_at"],
                is_changed=comment["created_at"] != comment["updated_at"],
                is_liked=comment["is_liked"],
                is_mine=comment["user_id"] == current_user_id if current_user_id else False,
                user_info=create_user_info(comment),
                sub_comments=child_map.get(comment["id"], []),
            )
            for comment in parent_comments
        ]

        return comment_list, has_more

    async def update_comment(self, current_user: AlphafinderUser, comment_id: int, comment_update: CommentUpdate) -> bool:
        """댓글 수정"""
        current_time = datetime.now(UTC)
        user_id = current_user[0] if current_user else None

        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        # 1. 댓글 존재 여부와 작성자 확인
        comment = self.db._select(table="comments", columns=["id", "user_id"], id=comment_id)

        if not comment:
            raise PostException(message="댓글을 찾을 수 없습니다", status_code=404)

        if comment[0][1] != user_id:
            raise PostException(message="댓글 수정 권한이 없습니다", status_code=403)

        # 2. 댓글 수정
        update_data = {"content": comment_update.content, "updated_at": current_time}

        result = self.db._update(table="comments", sets=update_data, id=comment_id)

        if not result.rowcount:
            raise PostException(message="댓글 수정에 실패했습니다", status_code=500)

        return True

    async def delete_comment(self, current_user: AlphafinderUser, comment_id: int) -> bool:
        """댓글 삭제"""
        user_id = current_user[0] if current_user else None

        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        # 1. 댓글 존재 여부와 작성자 확인
        comment = self.db._select(table="comments", columns=["id", "user_id", "post_id", "parent_id"], id=comment_id)

        if not comment:
            raise PostException(message="댓글을 찾을 수 없습니다", status_code=404)

        if comment[0][1] != user_id:
            raise PostException(message="댓글 삭제 권한이 없습니다", status_code=403)

        post_id = comment[0][2]

        # 2. 댓글 삭제 (cascade로 하위 댓글과 좋아요도 함께 삭제)
        result = self.db._delete(table="comments", id=comment_id)

        if not result.rowcount:
            raise PostException(message="댓글 삭제에 실패했습니다", status_code=500)

        # 3. 게시글의 댓글 수 감소
        if not comment[0][3]:  # 원댓글인 경우만 카운트 감소
            update_data = {
                "comment_count__inc": -1,
            }
            self.db._update(table="posts", sets=update_data, id=post_id)

        return True

    ###################
    ###  좋아요 CRUD  ###
    ###################

    async def update_post_like(self, current_user: AlphafinderUser, post_id: int, is_liked: bool) -> Tuple[bool, int]:
        """게시글 좋아요 상태 업데이트"""
        current_time = datetime.now(UTC)
        user_id = current_user[0] if current_user else None

        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        # 1. 게시글 확인
        post = self.db._select(table="posts", columns=["id", "like_count"], id=post_id)

        if not post:
            raise PostException(message="게시글을 찾을 수 없습니다", status_code=404, post_id=post_id)

        current_like_count = post[0][1]

        # 2. 현재 좋아요 상태 확인
        like_exists = bool(self.db._select(table="post_likes", post_id=post_id, user_id=user_id))

        # 3. 상태가 같으면 아무 것도 하지 않음
        if like_exists == is_liked:
            return is_liked, current_like_count

        # 4. 상태가 다르면 업데이트
        if is_liked:
            like_data = {"post_id": post_id, "user_id": user_id, "created_at": current_time, "updated_at": current_time}
            self.db._insert("post_likes", like_data)
            new_like_count = current_like_count + 1
        else:
            self.db._delete(table="post_likes", post_id=post_id, user_id=user_id)
            new_like_count = current_like_count - 1

        # 5. 게시글 좋아요 수 업데이트
        self.db._update(table="posts", sets={"like_count": new_like_count}, id=post_id)

        return is_liked, new_like_count

    async def update_comment_like(
        self, current_user: AlphafinderUser, comment_id: int, is_liked: bool
    ) -> Tuple[bool, int]:
        """댓글 좋아요 상태 업데이트"""
        current_time = datetime.now(UTC)
        user_id = current_user[0] if current_user else None

        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        # 1. 댓글 확인
        comment = self.db._select(table="comments", columns=["id", "like_count"], id=comment_id)

        if not comment:
            raise PostException(message="댓글을 찾을 수 없습니다", status_code=404, comment_id=comment_id)

        current_like_count = comment[0][1]

        # 2. 현재 좋아요 상태 확인
        like_exists = bool(self.db._select(table="comment_likes", comment_id=comment_id, user_id=user_id))

        # 3. 상태가 같으면 아무 것도 하지 않음
        if like_exists == is_liked:
            return is_liked, current_like_count

        # 4. 상태가 다르면 업데이트
        if is_liked:
            like_data = {
                "comment_id": comment_id,
                "user_id": user_id,
                "created_at": current_time,
                "updated_at": current_time,
            }
            self.db._insert("comment_likes", like_data)
            new_like_count = current_like_count + 1
        else:
            self.db._delete(table="comment_likes", comment_id=comment_id, user_id=user_id)
            new_like_count = current_like_count - 1

        # 5. 게시글 좋아요 수 업데이트
        self.db._update(table="comments", sets={"like_count": new_like_count}, id=comment_id)

        return is_liked, new_like_count

    ###################
    ###  북마크 CRUD  ###
    ###################

    async def update_post_bookmark(
        self, current_user: AlphafinderUser, post_id: int, is_bookmarked: bool
    ) -> Tuple[bool, int]:
        """게시글 북마크 상태 업데이트"""
        current_time = datetime.now(UTC)
        user_id = current_user[0] if current_user else None

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
        limit: int = 5,
    ) -> List[TrendingPostResponse]:
        """실시간 인기 게시글 조회 (24시간)"""
        query = """
            WITH post_likes_count AS (
                SELECT post_id, COUNT(*) as daily_likes
                FROM post_likes
                WHERE created_at >= UTC_TIMESTAMP() - INTERVAL 24 HOUR
                GROUP BY post_id
            )
            SELECT
                p.id, p.title, p.created_at,
                ROW_NUMBER() OVER (ORDER BY COALESCE(plc.daily_likes, 0) DESC, p.created_at DESC) as rank_num,
                u.id as user_id, u.nickname, u.profile_image
            FROM posts p
            LEFT JOIN post_likes_count plc ON p.id = plc.post_id
            LEFT JOIN alphafinder_user u ON p.user_id = u.id
            ORDER BY COALESCE(plc.daily_likes, 0) DESC, p.created_at DESC
            LIMIT :limit
        """

        result = self.db._execute(text(query), {"limit": limit})
        posts = result.mappings().all()

        return [
            TrendingPostResponse(
                id=post["id"],
                rank=post["rank_num"],
                title=post["title"],
                created_at=post["created_at"],
                user_info=UserInfo(
                    id=post["user_id"] if post["user_id"] else 0,
                    nickname=post["nickname"] if post["nickname"] else "(알 수 없는 유저)",
                    profile_image=post.get("profile_image"),
                ),
            )
            for post in posts
        ]

    async def get_trending_stocks(
        self, limit: int = 5, lang: TranslateCountry = TranslateCountry.KO
    ) -> List[TrendingStockResponse]:
        """실시간 인기 종목 조회 (24시간)"""
        name_field = "si.kr_name" if lang == TranslateCountry.KO else "si.en_name"

        query = f"""
            WITH recent_stock_mentions AS (
                SELECT ps.stock_ticker, COUNT(*) as mention_count
                FROM posts p
                JOIN post_stocks ps ON p.id = ps.post_id
                WHERE p.created_at >= UTC_TIMESTAMP() - INTERVAL 24 HOUR
                GROUP BY ps.stock_ticker
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY rsm.mention_count DESC) as rank_num,
                rsm.stock_ticker as ticker,
                {name_field} as name,
                si.ctry as ctry,
                rsm.mention_count
            FROM recent_stock_mentions rsm
            JOIN stock_information si ON rsm.stock_ticker = si.ticker
            ORDER BY rsm.mention_count DESC, ticker ASC
            LIMIT :limit
        """

        result = self.db._execute(text(query), {"limit": limit})
        stocks = result.mappings().all()

        return [
            TrendingStockResponse(rank=stock["rank_num"], ticker=stock["ticker"], name=stock["name"], ctry=stock["ctry"])
            for stock in stocks
        ]

    async def get_categories(self) -> List[CategoryResponse]:
        """카테고리 리스트 조회"""
        categories = self.db._select(table="categories", columns=["id", "name"])
        return [CategoryResponse(id=category[0], name=category[1]) for category in categories]


def get_community_service() -> CommunityService:
    return CommunityService()
