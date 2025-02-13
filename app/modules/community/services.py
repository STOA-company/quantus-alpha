from datetime import datetime

from sqlalchemy import text
from app.common.constants import UTC
from app.core.exception.custom import PostException, TooManyStockTickersException
from app.models.models_users import AlphafinderUser
from .schemas import CommentCreate, CommentItem, CommentUpdate, PostCreate, PostUpdate, ResponsePost, UserInfo
from typing import List, Optional
from app.database.crud import database


class CommunityService:
    def __init__(self):
        self.db = database

    async def create_post(self, current_user: AlphafinderUser, post_create: PostCreate) -> bool:
        """게시글 생성"""
        current_time = datetime.now(UTC)
        user_id = current_user[0][0]

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

        return True

    async def get_post_detail(self, current_user: AlphafinderUser, post_id: int) -> ResponsePost:
        """게시글 상세 조회"""
        current_user_id = current_user[0][0] if current_user else None

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
            stock_tickers=stock_tickers,
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
        order_by: str = "created_at",
    ) -> List[ResponsePost]:
        """게시글 목록 조회"""
        current_user_id = current_user[0][0] if current_user else None
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
                stock_tickers=(post["stock_tickers"]).split(",") if post["stock_tickers"] else [],  # None 값 제거
                user_info=(
                    UserInfo(id=post["user_id"], nickname=post["nickname"], profile_image=post.get("profile_image"))
                    if post["nickname"]
                    else UserInfo(id=0, nickname="(알 수 없는 유저)", profile_image=None)
                ),
            )
            for post in posts
        ]

    async def update_post(self, current_user: AlphafinderUser, post_id: int, post_update: PostUpdate) -> bool:
        """게시글 수정"""
        user_id = current_user[0][0] if current_user else None
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

        return True

    async def delete_post(self, current_user: AlphafinderUser, post_id: int) -> bool:
        """게시글 삭제"""
        user_id = current_user[0][0] if current_user else None
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
        user_id = current_user[0][0] if current_user else None

        if not user_id:
            raise PostException(message="로그인이 필요합니다", status_code=401)

        # 1. 게시글 존재 여부 확인
        post = self.db._select(table="posts", columns=["id"], id=post_id)
        if not post:
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
    ) -> List[CommentItem]:
        """댓글 목록 조회"""
        current_user_id = current_user[0][0] if current_user else None

        # 1. 원댓글 조회 (limit + 1개)
        parent_query = """
            SELECT
                c.id, c.content, c.like_count, c.depth, c.parent_id,
                c.created_at, c.updated_at,
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
            ORDER BY c.created_at DESC
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
                id=0,  # 또는 None
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
                    user_info=create_user_info(child),
                    sub_comments=[],  # 대댓글은 하위 댓글을 가질 수 없음
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
                user_info=create_user_info(comment),
                sub_comments=child_map.get(comment["id"], []),
            )
            for comment in parent_comments
        ]

        return comment_list, has_more

    async def update_comment(self, current_user: AlphafinderUser, comment_id: int, comment_update: CommentUpdate) -> bool:
        """댓글 수정"""
        current_time = datetime.now(UTC)
        user_id = current_user[0][0] if current_user else None

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
        user_id = current_user[0][0] if current_user else None

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


def get_community_service() -> CommunityService:
    return CommunityService()
