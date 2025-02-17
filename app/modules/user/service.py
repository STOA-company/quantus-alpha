import json
from app.database.crud import database
from typing import List, Tuple, Optional
from fastapi import HTTPException, UploadFile

from app.models.models_users import AlphafinderUser
from app.modules.community.schemas import CommentItemWithPostInfo, PostInfo, ResponsePost, UserInfo
from app.modules.user.schemas import UserProfileResponse

from sqlalchemy import text


def get_user_by_email(email: str):
    users = database._select(table="alphafinder_user", columns=["id", "email", "nickname"], email=email, limit=1)
    return users[0] if users else None


def create_user(email: str, nickname: str, provider: str, base64: Optional[str] = None):
    database._insert(
        table="alphafinder_user",
        sets={"email": email, "nickname": nickname, "provider": provider, "profile_image": base64},
    )

    user = get_user_by_email(email)
    return user


def delete_user(id: int):
    database._delete(table="alphafinder_user", id=id)


def update_user(id: int, nickname: str = None, profile_image: UploadFile = None, favorite_stock: List[str] = None):
    database._update(
        table="alphafinder_user",
        sets={
            "nickname": nickname,
            "profile_image": profile_image,
        },
        id=id,
    )

    for ticker in favorite_stock:
        add_favorite_stock(id, ticker)


def check_nickname_available(nickname: str):
    is_exist = database._select(table="alphafinder_user", nickname=nickname)
    if is_exist:
        return False
    return True


def add_favorite_stock(id: int, ticker: str):
    if database._select(table="user_stock_interest", user_id=id, ticker=ticker, limit=1):
        return
    database._insert(
        table="user_stock_interest",
        sets={
            "user_id": id,
            "ticker": ticker,
        },
    )


def delete_favorite_stock(id: int, ticker: str):
    database._delete(table="user_stock_interest", user_id=id, ticker=ticker)


class UserProfileService:
    def __init__(self):
        self.db = database

    def get_user_profile(self, current_user: AlphafinderUser, user_id: Optional[int]) -> UserProfileResponse:
        """
        사용자 프로필 조회

        Args:
            current_user: 현재 로그인한 사용자 정보
            user_id: 조회할 사용자 ID (None인 경우 현재 사용자)

        Returns:
            UserProfileResponse: 사용자 프로필 정보

        Raises:
            HTTPException: 사용자가 존재하지 않는 경우
        """
        if not user_id:
            user_id = current_user[0][0]

        profile_data = self._get_user_profile_with_counts(user_id)
        if not profile_data:
            raise HTTPException(status_code=404, detail="존재하지 않는 사용자입니다.")

        return self._create_profile_response(profile_data=profile_data, is_my_profile=current_user[0][0] == user_id)

    def _get_user_profile_with_counts(self, user_id: int) -> Optional[dict]:
        """
        사용자 정보와 게시글/댓글 수를 한 번의 쿼리로 조회

        Args:
            user_id: 조회할 사용자 ID

        Returns:
            Optional[dict]: 사용자 정보와 카운트 데이터
        """
        query = """
            SELECT
                u.id, u.nickname, u.profile_image,
                COALESCE(p.post_count, 0) as post_count,
                COALESCE(c.comment_count, 0) as comment_count
            FROM alphafinder_user u
            LEFT JOIN (
                SELECT user_id, COUNT(*) as post_count
                FROM posts
                WHERE user_id = :user_id
                GROUP BY user_id
            ) p ON u.id = p.user_id
            LEFT JOIN (
                SELECT user_id, COUNT(*) as comment_count
                FROM comments
                WHERE user_id = :user_id
                GROUP BY user_id
            ) c ON u.id = c.user_id
            WHERE u.id = :user_id
        """

        result = self.db._execute(text(query), {"user_id": user_id})
        row = result.mappings().first()

        return dict(row) if row else None

    def _create_profile_response(self, profile_data: dict, is_my_profile: bool) -> UserProfileResponse:
        """
        프로필 응답 생성

        Args:
            profile_data: 프로필 데이터
            is_my_profile: 자신의 프로필 여부

        Returns:
            UserProfileResponse: 프로필 응답 객체
        """
        # 자신의 프로필이 아닌 경우 설정에 따라 카운트 조정
        if not is_my_profile:
            # TODO: 사용자 설정에 따라 공개 여부 결정
            is_post_open = True  # 실제로는 사용자 설정에서 가져와야 함
            is_comment_open = True  # 실제로는 사용자 설정에서 가져와야 함

            post_count = profile_data["post_count"] if is_post_open else -1
            comment_count = profile_data["comment_count"] if is_comment_open else -1
        else:
            post_count = profile_data["post_count"]
            comment_count = profile_data["comment_count"]

        return UserProfileResponse(
            id=profile_data["id"],
            nickname=profile_data["nickname"],
            profile_image=profile_data.get("profile_image"),
            post_count=post_count,
            comment_count=comment_count,
        )

    async def get_user_posts(
        self,
        current_user: AlphafinderUser,
        offset: int = 0,
        limit: int = 10,
        user_id: Optional[int] = None,
    ) -> Tuple[List[ResponsePost], bool]:
        """
        사용자가 작성한 게시글 목록 조회

        Args:
            current_user: 현재 로그인한 사용자
            user_id: 조회할 사용자 ID
            offset: 시작 위치
            limit: 조회할 게시글 수

        Returns:
            Tuple[List[ResponsePost], bool]: 게시글 목록과 추가 데이터 존재 여부
        """
        current_user_id = current_user[0][0] if current_user else None
        if not user_id:
            user_id = current_user_id

        # 1. 사용자 존재 여부 및 프로필 공개 설정 확인
        user = self.db._select(table="alphafinder_user", columns=["id", "nickname", "profile_image"], id=user_id, limit=1)
        if not user:
            raise HTTPException(status_code=404, detail="존재하지 않는 사용자입니다.")

        user = user[0]._mapping
        is_my_profile = current_user[0][0] == user_id

        # 2. 다른 사용자의 프로필이고 게시글이 비공개인 경우
        if not is_my_profile:
            # TODO: 실제로는 사용자 설정에서 가져와야 함
            is_post_open = True
            if not is_post_open:
                return [], False

        # 3. 게시글 조회 쿼리
        query = """
            WITH post_data AS (
                SELECT DISTINCT
                    p.id, p.title, p.content, p.image_url,
                    p.like_count, p.comment_count, p.created_at, p.updated_at,
                    c.name as category_name,
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
                FROM posts p
                JOIN categories c ON p.category_id = c.id
                WHERE p.user_id = :user_id
                ORDER BY p.created_at DESC
                LIMIT :limit OFFSET :offset
            )
            SELECT
                pd.*,
                GROUP_CONCAT(ps.stock_ticker) as stock_tickers
            FROM post_data pd
            LEFT JOIN post_stocks ps ON pd.id = ps.post_id
            GROUP BY pd.id
        """

        params = {"user_id": user_id, "current_user_id": current_user[0][0], "limit": limit + 1, "offset": offset}

        result = self.db._execute(text(query), params)
        posts = result.mappings().all()

        # 4. 추가 데이터 존재 여부 확인
        has_more = len(posts) > limit
        if has_more:
            posts = posts[:-1]

        # 5. 응답 데이터 구성
        response_posts = []
        for post in posts:
            response_posts.append(
                ResponsePost(
                    id=post["id"],
                    title=post["title"],
                    content=post["content"],
                    category_name=post["category_name"],
                    image_url=post.get("image_url"),
                    like_count=post["like_count"],
                    comment_count=post["comment_count"],
                    is_changed=post["created_at"] != post["updated_at"],
                    is_bookmarked=post["is_bookmarked"],
                    is_liked=post["is_liked"],
                    created_at=post["created_at"],
                    stock_tickers=post["stock_tickers"].split(",") if post["stock_tickers"] else [],
                    user_info=UserInfo(id=user["id"], nickname=user["nickname"], profile_image=user.get("profile_image")),
                )
            )

        return response_posts, has_more

    async def get_user_comments(
        self,
        current_user: Optional[AlphafinderUser],
        offset: int = 0,
        limit: int = 10,
        user_id: Optional[int] = None,
    ) -> Tuple[List[CommentItemWithPostInfo], bool]:
        """
        사용자가 작성한 댓글 목록 조회

        Args:
            current_user: 현재 로그인한 사용자 (None일 수 있음)
            offset: 시작 위치
            limit: 조회할 댓글 수
            user_id: 조회할 사용자 ID (None인 경우 현재 사용자)

        Returns:
            Tuple[List[CommentItem], bool]: 댓글 목록과 추가 데이터 존재 여부
        """
        current_user_id = current_user[0][0] if current_user else None
        if not user_id:
            user_id = current_user_id

        is_my_profile = current_user_id == user_id if current_user_id else False

        # 1. 사용자 존재 여부 확인
        user = self.db._select(table="alphafinder_user", columns=["id", "nickname", "profile_image"], id=user_id, limit=1)
        if not user:
            raise HTTPException(status_code=404, detail="존재하지 않는 사용자입니다.")

        user = user[0]._mapping

        # 2. 다른 사용자의 프로필이고 댓글이 비공개인 경우
        if not is_my_profile:
            # TODO: 실제로는 사용자 설정에서 가져와야 함
            is_comment_open = True
            if not is_comment_open:
                return [], False

        # 3. 댓글 조회 쿼리
        query = """
            WITH comment_data AS (
                SELECT
                    c.id, c.content, c.like_count, c.depth, c.parent_id,
                    c.created_at, c.updated_at,
                    p.id as post_id, p.title as post_title,
                    CASE WHEN :current_user_id IS NOT NULL THEN
                        EXISTS(
                            SELECT 1 FROM comment_likes cl
                            WHERE cl.comment_id = c.id AND cl.user_id = :current_user_id
                        )
                    ELSE false END as is_liked
                FROM comments c
                JOIN posts p ON c.post_id = p.id
                WHERE c.user_id = :user_id
                ORDER BY c.created_at DESC
                LIMIT :limit OFFSET :offset
            )
            SELECT
                cd.*,
                JSON_OBJECT(
                    'post_id', cd.post_id,
                    'post_title', cd.post_title
                ) as post_info
            FROM comment_data cd
        """

        params = {"user_id": user_id, "current_user_id": current_user_id, "limit": limit + 1, "offset": offset}

        result = self.db._execute(text(query), params)
        comments = result.mappings().all()

        # 4. 추가 데이터 존재 여부 확인
        has_more = len(comments) > limit
        if has_more:
            comments = comments[:-1]

        # 5. 응답 데이터 구성
        response_comments = []
        for comment in comments:
            post_info = json.loads(comment["post_info"])
            response_comments.append(
                CommentItemWithPostInfo(
                    id=comment["id"],
                    content=comment["content"],
                    like_count=comment["like_count"],
                    depth=comment["depth"],
                    parent_id=comment["parent_id"],
                    created_at=comment["created_at"],
                    is_changed=comment["created_at"] != comment["updated_at"],
                    is_liked=comment["is_liked"],
                    is_mine=is_my_profile,
                    user_info=UserInfo(id=user["id"], nickname=user["nickname"], profile_image=user.get("profile_image")),
                    sub_comments=[],
                    post_info=PostInfo(id=post_info["post_id"], title=post_info["post_title"]),
                )
            )

        return response_comments, has_more


def get_user_profile_service() -> UserProfileService:
    """UserProfileService 의존성 주입을 위한 팩토리 함수"""
    return UserProfileService()
