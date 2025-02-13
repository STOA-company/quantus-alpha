from app.database.crud import database
from typing import List
from fastapi import HTTPException, UploadFile

from app.models.models_users import AlphafinderUser
from app.modules.user.schemas import UserProfileResponse

from typing import Optional
from sqlalchemy import text


def get_user_by_email(email: str):
    users = database._select(table="alphafinder_user", columns=["id", "email", "nickname"], email=email, limit=1)
    return users[0] if users else None


def create_user(email: str, nickname: str, provider: str):
    database._insert(
        table="alphafinder_user",
        sets={
            "email": email,
            "nickname": nickname,
            "provider": provider,
        },
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


def get_user_profile_service() -> UserProfileService:
    """UserProfileService 의존성 주입을 위한 팩토리 함수"""
    return UserProfileService()
