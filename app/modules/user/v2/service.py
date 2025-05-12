from fastapi import HTTPException

from app.core.logger import setup_logger
from app.database.crud import database_service, database_user
from app.utils.s3_utils import S3PresignedURLManager

logger = setup_logger(__name__)


class UserService:
    def __init__(self):
        self.db = database_service
        self.db_user = database_user
        self.s3_manager = S3PresignedURLManager(
            bucket_name="alpha-finder-image",
            max_file_size=5 * 1024 * 1024,  # 5MB
            presigned_url_expires_in=300,  # 5 minutes
            redis_cache_expires_in=270,  # 4 minutes 30 seconds
        )

    def update_profile(
        self,
        user_id: int,
        nickname: str = None,
        introduction: str = None,
        link: str = None,
    ):
        self.db._update(
            table="quantus_user",
            sets={"nickname": nickname, "introduction": introduction, "link": link},
            id=user_id,
        )
        return True

    def update_profile_image(
        self,
        user_id: int,
        image_url: str,
    ):
        self.db_user._update(
            table="quantus_user",
            sets={"image_url": image_url},
            id=user_id,
        )
        return True

    def check_nickname_available(self, nickname: str):
        is_exist = self.db_user._select(table="quantus_user", nickname=nickname)
        if is_exist:
            return False
        return True

    def generate_presigned_url(self, content_type: str, file_size: int, index: int = 0) -> dict:
        """
        Generate a presigned URL for uploading a profile image to S3.

        Args:
            content_type (str): Content type of the file (e.g., 'image/jpeg', 'image/png')
            file_size (int): Size of the file in bytes
            index (int): Index for multiple files (default: 0)

        Returns:
            dict: Dictionary containing upload URL and image key

        Raises:
            HTTPException: If the content type is not supported or file size is too large
        """
        try:
            return self.s3_manager.generate_upload_presigned_url(
                content_type=content_type, file_size=file_size, folder="profile", index=index
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Failed to generate presigned URL: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to generate presigned URL")

    def get_profile(self, user_id: int) -> dict:
        user = self.db_user._select(
            table="quantus_user", columns=["nickname", "profile_image", "introduction", "link"], id=user_id
        )
        user_info = user[0]
        return user_info


def get_user_service() -> UserService:
    """UserService 의존성 주입을 위한 팩토리 함수"""
    return UserService()
