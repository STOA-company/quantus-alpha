import json
import uuid
from datetime import datetime
from typing import Dict, Optional

from app.core.redis import redis_client
from Aws.common.configs import s3_client


class S3PresignedURLManager:
    def __init__(
        self,
        bucket_name: str,
        allowed_content_types: Dict[str, str] = None,
        max_file_size: int = 5 * 1024 * 1024,  # 5MB
        presigned_url_expires_in: int = 300,  # 5 minutes
        redis_cache_expires_in: int = 270,  # 4 minutes 30 seconds
    ):
        """
        Initialize S3PresignedURLManager with configuration.

        Args:
            bucket_name (str): S3 bucket name
            allowed_content_types (Dict[str, str]): Mapping of content types to file extensions
            max_file_size (int): Maximum allowed file size in bytes
            presigned_url_expires_in (int): Presigned URL expiration time in seconds
            redis_cache_expires_in (int): Redis cache expiration time in seconds
        """
        self.bucket_name = bucket_name
        self.allowed_content_types = allowed_content_types or {
            "image/jpeg": "jpg",
            "image/png": "png",
            "image/gif": "gif",
        }
        self.max_file_size = max_file_size
        self.presigned_url_expires_in = presigned_url_expires_in
        self.redis_cache_expires_in = redis_cache_expires_in
        self.redis = redis_client()

    def _get_extension_from_content_type(self, content_type: str) -> str:
        """Extract file extension from Content-Type."""
        extension = self.allowed_content_types.get(content_type.lower())
        if not extension:
            raise ValueError(f"Unsupported Content-Type. Allowed types: {', '.join(self.allowed_content_types.keys())}")
        return extension

    def _generate_image_key(self, extension: str, folder: str = "community", index: int = 0) -> str:
        """Generate a unique S3 key for an image."""
        now = datetime.now()
        date_path = now.strftime("%Y/%m/%d")
        unique_id = str(uuid.uuid4())
        return f"{folder}/{date_path}/{unique_id}_{index}.{extension}"

    def _get_cached_presigned_url(self, image_key: str, url_type: str) -> Optional[dict]:
        """Get cached presigned URL from Redis."""
        cached_data = self.redis.get(f"presigned_url:{url_type}:{image_key}")
        if cached_data:
            return json.loads(cached_data)
        return None

    def _cache_presigned_url(self, image_key: str, presigned_data: dict, url_type: str) -> None:
        """Cache presigned URL in Redis."""
        self.redis.setex(
            f"presigned_url:{url_type}:{image_key}",
            self.redis_cache_expires_in,
            json.dumps(presigned_data),
        )

    def generate_upload_presigned_url(
        self, content_type: str, file_size: int, folder: str = "community", index: int = 0
    ) -> dict:
        """
        Generate a presigned URL for uploading to S3.

        Args:
            content_type (str): Content type of the file
            file_size (int): Size of the file in bytes
            index (int): Index for multiple files

        Returns:
            dict: Dictionary containing upload URL and image key
        """
        if file_size > self.max_file_size:
            raise ValueError(f"File size too large. Maximum size: {self.max_file_size / (1024 * 1024)}MB")

        extension = self._get_extension_from_content_type(content_type)
        image_key = self._generate_image_key(extension, folder, index)

        presigned_post = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": self.bucket_name,
                "Key": image_key,
                "ContentType": content_type,
            },
            ExpiresIn=self.presigned_url_expires_in,
        )

        presigned_data = {
            "upload_url": presigned_post,
            "image_key": image_key,
        }

        return presigned_data

    def generate_get_presigned_url(self, image_key: str) -> dict:
        """
        Generate a presigned URL for getting an object from S3.

        Args:
            image_key (str): S3 object key

        Returns:
            dict: Dictionary containing get URL, image key, and expiration time
        """
        # Check Redis cache first
        cached_data = self._get_cached_presigned_url(image_key, "get")
        if cached_data:
            return cached_data

        # Generate new presigned URL
        get_url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.bucket_name,
                "Key": image_key,
            },
            ExpiresIn=self.presigned_url_expires_in,
        )

        presigned_data = {
            "get_url": get_url,
            "image_key": image_key,
            "expires_in": self.presigned_url_expires_in,
        }

        # Cache the presigned URL
        self._cache_presigned_url(image_key, presigned_data, "get")

        return presigned_data
