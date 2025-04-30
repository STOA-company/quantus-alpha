import uuid
from datetime import timedelta
from typing import List, Optional, Set, Union

from fastapi import Request, Response
from redis import Redis

from app.core.redis import redis_client


class StoryViewCache:
    """
    Redis-based cache for managing Instagram-style story views.
    """

    def __init__(self, redis_instance: Optional[Redis] = None):
        self.redis = redis_instance or redis_client()
        self.expiry_time = timedelta(days=2)  # Stories expire after 2 days
        self.anonymous_cookie_name = "anonymous_user_id"
        self.max_stories_per_user = 1000  # Prevent unlimited growth for a user

    def _get_user_key(self, user_id: Union[str, int]) -> str:
        """
        Generate a Redis key for a user's viewed stories set.
        """
        return f"user:{user_id}:viewed_stories"

    def _get_story_key(self, ticker: str, story_type: str, story_id: int) -> str:
        """
        Generate a key for a specific story item.
        """
        return f"{ticker}_{story_type}_{story_id}"

    def get_or_create_anonymous_id(self, request: Request, response: Response) -> str:
        """
        Get an existing anonymous user ID from cookies or create a new one.
        """
        if self.anonymous_cookie_name in request.cookies:
            return request.cookies[self.anonymous_cookie_name]

        # Create a new anonymous ID
        anonymous_id = str(uuid.uuid4())

        # Set cookie with the anonymous ID (expires in 90 days)
        response.set_cookie(
            key=self.anonymous_cookie_name,
            value=anonymous_id,
            max_age=60 * 60 * 24 * 90,  # 90 days
            httponly=True,
            samesite="lax",
        )

        return anonymous_id

    def mark_story_as_viewed(self, user_id: Union[str, int], ticker: str, story_type: str, story_id: int) -> bool:
        """
        Mark a story as viewed by a user in Redis.

        Args:
            user_id: The user's ID (can be authenticated user ID or anonymous ID)
            ticker: Stock ticker symbol
            story_type: Type of story ('news' or 'disclosure')
            story_id: ID of the story

        Returns:
            bool: True if operation was successful
        """
        user_key = self._get_user_key(user_id)
        story_key = self._get_story_key(ticker, story_type, story_id)

        # Add to the user's viewed stories set
        self.redis.sadd(user_key, story_key)

        # Set expiry time on the user's set
        self.redis.expire(user_key, int(self.expiry_time.total_seconds()))

        # Trim the set if it gets too large
        current_size = self.redis.scard(user_key)
        if current_size > self.max_stories_per_user:
            # Get all members
            all_members = self.redis.smembers(user_key)
            # Sort by timestamp (would need to implement if storing timestamps)
            # For now, just remove some random elements
            to_remove = list(all_members)[: current_size - self.max_stories_per_user]
            if to_remove:
                self.redis.srem(user_key, *to_remove)

        return True

    def is_story_viewed(self, user_id: Union[str, int], ticker: str, story_type: str, story_id: int) -> bool:
        """
        Check if a story has been viewed by a user.

        Args:
            user_id: The user's ID
            ticker: Stock ticker symbol
            story_type: Type of story ('news' or 'disclosure')
            story_id: ID of the story

        Returns:
            bool: True if the story has been viewed
        """
        user_key = self._get_user_key(user_id)
        story_key = self._get_story_key(ticker, story_type, story_id)

        return bool(self.redis.sismember(user_key, story_key))

    def get_viewed_stories(self, user_id: Union[str, int]) -> Set[str]:
        """
        Get all stories viewed by a user.

        Args:
            user_id: The user's ID

        Returns:
            Set[str]: Set of story keys viewed by the user
        """
        user_key = self._get_user_key(user_id)
        return self.redis.smembers(user_key)

    def check_stories_viewed(self, user_id: Union[str, int], stories: List[dict]) -> List[dict]:
        """
        Check which stories have been viewed by a user and add a flag to each story.

        Args:
            user_id: The user's ID
            stories: List of story dictionaries with 'ticker', 'type', and 'id' keys

        Returns:
            List[dict]: Same list with 'is_viewed' flag added to each story
        """
        user_key = self._get_user_key(user_id)
        viewed_stories = self.redis.smembers(user_key)

        for story in stories:
            story_key = self._get_story_key(story["ticker"], story["type"], story["id"])
            story["is_viewed"] = story_key in viewed_stories

        return stories

    def get_story_view_count(self, ticker: str, story_type: str, story_id: int) -> int:
        """
        Get the number of users who have viewed a specific story.
        This performs a scan across all user sets, so it's expensive.
        Consider using a separate counter for high-traffic scenarios.

        Args:
            ticker: Stock ticker symbol
            story_type: Type of story ('news' or 'disclosure')
            story_id: ID of the story

        Returns:
            int: Number of users who have viewed the story
        """
        story_key = self._get_story_key(ticker, story_type, story_id)
        count = 0

        # Scan all users with pattern user:*:viewed_stories
        cursor = "0"
        while cursor != 0:
            cursor, keys = self.redis.scan(cursor=cursor, match="user:*:viewed_stories", count=100)

            for key in keys:
                if self.redis.sismember(key, story_key):
                    count += 1

            if cursor == "0":
                break

        return count


# Create a singleton instance
story_view_cache = StoryViewCache()


def get_story_view_cache() -> StoryViewCache:
    """
    Dependency for FastAPI to get the StoryViewCache instance.
    """
    return story_view_cache
