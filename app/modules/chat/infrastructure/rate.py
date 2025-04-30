from datetime import datetime

from app.core.redis import redis_client
from app.modules.chat.infrastructure.constants import MAX_REQUEST_COUNT


def get_rate_limit_key(user_id: int) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    return f"rate_limit:stream_chat:{user_id}:{today}"


def check_rate_limit(user_id: int, user_is_staff: bool) -> bool:
    if user_is_staff:
        return True

    key = get_rate_limit_key(user_id)
    redis = redis_client()
    count = redis.get(key)

    if count is None:
        redis.setex(key, 86400, 1)
        return True

    count = int(count)
    if count >= MAX_REQUEST_COUNT:
        return False

    return True


def increment_rate_limit(user_id: int, user_is_staff: bool) -> None:
    if not user_is_staff:
        key = get_rate_limit_key(user_id)
        redis = redis_client()
        if not redis.exists(key):
            redis.setex(key, 86400, 1)
        else:
            redis.incr(key)


def decrement_rate_limit(user_id: int) -> None:
    key = get_rate_limit_key(user_id)
    redis = redis_client()
    redis.decr(key)
