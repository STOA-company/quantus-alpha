from datetime import datetime

from app.core.redis import redis_client


# Redis 키 설정 함수
def get_rate_limit_key(user_id: int) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    return f"rate_limit:stream_chat:{user_id}:{today}"


# API 호출 제한 체크 함수
def check_rate_limit(user_id: int, user_is_staff: bool) -> bool:
    if user_is_staff:
        return True

    key = get_rate_limit_key(user_id)
    count = redis_client.get(key)

    if count is None:
        # 첫 호출인 경우는 허용하고 카운트 시작
        redis_client.setex(key, 86400, 1)
        return True

    count = int(count)
    if count >= 3:
        return False

    # 아직 제한에 도달하지 않았으면 카운트 증가하지 않고 true 반환
    return True


# 호출 성공 시 카운트 증가 함수
def increment_rate_limit(user_id: int, user_is_staff: bool) -> None:
    if not user_is_staff:
        key = get_rate_limit_key(user_id)
        # 키가 이미 존재하면 증가, 없으면 생성하고 만료 설정
        if not redis_client.exists(key):
            redis_client.setex(key, 86400, 1)
        else:
            redis_client.incr(key)
