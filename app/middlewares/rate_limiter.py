import time
from typing import Optional, Tuple, List, Callable
import json
from fastapi import Request, Response, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
from redis.exceptions import RedisError

from app.core.redis import redis_client
from app.core.logger import setup_logger

logger = setup_logger(__name__)


class RateLimitExceeded(HTTPException):
    def __init__(self, retry_after: int):
        super().__init__(status_code=429, detail="Rate limit exceeded. Please try again later.")
        self.headers = {"Retry-After": str(retry_after)}


class RateLimiterService:
    """Redis를 사용한 레이트 리미팅 핵심 서비스"""

    def __init__(self, redis=None):
        self._redis = redis or redis_client()

    async def is_rate_limited(
        self, key: str, max_requests: int, window_seconds: int, current_time: Optional[int] = None
    ) -> Tuple[bool, int, int]:
        """
        요청이 레이트 제한되어야 하는지 확인합니다.

        Args:
            key: 이 레이트 제한의 Redis 키 (예: 'rate:global:127.0.0.1')
            max_requests: 시간 윈도우 내에 허용되는 최대 요청 수
            window_seconds: 시간 윈도우 (초 단위)
            current_time: 현재 시간 (초 단위, 기본값은 현재 시간)

        Returns:
            다음을 포함하는 튜플:
            - is_limited: 요청이 제한되었는지 여부
            - remaining: 남은 허용 요청 수
            - retry_after: 레이트 제한이 재설정될 때까지의 시간 (초)
        """
        current_time = current_time or int(time.time())
        current_window = current_time // window_seconds
        current_window_key = f"{key}:{current_window}"
        prev_window_key = f"{key}:{current_window - 1}"

        try:
            # 증가시키기 전에 현재 카운트 가져오기
            pipeline = self._redis.pipeline()
            pipeline.get(current_window_key)
            pipeline.get(prev_window_key)
            results = pipeline.execute()

            # 결과에서 값 가져오기
            current_count = int(results[0] or 0)
            prev_count = int(results[1] or 0)

            # 이전 윈도우의 적용되는 비율 계산
            elapsed_in_window = current_time % window_seconds
            time_since_window_start = elapsed_in_window
            weight_of_previous = 1 - (time_since_window_start / window_seconds)

            # 두 윈도우를 고려한 총 요청 수 계산
            weighted_prev_count = prev_count * weight_of_previous
            total_requests = current_count + weighted_prev_count

            # 이 요청이 제한을 초과하는지 확인
            is_limited = total_requests >= max_requests

            # 남은 요청 수와 재시도 시간 계산
            remaining = max(0, max_requests - int(total_requests))
            window_end = (current_window + 1) * window_seconds
            retry_after = window_end - current_time

            # 제한되지 않은 경우에만 카운터 증가
            if not is_limited:
                pipeline = self._redis.pipeline()
                pipeline.incr(current_window_key)
                pipeline.expire(current_window_key, window_seconds * 2)  # 정리를 위해 윈도우의 2배로 설정
                pipeline.execute()

                # 응답 헤더를 위해 total_requests와 remaining 조정
                total_requests += 1
                remaining = max(0, max_requests - int(total_requests))

            logger.debug(
                f"레이트 제한 확인: key={key}, current={current_count}, prev={prev_count}, weighted_prev={weighted_prev_count:.2f}, total={total_requests:.2f}, max={max_requests}, remaining={remaining}, is_limited={is_limited}"
            )

            return is_limited, remaining, retry_after

        except RedisError as e:
            # Redis 오류 시 로깅하지만 레이트 제한은 적용하지 않음
            logger.error(f"레이트 리미터에서 Redis 오류 발생: {str(e)}")
            return False, max_requests, 0

    async def is_whitelisted(self, client_id: str) -> bool:
        """클라이언트가 화이트리스트에 있는지 확인합니다"""
        try:
            whitelist_key = "rate_limit:whitelist"
            return bool(self._redis.sismember(whitelist_key, client_id))
        except RedisError as e:
            logger.error(f"화이트리스트 확인 중 Redis 오류 발생: {str(e)}")
            return False

    async def add_to_whitelist(self, client_id: str) -> bool:
        """클라이언트를 화이트리스트에 추가합니다"""
        try:
            whitelist_key = "rate_limit:whitelist"
            return bool(self._redis.sadd(whitelist_key, client_id))
        except RedisError as e:
            logger.error(f"화이트리스트에 추가 중 Redis 오류 발생: {str(e)}")
            return False

    async def remove_from_whitelist(self, client_id: str) -> bool:
        """클라이언트를 화이트리스트에서 제거합니다"""
        try:
            whitelist_key = "rate_limit:whitelist"
            return bool(self._redis.srem(whitelist_key, client_id))
        except RedisError as e:
            logger.error(f"화이트리스트에서 제거 중 Redis 오류 발생: {str(e)}")
            return False

    async def clear_rate_limits(self, key_pattern: str) -> int:
        """지정된 패턴과 일치하는 레이트 제한을 지웁니다"""
        try:
            keys = self._redis.keys(key_pattern)
            if keys:
                return self._redis.delete(*keys)
            return 0
        except RedisError as e:
            logger.error(f"레이트 제한 지우는 중 Redis 오류 발생: {str(e)}")
            return 0


class GlobalRateLimitMiddleware(BaseHTTPMiddleware):
    """모든 엔드포인트에 대한 전역 레이트 리미팅 미들웨어"""

    def __init__(
        self,
        app: ASGIApp,
        max_requests: int = 100,
        window_seconds: int = 60,
        exclude_paths: List[str] = None,
        get_client_id: Optional[Callable[[Request], str]] = None,
        rate_limiter_service: Optional[RateLimiterService] = None,
    ):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.exclude_paths = exclude_paths or []
        self.rate_limiter_service = rate_limiter_service or RateLimiterService()
        self.get_client_id = get_client_id or self._default_client_id

    @staticmethod
    def _default_client_id(request: Request) -> str:
        """IP 주소를 사용한 기본 클라이언트 ID 추출기"""
        # 프록시/로드 밸런서 뒤에 있는 경우 실제 IP 가져오기
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # 체인에서 첫 번째 IP 가져오기
            client_ip = forwarded_for.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client else "unknown"
        return client_ip

    async def dispatch(self, request: Request, call_next) -> Response:
        """레이트 리미터를 통해 요청 처리"""
        # 제외된 경로의 경우 레이트 제한 건너뛰기
        if any(request.url.path.startswith(path) for path in self.exclude_paths):
            return await call_next(request)

        # 클라이언트 식별자 가져오기
        client_id = self.get_client_id(request)

        # 화이트리스트 확인
        if await self.rate_limiter_service.is_whitelisted(client_id):
            return await call_next(request)

        # 레이트 제한 키 생성
        key = f"rate:global:{client_id}"

        # 레이트 제한 확인
        is_limited, remaining, retry_after = await self.rate_limiter_service.is_rate_limited(
            key, self.max_requests, self.window_seconds
        )

        # 레이트 제한 헤더 설정
        if is_limited:
            response = Response(
                content=json.dumps({"detail": "Rate limit exceeded. Please try again later."}),
                status_code=429,
                media_type="application/json",
            )
        else:
            response = await call_next(request)

        # 레이트 제한 헤더 추가
        response.headers["X-RateLimit-Limit"] = str(self.max_requests)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(retry_after)

        if is_limited:
            response.headers["Retry-After"] = str(retry_after)

        return response


def endpoint_rate_limiter(
    max_requests: int = 1,
    window_seconds: int = 10,
    get_client_id: Optional[Callable[[Request], str]] = None,
):
    """
    엔드포인트별 레이트 리미팅을 위한 의존성.

    사용 예:
        @app.post("/sensitive-endpoint")
        async def sensitive_operation(
            request: Request,
            rate_limit: bool = Depends(endpoint_rate_limiter(max_requests=1, window_seconds=10))
        ):
            return {"result": "success"}
    """

    async def _check_rate_limit(request: Request):
        service = RateLimiterService()

        # 클라이언트 식별자 가져오기
        client_id_func = get_client_id or GlobalRateLimitMiddleware._default_client_id
        client_id = client_id_func(request)

        # 화이트리스트에 있는 경우 건너뛰기
        if await service.is_whitelisted(client_id):
            return True

        # 엔드포인트별 레이트 제한 키 생성
        path = request.url.path
        key = f"rate:endpoint:{path}:{client_id}"

        # 레이트 제한 확인
        is_limited, _, retry_after = await service.is_rate_limited(key, max_requests, window_seconds)

        if is_limited:
            raise RateLimitExceeded(retry_after)

        return True

    return _check_rate_limit
