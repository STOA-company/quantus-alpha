import functools
import inspect
import json
import time
from typing import Any, Callable, TypeVar

from fastapi import Request
from fastapi.encoders import jsonable_encoder

from app.core.logger import setup_logger
from app.core.redis import redis_client

T = TypeVar("T")
logger = setup_logger(__name__)


def one_minute_cache(prefix: str = "") -> Callable:
    """
    Redis 기반 1분 캐싱 데코레이터

    이 데코레이터는 함수의 결과를 Redis에 캐싱하고, 1분 동안 동일한 파라미터로 호출 시 캐시된 결과를 반환합니다.

    Args:
        prefix: 캐시 키에 사용할 프리픽스

    Returns:
        캐싱 로직이 적용된 데코레이터 함수
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            # Redis 클라이언트 초기화
            redis = redis_client()

            # 캐시 키 생성을 위해 필요한 정보만 추출
            func_name = func.__name__
            module_name = func.__module__

            # 함수의 파라미터 이름과 값을 가져옴
            sig = inspect.signature(func)

            # 모든 인자 매핑 (위치 인자 + 키워드 인자)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            # 간소화된 키 생성을 위한 파라미터 추출
            simplified_params = {}

            for name, value in bound_args.arguments.items():
                # Request 객체는 제외
                if isinstance(value, Request):
                    continue

                # 클래스 인스턴스인 경우
                if name == "self" or (
                    hasattr(value, "__class__") and not isinstance(value, (str, int, float, bool, type(None)))
                ):
                    # 특별한 경우 처리: TrendingStockRequest나 유사한 요청 객체
                    if hasattr(value, "ctry") and hasattr(value, "dict"):
                        simplified_params[name] = value.dict()
                    elif hasattr(value, "__dict__"):
                        # 중요 속성만 추출 (너무 복잡한 객체는 피함)
                        try:
                            reduced_dict = {}
                            for k, v in value.__dict__.items():
                                if not k.startswith("_") and isinstance(v, (str, int, float, bool, type(None))):
                                    reduced_dict[k] = v
                            simplified_params[name] = reduced_dict
                        except Exception:
                            simplified_params[name] = str(type(value))
                    else:
                        # 기타 객체는 클래스 이름만 사용
                        simplified_params[name] = str(type(value))
                else:
                    # 기본 타입은 그대로 사용
                    simplified_params[name] = value

            # 캐시 키 생성
            cache_key = f"{prefix}:{module_name}:{func_name}"

            # 파라미터가 있으면 추가 (정렬하여 일관성 보장)
            if simplified_params:
                params_str = json.dumps(simplified_params, sort_keys=True, default=str)
                import hashlib

                # 긴 파라미터 문자열은 해시값으로 대체
                params_hash = hashlib.md5(params_str.encode()).hexdigest()
                cache_key = f"{cache_key}:{params_hash}"

            logger.info(f"[REDIS CACHE] Generated cache key: {cache_key}")
            logger.debug(f"[REDIS CACHE] Params used for key: {simplified_params}")

            # 캐시 데이터 확인
            cached_data = redis.get(cache_key)

            if cached_data:
                try:
                    # 캐시된 데이터가 있으면 역직렬화하여 반환
                    logger.info(f"[REDIS CACHE] Cache hit for key: {cache_key}")
                    return json.loads(cached_data)
                except Exception as e:
                    # 직렬화 오류 시 원본 함수 실행
                    logger.error(f"[REDIS CACHE] Error deserializing cached data: {str(e)}")
            else:
                logger.info(f"[REDIS CACHE] Cache miss for key: {cache_key}")

            # 캐시된 데이터가 없으면 원본 함수 실행
            start_time = time.time()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"[REDIS CACHE] Function execution time: {execution_time:.4f}s")

            try:
                # Pydantic 모델을 JSON으로 변환 (FastAPI의 jsonable_encoder 사용)
                jsonable_result = jsonable_encoder(result)

                # 결과 직렬화하여 Redis에 저장 (60초 = 1분)
                redis.setex(name=cache_key, time=60, value=json.dumps(jsonable_result))
                logger.info(f"[REDIS CACHE] Cached result for key: {cache_key}, TTL: 60s")
            except Exception as e:
                # 직렬화 오류 시 캐싱하지 않고 결과만 반환
                logger.error(f"[REDIS CACHE] Error caching result: {str(e)}")

            return result

        return wrapper

    return decorator
