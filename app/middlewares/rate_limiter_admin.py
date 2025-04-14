from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
import time

from app.core.redis import redis_client
from app.core.logger import setup_logger
from app.middlewares.rate_limiter import RateLimiterService

logger = setup_logger(__name__)

# 레이트 리미터 관리를 위한 라우터 생성
router = APIRouter(
    prefix="/admin/rate-limiter",
    tags=["레이트 리미터 관리"],
)

# 관리자 엔드포인트를 위한 API 키 보안
API_KEY_HEADER = APIKeyHeader(name="X-API-Key")


async def get_rate_limiter_service():
    """레이트 리미터 서비스를 가져오기 위한 의존성"""
    return RateLimiterService()


async def verify_api_key(api_key: str = Security(API_KEY_HEADER)):
    """관리자 작업을 위한 API 키 검증"""
    # TODO: 실제 API 키 검증 로직으로 대체하세요
    valid_api_key = "your-secret-admin-api-key"
    if api_key != valid_api_key:
        raise HTTPException(
            status_code=403,
            detail="Invalid or missing API key",
        )
    return api_key


@router.get("/whitelist", response_model=List[str])
async def get_whitelist(
    service: RateLimiterService = Depends(get_rate_limiter_service),
    _: str = Depends(verify_api_key),
):
    """모든 화이트리스트에 등록된 클라이언트 ID 가져오기"""
    try:
        redis = service._redis
        whitelist_key = "rate_limit:whitelist"
        whitelist = redis.smembers(whitelist_key)
        return list(whitelist)
    except Exception as e:
        logger.error(f"화이트리스트 가져오기 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="화이트리스트를 가져오는데 실패했습니다")


@router.post("/whitelist/{client_id}")
async def add_to_whitelist(
    client_id: str,
    service: RateLimiterService = Depends(get_rate_limiter_service),
    _: str = Depends(verify_api_key),
):
    """클라이언트 ID를 화이트리스트에 추가"""
    success = await service.add_to_whitelist(client_id)
    if not success:
        raise HTTPException(status_code=500, detail="화이트리스트에 추가하는데 실패했습니다")
    return {"status": "success", "message": f"{client_id}를 화이트리스트에 추가했습니다"}


@router.delete("/whitelist/{client_id}")
async def remove_from_whitelist(
    client_id: str,
    service: RateLimiterService = Depends(get_rate_limiter_service),
    _: str = Depends(verify_api_key),
):
    """클라이언트 ID를 화이트리스트에서 제거"""
    success = await service.remove_from_whitelist(client_id)
    if not success:
        raise HTTPException(status_code=500, detail="화이트리스트에서 제거하는데 실패했습니다")
    return {"status": "success", "message": f"{client_id}를 화이트리스트에서 제거했습니다"}


@router.get("/stats")
async def get_rate_limit_stats(
    client_id: Optional[str] = None,
    path: Optional[str] = None,
    _: str = Depends(verify_api_key),
):
    """레이트 제한 통계 가져오기"""
    try:
        redis = redis_client()
        now = int(time.time())

        # 검색할 키의 패턴 정의
        if client_id and path:
            pattern = f"rate:endpoint:{path}:{client_id}:*"
        elif client_id:
            pattern = f"rate:*:{client_id}:*"
        elif path:
            pattern = f"rate:endpoint:{path}:*"
        else:
            pattern = "rate:*"

        keys = redis.keys(pattern)

        # 통계 추출
        stats = {}
        for key in keys:
            count = int(redis.get(key) or 0)
            ttl = redis.ttl(key)

            # 키를 파싱하여 타입, 경로, 클라이언트 ID, 윈도우 정보 가져오기
            key_parts = key.split(":")

            if len(key_parts) >= 3:
                rate_type = key_parts[1]  # global 또는 endpoint

                if rate_type == "global":
                    client = key_parts[2]
                    window = key_parts[3] if len(key_parts) > 3 else "unknown"
                    path_info = "global"
                else:  # endpoint
                    if len(key_parts) >= 4:
                        path_info = key_parts[2]
                        client = key_parts[3]
                        window = key_parts[4] if len(key_parts) > 4 else "unknown"
                    else:
                        path_info = "unknown"
                        client = "unknown"
                        window = "unknown"

                key_info = f"{rate_type}:{path_info}:{client}"

                if key_info not in stats:
                    stats[key_info] = {"count": 0, "ttl": 0, "windows": {}}

                stats[key_info]["count"] += count
                stats[key_info]["ttl"] = max(stats[key_info]["ttl"], ttl)
                stats[key_info]["windows"][window] = {"count": count, "ttl": ttl}

        return {"stats": stats, "timestamp": now}

    except Exception as e:
        logger.error(f"레이트 제한 통계 가져오기 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="레이트 제한 통계를 가져오는데 실패했습니다")


@router.delete("/clear")
async def clear_rate_limits(
    client_id: Optional[str] = None,
    path: Optional[str] = None,
    _: str = Depends(verify_api_key),
):
    """특정 클라이언트 또는 경로, 또는 둘 다에 대한 레이트 제한 지우기"""
    try:
        redis = redis_client()

        # 삭제할 키의 패턴 정의
        if client_id and path:
            pattern = f"rate:endpoint:{path}:{client_id}:*"
        elif client_id:
            pattern = f"rate:*:{client_id}:*"
        elif path:
            pattern = f"rate:endpoint:{path}:*"
        else:
            raise HTTPException(status_code=400, detail="client_id 또는 path를 최소한 하나 제공해야 합니다")

        keys = redis.keys(pattern)
        if keys:
            redis.delete(*keys)

        return {"status": "success", "message": f"{len(keys)}개의 레이트 제한 키를 지웠습니다", "keys_deleted": len(keys)}

    except Exception as e:
        logger.error(f"레이트 제한 지우기 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="레이트 제한을 지우는데 실패했습니다")
