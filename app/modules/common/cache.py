from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Tuple
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class CacheStrategy(Enum):
    PERMANENT = "permanent"  # 과거 데이터
    TEMPORARY = "temporary"  # 최근 데이터
    NO_CACHE = "no_cache"  # 실시간 데이터


class MemoryCache:
    """메모리 캐시 관리"""

    def __init__(self):
        self._cache: Dict[str, Tuple[pd.DataFrame, datetime, int]] = {}

    def get(self, key: str) -> Optional[pd.DataFrame]:
        """캐시된 데이터 조회"""
        try:
            if key in self._cache:
                df, cached_time, ttl = self._cache[key]
                # TTL 체크
                if (datetime.now() - cached_time).total_seconds() < ttl:
                    return df.copy()
                # 만료된 캐시 삭제
                del self._cache[key]
            return None
        except Exception as e:
            logger.error(f"Error retrieving from cache: {str(e)}")
            return None

    def set(self, key: str, df: pd.DataFrame, ttl: int) -> None:
        """데이터 캐싱"""
        try:
            if not df.empty:
                self._cache[key] = (df.copy(), datetime.now(), ttl)
                # 캐시 크기 제한 (예: 최대 100개)
                if len(self._cache) > 100:
                    oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k][1])
                    del self._cache[oldest_key]
        except Exception as e:
            logger.error(f"Error setting cache: {str(e)}")

    def clear(self, pattern: Optional[str] = None) -> None:
        """캐시 삭제"""
        try:
            if pattern:
                keys_to_delete = [key for key in self._cache.keys() if pattern in key]
                for key in keys_to_delete:
                    del self._cache[key]
            else:
                self._cache.clear()
        except Exception as e:
            logger.error(f"Error clearing cache: {str(e)}")

    def get_stats(self) -> dict:
        """캐시 상태 정보"""
        try:
            return {"total_cached_items": len(self._cache), "memory_keys": list(self._cache.keys())}
        except Exception as e:
            logger.error(f"Error getting cache stats: {str(e)}")
            return {}
