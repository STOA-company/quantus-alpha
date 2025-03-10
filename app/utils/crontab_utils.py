from app.utils.factor_utils import factor_utils
from app.cache.leaderboard import leaderboard_cache
from app.cache.factors import factors_cache
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)


def update_kr_parquet():
    """
    파퀴 업데이트
    """
    try:
        factor_utils.process_kr_factor_data()
        factor_utils.archive_parquet("kr")
        factors_cache.force_update(country="kr")
        return {"message": "Parquet updated successfully"}
    except Exception as e:
        logger.error(f"Error updating parquet: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def update_us_parquet():
    """
    파퀴 업데이트
    """
    try:
        factor_utils.process_us_factor_data()
        factor_utils.archive_parquet("us")
        factors_cache.force_update(country="us")
        return {"message": "Parquet updated successfully"}
    except Exception as e:
        logger.error(f"Error updating parquet: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def reset_leaderboard_cache():
    """
    리더보드 캐시 초기화
    """
    try:
        leaderboard_cache.reset_daily_leaderboard()
        return {"message": "Leaderboard cache reset successfully"}
    except Exception as e:
        logger.error(f"Error resetting leaderboard cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))