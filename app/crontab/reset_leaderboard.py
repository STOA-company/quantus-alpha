from app.cache.leaderboard import leaderboard_cache
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)


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


if __name__ == "__main__":
    reset_leaderboard_cache()
