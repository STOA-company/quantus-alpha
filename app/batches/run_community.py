import logging
from datetime import datetime

from sqlalchemy import text

from app.database.crud import database

logger = logging.getLogger(__name__)


def update_post_statistics():
    """24시간 동안의 게시글 좋아요 통계 업데이트"""
    try:
        # 통계 업데이트 쿼리
        query = """
            INSERT INTO post_statistics (post_id, daily_likes, last_liked_at, created_at, updated_at)
            SELECT
                p.id,
                COUNT(pl.post_id) as daily_likes,
                MAX(pl.created_at) as last_liked_at,
                UTC_TIMESTAMP(),
                UTC_TIMESTAMP()
            FROM posts p
            JOIN post_likes pl ON p.id = pl.post_id
            WHERE pl.created_at >= UTC_TIMESTAMP() - INTERVAL 24 HOUR
            GROUP BY p.id
            ON DUPLICATE KEY UPDATE
                daily_likes = VALUES(daily_likes),
                last_liked_at = VALUES(last_liked_at),
                updated_at = UTC_TIMESTAMP()
        """

        result = database._execute(text(query))

        # 로깅
        affected_rows = result.rowcount
        logger.info(f"{datetime.now()}: Statistics updated for {affected_rows} posts")

        return True

    except Exception as e:
        print(f"Error updating post statistics: {str(e)}")
        return False


def update_stock_statistics():
    """24시간 동안의 종목별 게시글 통계 업데이트"""
    try:
        query = """
            INSERT INTO stock_statistics (stock_ticker, daily_post_count, last_tagged_at, created_at, updated_at)
            SELECT
                ps.stock_ticker,
                COUNT(DISTINCT p.id) as daily_post_count,
                MAX(p.created_at) as last_tagged_at,
                UTC_TIMESTAMP(),
                UTC_TIMESTAMP()
            FROM posts p
            JOIN post_stocks ps ON p.id = ps.post_id
            WHERE p.created_at >= UTC_TIMESTAMP() - INTERVAL 24 HOUR
            GROUP BY ps.stock_ticker
            ON DUPLICATE KEY UPDATE
                daily_post_count = VALUES(daily_post_count),
                last_tagged_at = VALUES(last_tagged_at),
                updated_at = UTC_TIMESTAMP()
        """

        result = database._execute(text(query))

        # 로깅
        affected_rows = result.rowcount
        logger.info(f"{datetime.now()}: Stock statistics updated for {affected_rows} stocks")

        return True

    except Exception as e:
        logger.error(f"Error updating stock statistics: {str(e)}")
        return False
