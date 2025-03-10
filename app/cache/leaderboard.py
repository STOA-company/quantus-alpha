from datetime import datetime
from typing import List, Dict
from decimal import Decimal
import json
import logging
from app.core.redis import redis_client
from app.modules.common.enum import TranslateCountry

logger = logging.getLogger(__name__)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class Leaderboard:
    def __init__(self):
        self.redis = redis_client()
        self.DAILY_SEARCH_LEADERBOARD = "daily_search_leaderboard"

    def increment_score(self, ticker: str, kr_name: str, en_name: str) -> None:
        """종목 검색 횟수 증가"""
        pipe = self.redis.pipeline()

        stock_info_key = f"stock:{ticker}"
        pipe.hset(
            stock_info_key,
            mapping={
                "ticker": ticker,
                "kr_name": kr_name,
                "en_name": en_name,
                "last_updated": datetime.now().isoformat(),
            },
        )

        pipe.zincrby(self.DAILY_SEARCH_LEADERBOARD, 1, ticker)
        pipe.execute()

    def get_leaderboard(self, lang: TranslateCountry, start: int = 0, end: int = 4) -> List[Dict]:
        """리더보드 조회"""
        key = self.DAILY_SEARCH_LEADERBOARD
        leaders = self.redis.zrevrange(key, start, end, withscores=True)

        result = []
        for ticker, score in leaders:
            stock_info = self.redis.hgetall(f"stock:{ticker}")
            if stock_info:
                name = stock_info["kr_name"] if lang == TranslateCountry.KO else stock_info["en_name"]
                result.append(
                    {
                        "rank": start + len(result) + 1,
                        "ticker": stock_info["ticker"],
                        "name": name,
                        "score": int(score),
                        "last_updated": stock_info["last_updated"],
                    }
                )

        return result

    def reset_daily_leaderboard(self):
        """일일 리더보드 초기화"""
        self.redis.delete(self.DAILY_SEARCH_LEADERBOARD)


leaderboard_cache = Leaderboard()