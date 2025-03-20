from datetime import datetime
from typing import List, Dict, Optional
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


class BaseLeaderboard:
    """리더보드의 기본 기능을 제공하는 추상 클래스"""

    def __init__(self):
        self.redis = redis_client()

    def get_leaderboard(self, lang: TranslateCountry, start: int = 0, end: int = 4) -> List[Dict]:
        """리더보드 조회 - 자식 클래스에서 구현"""
        raise NotImplementedError

    def reset_daily_leaderboard(self, key: str):
        """일일 리더보드 초기화"""
        self.redis.delete(key)


class StockLeaderboard(BaseLeaderboard):
    """종목 검색 리더보드"""

    def __init__(self):
        super().__init__()
        self.DAILY_SEARCH_LEADERBOARD = "daily_stock_search_leaderboard"

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
        """종목 리더보드 조회"""
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
        """일일 종목 리더보드 초기화"""
        super().reset_daily_leaderboard(self.DAILY_SEARCH_LEADERBOARD)


class NewsLeaderboard(BaseLeaderboard):
    """뉴스 검색 리더보드"""

    def __init__(self):
        super().__init__()
        self.DAILY_SEARCH_LEADERBOARD = "daily_news_search_leaderboard"

    def increment_score(self, news_id: int, ticker: str) -> None:
        """뉴스 검색 횟수 증가"""
        pipe = self.redis.pipeline()

        news_info_key = f"news:{news_id}"
        pipe.hset(
            news_info_key,
            mapping={
                "news_id": news_id,
                "ticker": ticker,
                "last_updated": datetime.now().isoformat(),
            },
        )

        pipe.zincrby(self.DAILY_SEARCH_LEADERBOARD, 1, news_id)
        pipe.execute()

    def get_leaderboard(
        self, lang: TranslateCountry, tickers: Optional[List[str]] = None, start: int = 0, end: int = 4
    ) -> List[Dict]:
        """뉴스 리더보드 조회"""
        key = self.DAILY_SEARCH_LEADERBOARD
        leaders = self.redis.zrevrange(key, start, end, withscores=True)

        result = []
        for news_id, score in leaders:
            news_info = self.redis.hgetall(f"news:{news_id}")
            if news_info:
                title = news_info["kr_title"] if lang == TranslateCountry.KO else news_info["en_title"]
                if tickers:
                    if news_info["ticker"] in tickers:
                        result.append(
                            {
                                "rank": start + len(result) + 1,
                                "news_id": news_info["news_id"],
                                "title": title,
                                "score": int(score),
                                "last_updated": news_info["last_updated"],
                            }
                        )
                else:
                    result.append(
                        {
                            "rank": start + len(result) + 1,
                            "news_id": news_info["news_id"],
                            "title": title,
                            "score": int(score),
                            "last_updated": news_info["last_updated"],
                        }
                    )

        return result

    def reset_daily_leaderboard(self):
        """일일 뉴스 리더보드 초기화"""
        super().reset_daily_leaderboard(self.DAILY_SEARCH_LEADERBOARD)
