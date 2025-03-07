from datetime import datetime
from typing import List, Dict
from app.core.redis import redis_client
from app.modules.common.enum import TranslateCountry


class Leaderboard:
    def __init__(self):
        self.redis = redis_client()
        self.DAILY_SEARCH_LEADERBOARD = "daily_search_leaderboard"

    def increment_score(self, ticker: str, kr_name: str, en_name: str) -> None:
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
        key = self.DAILY_SEARCH_LEADERBOARD

        leaders = self.redis.zrevrange(key, start, end, withscores=True)

        result = []
        for ticker, score in leaders:
            stock_info = self.redis.hgetall(f"stock:{ticker}")
            if stock_info:
                if lang == TranslateCountry.KO:
                    name = stock_info["kr_name"]
                else:
                    name = stock_info["en_name"]
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


if __name__ == "__main__":
    redis = Leaderboard()
    redis.reset_daily_leaderboard()
