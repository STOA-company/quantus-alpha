from datetime import datetime
import json
from app.core.redis import redis_client
from app.database.crud import database


class FactorsCache:
    def __init__(self):
        self.redis = redis_client()
        self.factors_key = "factors"
        self.last_update_key = "factors_last_update"

    def get_configs(self) -> dict:
        cached = self.redis.get(self.factors_key)
        last_update_str = self.redis.get(self.last_update_key)

        if not cached or not last_update_str:
            return self._update_configs()

        configs = json.loads(cached)
        last_update = datetime.fromisoformat(last_update_str)

        if last_update.date() < datetime.now().date():  # 하루 한번 캐싱
            return self._update_configs()

        return configs

    def _update_configs(self) -> dict:
        factors = database._select("factors", columns=["factor", "description", "unit", "category", "sort_direction"])

        configs = {}
        for factor in factors:
            configs[factor.factor] = {
                "description": factor.description,
                "unit": factor.unit,
                "category": factor.category,
                "direction": factor.sort_direction,
            }

        self.redis.set(self.factors_key, json.dumps(configs))
        self.redis.set(self.last_update_key, datetime.now().isoformat())

        return configs

    def force_update(self) -> dict:
        """캐시를 강제로 업데이트하는 함수"""
        self.clear_cache()
        return self._update_configs()

    def clear_cache(self) -> None:
        """캐시를 삭제하는 함수"""
        self.redis.delete(self.factors_key)
        self.redis.delete(self.last_update_key)


factors_cache = FactorsCache()

if __name__ == "__main__":
    factors_cache.force_update()
