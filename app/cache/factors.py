from datetime import datetime
import json
from app.core.redis import redis_client
from app.database.crud import database
from typing import Literal

class FactorsCache:
    def __init__(self, asset_type: Literal["stock", "etf"] = "stock"):
        self.redis = redis_client()
        self.factors_key = f"{asset_type}_factors"
        self.last_update_key = f"{asset_type}_factors_last_update"
        self.asset_type = asset_type

    def get_configs(self) -> dict:
        """
        특정 국가 및 자산 유형에 대한 팩터 설정을 가져옵니다.

        Args:
            asset_type: 자산 유형 ('stock' 또는 'etf')
        """

        factors_key = self.factors_key
        last_update_key = self.last_update_key

        cached = self.redis.get(factors_key)
        last_update_str = self.redis.get(last_update_key)

        if not cached or not last_update_str:
            return self._update_configs()

        configs = json.loads(cached)

        return configs

    def _update_configs(self) -> dict:
        """특정 국가 및 자산 유형에 대한 팩터 설정을 업데이트합니다."""
        condition = {}
        if self.asset_type == "stock":
            condition["is_stock"] = True
        elif self.asset_type == "etf":
            condition["is_etf"] = True

        factors = database._select(
            "factors",
            columns=["factor", "description", "unit", "category", "sort_direction", "min_value", "max_value"],
            **condition,
        )

        configs = {}
        for factor in factors:
            configs[factor.factor] = {
                "description": factor.description,
                "unit": factor.unit,
                "category": factor.category,
                "direction": factor.sort_direction,
                "min_value": factor.min_value,
                "max_value": factor.max_value,
            }
        

        factors_key = self.factors_key
        last_update_key = self.last_update_key

        self.redis.set(factors_key, json.dumps(configs))
        self.redis.set(last_update_key, datetime.now().isoformat())

        return configs

    def force_update(self, asset_type: str = "stock") -> dict:
        """
        캐시를 강제로 업데이트하는 함수

        Args:
            asset_type: 특정 자산 유형만 업데이트하려면 지정 ('stock' 또는 'etf')
        """
        if asset_type:
            self.clear_cache()
            return self._update_configs()

        results = {}
        for asset_type in self.asset_types:
            self.clear_cache()
            results[f"{self.asset_type}"] = self._update_configs()

        return results

    def clear_cache(self) -> None:
        """
        캐시를 삭제하는 함수

        Args:
            asset_type: 특정 자산 유형만 삭제하려면 지정 ('stock' 또는 'etf')
        """
        factors_key = self.factors_key
        last_update_key = self.last_update_key
        self.redis.delete(factors_key)
        self.redis.delete(last_update_key)
        return


factors_cache = FactorsCache()

if __name__ == "__main__":
    factors_cache.force_update()