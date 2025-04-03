import json
from app.core.redis import redis_client
from app.database.crud import database
from typing import Literal


class FactorsCache:
    def __init__(self, asset_type: Literal["stock", "etf"] = "stock"):
        self.redis = redis_client()
        self.factors_key = f"{asset_type}_factors"
        self.asset_type = asset_type

    def get_configs(self) -> dict:
        cached = self.redis.get(self.factors_key)

        if not cached:
            return self._update_configs()

        configs = json.loads(cached)

        return configs

    def _update_configs(self) -> dict:
        condition = {}
        if self.asset_type == "stock":
            condition["is_stock"] = True
        elif self.asset_type == "etf":
            condition["is_etf"] = True

        condition["is_active"] = True

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

        self.redis.set(self.factors_key, json.dumps(configs))

    def force_update(self) -> dict:
        self.clear_cache()
        self._update_configs()

    def clear_cache(self) -> None:
        self.redis.delete(self.factors_key)


factors_cache = FactorsCache()
etf_factors_cache = FactorsCache(asset_type="etf")
