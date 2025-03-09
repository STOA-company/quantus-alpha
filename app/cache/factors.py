from datetime import datetime
import json
from app.core.redis import redis_client
from app.database.crud import database


class FactorsCache:
    def __init__(self):
        self.redis = redis_client()
        # 국가와 자산 유형별 캐시 키 형식 정의
        self.factors_key_format = "factors:{country}:{asset_type}"
        self.last_update_key_format = "factors_last_update:{country}:{asset_type}"
        self.countries = ["kr", "us"]
        self.asset_types = ["stock", "etf"]

    def get_configs(self, country: str, asset_type: str) -> dict:
        """
        특정 국가 및 자산 유형에 대한 팩터 설정을 가져옵니다.

        Args:
            country: 국가 코드 ('kr' 또는 'us')
            asset_type: 자산 유형 ('stock' 또는 'etf')
        """
        if country not in self.countries or asset_type not in self.asset_types:
            raise ValueError(f"지원되지 않는 국가({country}) 또는 자산 유형({asset_type})입니다.")

        factors_key = self.factors_key_format.format(country=country, asset_type=asset_type)
        last_update_key = self.last_update_key_format.format(country=country, asset_type=asset_type)

        cached = self.redis.get(factors_key)
        last_update_str = self.redis.get(last_update_key)

        if not cached or not last_update_str:
            return self._update_configs(country, asset_type)

        configs = json.loads(cached)
        last_update = datetime.fromisoformat(last_update_str)

        if last_update.date() < datetime.now().date():  # 하루 한번 캐싱
            return self._update_configs(country, asset_type)

        return configs

    def _update_configs(self, country: str, asset_type: str) -> dict:
        """특정 국가 및 자산 유형에 대한 팩터 설정을 업데이트합니다."""
        condition = {}
        if asset_type == "stock":
            condition["is_stock"] = True
        elif asset_type == "etf":
            condition["is_etf"] = True

        factors = database._select(
            "factors",
            columns=["factor", "description", "unit", "category", "sort_direction"],
            **condition,
        )

        configs = {}
        for factor in factors:
            configs[factor.factor] = {
                "description": factor.description,
                "unit": factor.unit,
                "category": factor.category,
                "direction": factor.sort_direction,
                "range": (factor.min_value, factor.max_value),
            }

        factors_key = self.factors_key_format.format(country=country, asset_type=asset_type)
        last_update_key = self.last_update_key_format.format(country=country, asset_type=asset_type)

        self.redis.set(factors_key, json.dumps(configs))
        self.redis.set(last_update_key, datetime.now().isoformat())

        return configs

    def force_update(self, country: str = None, asset_type: str = None) -> dict:
        """
        캐시를 강제로 업데이트하는 함수

        Args:
            country: 특정 국가만 업데이트하려면 지정 ('kr' 또는 'us')
            asset_type: 특정 자산 유형만 업데이트하려면 지정 ('stock' 또는 'etf')
        """
        if country and asset_type:
            self.clear_cache(country, asset_type)
            return self._update_configs(country, asset_type)

        results = {}
        for country in self.countries:
            for asset_type in self.asset_types:
                self.clear_cache(country, asset_type)
                results[f"{country}_{asset_type}"] = self._update_configs(country, asset_type)

        return results

    def clear_cache(self, country: str = None, asset_type: str = None) -> None:
        """
        캐시를 삭제하는 함수

        Args:
            country: 특정 국가만 삭제하려면 지정 ('kr' 또는 'us')
            asset_type: 특정 자산 유형만 삭제하려면 지정 ('stock' 또는 'etf')
        """
        if country and asset_type:
            factors_key = self.factors_key_format.format(country=country, asset_type=asset_type)
            last_update_key = self.last_update_key_format.format(country=country, asset_type=asset_type)
            self.redis.delete(factors_key)
            self.redis.delete(last_update_key)
            return

        for country in self.countries:
            for asset_type in self.asset_types:
                factors_key = self.factors_key_format.format(country=country, asset_type=asset_type)
                last_update_key = self.last_update_key_format.format(country=country, asset_type=asset_type)
                self.redis.delete(factors_key)
                self.redis.delete(last_update_key)


factors_cache = FactorsCache()

if __name__ == "__main__":
    factors_cache.force_update()
