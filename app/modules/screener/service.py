from app.database.crud import database_service
from typing import Optional, List, Dict, Tuple
import logging
from app.utils.score_utils import calculate_factor_score, calculate_factor_score_with_description
from app.cache.factors import factors_cache
import pandas as pd
import numpy as np
from app.models.models_factors import CategoryEnum
from app.modules.screener.schemas import MarketEnum
from app.utils.factor_utils import factor_utils
from app.common.constants import FACTOR_MAP

logger = logging.getLogger(__name__)


class ScreenerService:
    def __init__(self):
        self.database = database_service

    def get_factors(self):
        try:
            factors = factor_utils.get_factors()
            return factors

        except Exception as e:
            logger.error(f"Error in get_factors: {e}")
            raise e

    def get_filtered_stocks(
        self,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = 50,
        offset: Optional[int] = 0,
    ) -> Tuple[List[Dict], bool]:
        try:
            stocks = factor_utils.filter_stocks(market_filter, sector_filter, custom_filters)
            filtered_df = factor_utils.get_filtered_stocks_df(market_filter, stocks, columns)
            scored_df = calculate_factor_score(filtered_df)
            merged_df = filtered_df.merge(scored_df, on="Code", how="inner")
            sorted_df = merged_df.sort_values(by="score", ascending=False).reset_index(drop=True)
            if market_filter in [MarketEnum.US, MarketEnum.SNP500, MarketEnum.NASDAQ]:
                sorted_df["Code"] = sorted_df["Code"].str.replace("-US", "")

            total_count = len(sorted_df)
            sorted_df = sorted_df.iloc[offset * limit : offset * limit + limit]
            has_next = offset * limit + limit < total_count

            factors = factors_cache.get_configs()
            result = []

            for _, row in sorted_df.iterrows():
                # 기본으로 표시될 컬럼들
                stock_data = {
                    "Code": row["Code"],
                    "Name": row["Name"],
                    "ExchMnem": row["ExchMnem"],
                    "sector": row["sector"],
                    "country": row["country"],
                }

                # 숫자형 데이터 처리
                for col in sorted_df.columns:
                    if col in ["Code", "Name", "ExchMnem", "sector", "country"]:
                        continue

                    if pd.isna(row[col]) or np.isinf(row[col]):  # NA / INF -> 빈 문자열
                        stock_data[col] = {"value": "", "unit": ""}
                    else:
                        is_small_price = col == "close"
                        value, unit = factor_utils.convert_unit_and_value(
                            market_filter,
                            float(row[col]),
                            factors[col].get("unit", "") if col in factors else "",
                            is_small_price,
                        )

                        stock_data[col] = {"value": value, "unit": unit}

                result.append(stock_data)

            mapped_result = []
            for item in result:
                mapped_item = {}
                for key, value in item.items():
                    mapped_key = FACTOR_MAP.get(key, key)
                    mapped_item[mapped_key] = value
                mapped_result.append(mapped_item)

            return mapped_result, has_next

        except Exception as e:
            logger.error(f"Error in get_filtered_stocks: {e}")
            raise e

    def get_filtered_stocks_with_description(
        self,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = 50,
        offset: Optional[int] = 0,
    ) -> Tuple[List[Dict], bool]:
        try:
            stocks = factor_utils.filter_stocks(market_filter, sector_filter, custom_filters)
            filtered_df = factor_utils.get_filtered_stocks_df(market_filter, stocks, columns)
            scored_df = calculate_factor_score_with_description(filtered_df)
            merged_df = filtered_df.merge(scored_df, on="Code", how="inner")
            sorted_df = merged_df.sort_values(by="score", ascending=False).reset_index(drop=True)
            if market_filter in [MarketEnum.US, MarketEnum.SNP500, MarketEnum.NASDAQ]:
                sorted_df["Code"] = sorted_df["Code"].str.replace("-US", "")

            total_count = len(sorted_df)
            sorted_df = sorted_df.iloc[offset * limit : offset * limit + limit]
            has_next = offset * limit + limit < total_count

            factors = factors_cache.get_configs()
            result = []

            for _, row in sorted_df.iterrows():
                # 기본으로 표시될 컬럼들
                stock_data = {
                    "Code": row["Code"],
                    "Name": row["Name"],
                    "ExchMnem": row["ExchMnem"],
                    "sector": row["sector"],
                    "country": row["country"],
                    "description": row["description"],
                }

                # 숫자형 데이터 처리
                for col in sorted_df.columns:
                    if col in ["Code", "Name", "ExchMnem", "sector", "country", "description"]:
                        continue

                    if pd.isna(row[col]) or np.isinf(row[col]):  # NA / INF -> 빈 문자열
                        stock_data[col] = {"value": "", "unit": ""}
                    else:
                        is_small_price = col == "close"
                        value, unit = factor_utils.convert_unit_and_value(
                            market_filter,
                            float(row[col]),
                            factors[col].get("unit", "") if col in factors else "",
                            is_small_price,
                        )

                        stock_data[col] = {"value": value, "unit": unit}

                result.append(stock_data)

            mapped_result = []
            for item in result:
                mapped_item = {}
                for key, value in item.items():
                    mapped_key = FACTOR_MAP.get(key, key)
                    mapped_item[mapped_key] = value
                mapped_result.append(mapped_item)

            return mapped_result, has_next

        except Exception as e:
            logger.error(f"Error in get_filtered_stocks: {e}")
            raise e

    def create_filter_group(
        self,
        user_id: int,
        name: str,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
    ) -> bool:
        try:
            last_filter = self.database._select(
                table="screener_filter_groups", user_id=user_id, order="order", ascending=False
            )
            if last_filter:
                order = last_filter[0].order + 1
            else:
                order = 1
            self.database._insert(table="screener_filter_groups", sets={"user_id": user_id, "name": name, "order": order})
            filter_group_id = self.database._select(table="screener_filter_groups", user_id=user_id, name=name)[0].id
            if market_filter:
                self.database._insert(
                    table="screener_filter_conditions",
                    sets={"filter_group_id": filter_group_id, "factor": "market", "value": market_filter},
                )

            if sector_filter:
                for sector in sector_filter:
                    self.database._insert(
                        table="screener_filter_conditions",
                        sets={"filter_group_id": filter_group_id, "factor": "sector", "value": sector},
                    )

            if custom_filters:
                for condition in custom_filters:
                    self.database._insert(
                        table="screener_filter_conditions",
                        sets={
                            "filter_group_id": filter_group_id,
                            "factor": condition.factor,
                            "above": condition.above,
                            "below": condition.below,
                        },
                    )

            return True

        except Exception as e:
            logger.error(f"Error in create_filter: {e}")
            raise e

    def update_filter_group(
        self,
        filter_group_id: int,
        name: str,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
    ) -> bool:
        try:
            if name:
                self.database._update(table="screener_filter_groups", id=filter_group_id, sets={"name": name})

            self.database._delete(table="screener_filter_conditions", filter_group_id=filter_group_id)

            if custom_filters:
                for condition in custom_filters:
                    self.database._insert(
                        table="screener_filter_conditions",
                        sets={
                            "filter_group_id": filter_group_id,
                            "factor": condition.factor,
                            "above": condition.above,
                            "below": condition.below,
                        },
                    )

            if market_filter:
                self.database._insert(
                    table="screener_filter_conditions",
                    sets={
                        "filter_group_id": filter_group_id,
                        "factor": "market",
                        "value": market_filter,
                    },
                )

            if sector_filter:
                for sector in sector_filter:
                    self.database._insert(
                        table="screener_filter_conditions",
                        sets={
                            "filter_group_id": filter_group_id,
                            "factor": "sector",
                            "value": sector,
                        },
                    )

            return True
        except Exception as e:
            logger.error(f"Error in update_filter: {e}")
            raise e

    def delete_filter_group(self, filter_group_id: int) -> bool:
        try:
            self.database._delete(table="screener_filter_groups", id=filter_group_id)
            return True
        except Exception as e:
            logger.error(f"Error in delete_filter: {e}")
            raise e

    def get_saved_filter_groups(self, user_id: str) -> List[Dict]:
        try:
            filters = self.database._select(
                table="screener_filter_groups", user_id=user_id, order="order", ascending=True
            )
            return filters
        except Exception as e:
            logger.error(f"Error in get_saved_filter_groups: {e}")
            raise e

    def reorder_filter_groups(self, filter_groups: List[int]) -> bool:
        try:
            for index, filter_group_id in enumerate(filter_groups):
                self.database._update(table="screener_filter_groups", id=filter_group_id, order=index + 1)
            return True
        except Exception as e:
            logger.error(f"Error in reorder_filters: {e}")
            raise e

    def get_column_sets(self, user_id: str) -> List[Dict]:
        try:
            column_sets = self.database._select(table="screener_column_sets", user_id=user_id)
            return [
                {
                    "id": column_set.id,
                    "name": column_set.name,
                    "columns": [column.factor for column in column_set.columns],
                }
                for column_set in column_sets
            ]
        except Exception as e:
            logger.error(f"Error in get_column_sets: {e}")
            raise e

    def create_column_set(self, user_id: str, name: str, columns: List[str]) -> bool:
        try:
            self.database._insert(table="screener_column_sets", sets={"user_id": user_id, "name": name})
            column_set_id = self.database._select(table="screener_column_sets", user_id=user_id, name=name)[0].id
            for column in columns:
                self.database._insert(table="screener_columns", sets={"column_set_id": column_set_id, "factor": column})
            return True
        except Exception as e:
            logger.error(f"Error in create_column_set: {e}")
            raise e

    def update_column_set(self, column_set_id: int, name: str, columns: List[str]) -> bool:
        try:
            self.database._update(table="screener_column_sets", id=column_set_id, sets={"name": name})
            self.database._delete(table="screener_columns", column_set_id=column_set_id)
            for column in columns:
                self.database._insert(table="screener_columns", sets={"column_set_id": column_set_id, "factor": column})
            return True
        except Exception as e:
            logger.error(f"Error in update_column_set: {e}")
            raise e

    def delete_column_set(self, column_set_id: int) -> bool:
        try:
            self.database._delete(table="screener_column_sets", column_set_id=column_set_id)
            return True
        except Exception as e:
            logger.error(f"Error in delete_column_set: {e}")
            raise e

    def get_columns(self, category: Optional[CategoryEnum] = None, id: Optional[int] = None) -> List[str]:
        try:
            if category:
                columns = factor_utils.get_columns(category)
            elif id:
                column_set = self.database._select(table="screener_column_sets", columns=["id"], id=id)
                columns = self.database._select(table="screener_columns", columns=["factor"], column_set_id=column_set.id)
            else:
                raise ValueError("Invalid category or id")
            return [column for column in columns]
        except Exception as e:
            logger.error(f"Error in get_columns: {e}")
            raise e


screener_service = ScreenerService()
