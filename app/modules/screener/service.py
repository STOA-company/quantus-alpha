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
from app.enum.type import StockType
from app.common.constants import FACTOR_MAP, NON_NUMERIC_COLUMNS, DEFAULT_COLUMNS

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
    ) -> Tuple[List[Dict], int]:
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

            factors = factors_cache.get_configs()
            result = []

            ordered_columns = []
            if columns:
                for col in columns:
                    mapped_col = next((k for k, v in FACTOR_MAP.items() if v == col), col)
                    if mapped_col not in ordered_columns:
                        ordered_columns.append(mapped_col)
            else:
                for col in sorted_df.columns:
                    if col not in ordered_columns and col != "score":
                        ordered_columns.append(col)

            for _, row in sorted_df.iterrows():
                stock_data = {}

                for col in ordered_columns:
                    if col in NON_NUMERIC_COLUMNS:
                        if col in row:
                            stock_data[col] = row[col]
                    elif col == "score":
                        stock_data[col] = {"value": float(row[col]), "unit": ""}
                    elif col in row:
                        if pd.isna(row[col]) or np.isinf(row[col]):  # NA / INF -> 빈 문자열
                            stock_data[col] = {"value": "", "unit": ""}
                        else:
                            value, unit = factor_utils.convert_unit_and_value(
                                market_filter,
                                float(row[col]),
                                factors[col].get("unit", "") if col in factors else "",
                            )
                            stock_data[col] = {"value": value, "unit": unit}

                result.append(stock_data)

            mapped_result = []
            for item in result:
                mapped_item = {}
                for key in ordered_columns:
                    if key in item:
                        mapped_key = FACTOR_MAP.get(key, key)
                        mapped_item[mapped_key] = item[key]
                mapped_result.append(mapped_item)

            return mapped_result, total_count

        except Exception as e:
            logger.error(f"Error in get_filtered_stocks: {e}")
            raise e

    def get_filtered_stocks_count(
        self,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        columns: Optional[List[str]] = None,
    ) -> int:
        try:
            stocks = factor_utils.filter_stocks(market_filter, sector_filter, custom_filters)
            filtered_df = factor_utils.get_filtered_stocks_df(market_filter, stocks, columns)

            return len(filtered_df)
        except Exception as e:
            logger.error(f"Error in get_filtered_stocks_count: {e}")
            raise e

    def get_filtered_stocks_with_description(
        self,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = 50,
        offset: Optional[int] = 0,
    ) -> Tuple[List[Dict], int]:
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

            factors = factors_cache.get_configs()
            result = []

            for _, row in sorted_df.iterrows():
                # 기본으로 표시될 컬럼들
                stock_data = {
                    "Code": row["Code"],
                    "Name": row["Name"],
                    "market": row["market"],
                    "sector": row["sector"],
                    "country": row["country"],
                    "description": row["description"],
                }

                # 숫자형 데이터 처리
                for col in sorted_df.columns:
                    if col in ["Code", "Name", "market", "sector", "country", "description"]:
                        continue

                    if pd.isna(row[col]) or np.isinf(row[col]):  # NA / INF -> 빈 문자열
                        stock_data[col] = {"value": "", "unit": ""}
                    else:
                        value, unit = factor_utils.convert_unit_and_value(
                            market_filter,
                            float(row[col]),
                            factors[col].get("unit", "") if col in factors else "",
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

            return mapped_result, total_count

        except Exception as e:
            logger.error(f"Error in get_filtered_stocks: {e}")
            raise e

    def create_group(
        self,
        user_id: int,
        name: str,
        type: Optional[StockType] = StockType.STOCK,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        factor_filters: Optional[List[str]] = None,
    ) -> bool:
        try:
            last_group = self.database._select(table="screener_groups", user_id=user_id, order="order", ascending=False)
            if last_group:
                order = last_group[0].order + 1
            else:
                order = 1

            self.database._insert(
                table="screener_groups", sets={"user_id": user_id, "name": name, "order": order, "type": type}
            )

            group_id = self.database._select(table="screener_groups", user_id=user_id, name=name)[0].id

            # 종목 필터
            if market_filter:
                self.database._insert(
                    table="screener_stock_filters",
                    sets={"group_id": group_id, "factor": "market", "value": market_filter},
                )

            if sector_filter:
                for sector in sector_filter:
                    self.database._insert(
                        table="screener_stock_filters",
                        sets={"group_id": group_id, "factor": "sector", "value": sector},
                    )

            if custom_filters:
                for condition in custom_filters:
                    self.database._insert(
                        table="screener_stock_filters",
                        sets={
                            "group_id": group_id,
                            "factor": condition.factor,
                            "above": condition.above,
                            "below": condition.below,
                        },
                    )

            # 팩터 필터
            if factor_filters:
                for idx, factor in enumerate(factor_filters):
                    self.database._insert(
                        table="screener_factor_filters",
                        sets={"group_id": group_id, "factor": factor, "order": idx + 1},
                    )

            return True

        except Exception as e:
            logger.error(f"Error in create_group: {e}")
            raise e

    def update_group(
        self,
        group_id: int,
        name: str,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        factor_filters: Optional[List[str]] = None,
    ) -> bool:
        try:
            if name:
                self.database._update(table="screener_groups", id=group_id, sets={"name": name})

            # 종목 필터
            if custom_filters or market_filter or sector_filter:
                self.database._delete(table="screener_stock_filters", group_id=group_id)

            if custom_filters:
                for condition in custom_filters:
                    self.database._insert(
                        table="screener_stock_filters",
                        sets={
                            "group_id": group_id,
                            "factor": condition.factor,
                            "above": condition.above,
                            "below": condition.below,
                        },
                    )

            if market_filter:
                self.database._insert(
                    table="screener_stock_filters",
                    sets={
                        "group_id": group_id,
                        "factor": "market",
                        "value": market_filter,
                    },
                )

            if sector_filter:
                for sector in sector_filter:
                    self.database._insert(
                        table="screener_stock_filters",
                        sets={
                            "group_id": group_id,
                            "factor": "sector",
                            "value": sector,
                        },
                    )

            # 팩터 필터
            if factor_filters:
                self.database._delete(table="screener_factor_filters", group_id=group_id)
                for idx, factor in enumerate(factor_filters):
                    self.database._insert(
                        table="screener_factor_filters",
                        sets={"group_id": group_id, "factor": factor, "order": idx + 1},
                    )

            return True
        except Exception as e:
            logger.error(f"Error in update_group: {e}")
            raise e

    def delete_group(self, group_id: int) -> bool:
        try:
            self.database._delete(table="screener_groups", id=group_id)  # CASCADE
            return True
        except Exception as e:
            logger.error(f"Error in delete_group: {e}")
            raise e

    def get_groups(self, user_id: str) -> List[Dict]:
        try:
            groups = self.database._select(table="screener_groups", user_id=user_id, order="order", ascending=True)
            return [
                {
                    "id": group.id,
                    "name": group.name,
                    "type": group.type,
                }
                for group in groups
            ]
        except Exception as e:
            logger.error(f"Error in get_groups: {e}")
            raise e

    def get_group_filters(self, group_id: int) -> Dict:
        try:
            stock_filters = self.database._select(table="screener_stock_filters", group_id=group_id)
            factor_filters = self.database._select(table="screener_factor_filters", group_id=group_id)
            return {
                "stock_filters": [
                    {
                        "factor": stock_filter.factor,
                        "value": stock_filter.value if stock_filter.value else None,
                        "above": stock_filter.above if stock_filter.above else None,
                        "below": stock_filter.below if stock_filter.below else None,
                    }
                    for stock_filter in stock_filters
                ],
                "factor_filters": [factor_filter.factor for factor_filter in factor_filters],
            }
        except Exception as e:
            logger.error(f"Error in get_group_filters: {e}")
            raise e

    def reorder_groups(self, groups: List[int]) -> bool:
        try:
            for index, group_id in enumerate(groups):
                self.database._update(table="screener_groups", id=group_id, order=index + 1)
            return True
        except Exception as e:
            logger.error(f"Error in reorder_groups: {e}")
            raise e

    def get_columns(self, category: Optional[CategoryEnum] = None, id: Optional[int] = None) -> List[str]:
        try:
            if category:
                columns = factor_utils.get_columns(category)
            elif id:
                group = self.database._select(table="screener_groups", columns=["id"], id=id)
                factor_filters = self.database._select(
                    table="screener_factor_filters", columns=["factor"], group_id=group.id
                )
                columns = [factor_filter.factor for factor_filter in factor_filters]
            else:
                raise ValueError("Category or GroupId is required")

            result = DEFAULT_COLUMNS + columns
            return [FACTOR_MAP[column] for column in result]
        except Exception as e:
            logger.error(f"Error in get_columns: {e}")
            raise e


screener_service = ScreenerService()
