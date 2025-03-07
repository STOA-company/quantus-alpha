from app.database.crud import database_service
from typing import Optional, List, Dict, Tuple
import logging
from app.utils.score_utils import calculate_factor_score, calculate_factor_score_with_description
from app.cache.factors import factors_cache
import pandas as pd
import numpy as np
from app.modules.screener.schemas import MarketEnum
from app.utils.factor_utils import factor_utils
from app.enum.type import StockType
from app.common.constants import FACTOR_MAP, NON_NUMERIC_COLUMNS, REVERSE_FACTOR_MAP, FACTOR_MAP_EN
from app.core.exception.custom import CustomException

logger = logging.getLogger(__name__)


class ScreenerService:
    def __init__(self):
        self.database = database_service

    def get_factors(self, market: Optional[MarketEnum] = None):
        try:
            factors = factor_utils.get_factors(market)

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
        sort_by: Optional[str] = "score",
        ascending: Optional[bool] = False,
        lang: Optional[str] = "kr",
    ) -> Tuple[List[Dict], int]:
        try:
            if sort_by not in columns and sort_by not in ["Code", "Name", "country", "market", "sector", "score"]:
                raise CustomException(status_code=400, message="sort_by must be in columns")

            if sector_filter:
                for sector in sector_filter:
                    if sector not in self.get_available_sectors():
                        raise CustomException(status_code=400, message=f"Invalid sector: {sector}")

            stocks = factor_utils.filter_stocks(market_filter, sector_filter, custom_filters)
            filtered_df = factor_utils.get_filtered_stocks_df(market_filter, stocks, columns)
            scored_df = calculate_factor_score(filtered_df)
            if scored_df.empty:
                return [], 0
            merged_df = filtered_df.merge(scored_df, on="Code", how="inner")
            sorted_df = merged_df.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)
            if market_filter in [MarketEnum.US, MarketEnum.SNP500, MarketEnum.NASDAQ]:
                sorted_df["Code"] = sorted_df["Code"].str.replace("-US", "")

            total_count = len(sorted_df)
            sorted_df = sorted_df.iloc[offset * limit : offset * limit + limit]

            factors = factors_cache.get_configs()
            result = []

            if columns:
                ordered_columns = []
                for col in columns:
                    mapped_col = next((k for k, v in FACTOR_MAP.items() if v == col), col)
                    if mapped_col not in ordered_columns:
                        ordered_columns.append(mapped_col)
            else:
                ordered_columns = ["Code", "Name", "score", "country"]

            selected_columns = ordered_columns.copy()

            sorted_df = sorted_df[selected_columns]

            for _, row in sorted_df.iterrows():
                stock_data = {}

                for col in selected_columns:
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
                                lang,
                            )
                            stock_data[col] = {"value": value, "unit": unit}

                result.append(stock_data)

            factor_map = FACTOR_MAP
            if lang == "en":
                factor_map = FACTOR_MAP_EN

            mapped_result = []
            for item in result:
                mapped_item = {}
                for key in ordered_columns:
                    if key in item:
                        mapped_key = factor_map.get(key, key)
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
        sort_by: Optional[str] = "score",
        ascending: Optional[bool] = False,
        lang: Optional[str] = "kr",
    ) -> Tuple[List[Dict], int]:
        try:
            stocks = factor_utils.filter_stocks(market_filter, sector_filter, custom_filters)
            filtered_df = factor_utils.get_filtered_stocks_df(market_filter, stocks, columns)

            scored_df = calculate_factor_score_with_description(filtered_df)
            if scored_df.empty:
                return [], 0

            merged_df = filtered_df.merge(scored_df, on="Code", how="inner")
            sorted_df = merged_df.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)

            if market_filter in [MarketEnum.US, MarketEnum.SNP500, MarketEnum.NASDAQ]:
                sorted_df["Code"] = sorted_df["Code"].str.replace("-US", "")

            total_count = len(sorted_df)
            sorted_df = sorted_df.iloc[offset * limit : offset * limit + limit]

            factors = factors_cache.get_configs()
            result = []

            if columns:
                ordered_columns = []
                for col in columns:
                    mapped_col = next((k for k, v in FACTOR_MAP.items() if v == col), col)
                    if mapped_col not in ordered_columns:
                        ordered_columns.append(mapped_col)
            else:
                ordered_columns = ["Code", "Name", "score", "country"]

            if "description" not in ordered_columns and "description" in sorted_df.columns:
                ordered_columns.append("description")

            selected_columns = ordered_columns.copy()

            available_columns = [col for col in selected_columns if col in sorted_df.columns]
            sorted_df = sorted_df[available_columns]

            for _, row in sorted_df.iterrows():
                stock_data = {}

                for col in available_columns:
                    if col in NON_NUMERIC_COLUMNS or col == "description":
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
                                lang,
                            )
                            stock_data[col] = {"value": value, "unit": unit}

                result.append(stock_data)

            factor_map = FACTOR_MAP
            if lang == "en":
                factor_map = FACTOR_MAP_EN

            mapped_result = []
            for item in result:
                mapped_item = {}
                for key in ordered_columns:
                    if key in item:
                        mapped_key = factor_map.get(key, key)
                        mapped_item[mapped_key] = item[key]
                mapped_result.append(mapped_item)

            return mapped_result, total_count

        except Exception as e:
            logger.error(f"Error in get_filtered_stocks_with_description: {e}")
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
        existing_groups = self.database._select(table="screener_groups", user_id=user_id, name=name, type=type)
        if existing_groups:
            raise CustomException(status_code=409, message="Group name already exists for this type")

        try:
            groups = self.database._select(table="screener_groups", user_id=user_id, order="order", ascending=False)
            if groups:
                order = groups[0].order + 1
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
                    if sector not in self.get_available_sectors():
                        raise CustomException(status_code=400, message=f"Invalid sector: {sector}")
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
                            "factor": REVERSE_FACTOR_MAP[condition.factor],
                            "above": condition.above,
                            "below": condition.below,
                        },
                    )

            # 팩터 필터
            if factor_filters:
                for idx, factor in enumerate(factor_filters):
                    self.database._insert(
                        table="screener_factor_filters",
                        sets={"group_id": group_id, "factor": REVERSE_FACTOR_MAP[factor], "order": idx + 1},
                    )

            return True

        except Exception as e:
            if hasattr(e, "orig") and "1062" in str(getattr(e, "orig", "")):
                raise CustomException(status_code=409, message="Group name already exists for this type")

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
                current_group = self.database._select(table="screener_groups", id=group_id)
                if not current_group:
                    raise ValueError(f"Group with id {group_id} not found")

                current_type = current_group[0].type

                existing_groups = self.database._select(
                    table="screener_groups", user_id=current_group[0].user_id, name=name, type=current_type
                )
                if existing_groups and any(group.id != group_id for group in existing_groups):
                    raise CustomException(status_code=409, message="Group name already exists for this type")

                self.database._update(table="screener_groups", id=group_id, sets={"name": name})

            # 종목 필터
            if custom_filters or market_filter or sector_filter:
                self.database._delete(table="screener_stock_filters", group_id=group_id)

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

            if custom_filters:
                for condition in custom_filters:
                    self.database._insert(
                        table="screener_stock_filters",
                        sets={
                            "group_id": group_id,
                            "factor": REVERSE_FACTOR_MAP[condition.factor],
                            "above": condition.above,
                            "below": condition.below,
                        },
                    )

            # 팩터 필터
            if factor_filters:
                self.database._delete(table="screener_factor_filters", group_id=group_id)
                for idx, factor in enumerate(factor_filters):
                    self.database._insert(
                        table="screener_factor_filters",
                        sets={"group_id": group_id, "factor": REVERSE_FACTOR_MAP[factor], "order": idx + 1},
                    )

            return True
        except Exception as e:
            if hasattr(e, "orig") and "1062" in str(getattr(e, "orig", "")):
                raise CustomException(status_code=409, message="Group name already exists for this type")

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
                    "type": group.type.lower(),
                }
                for group in groups
            ]
        except Exception as e:
            logger.error(f"Error in get_groups: {e}")
            raise e

    def get_group_filters(self, group_id: int) -> Dict:
        try:
            group = self.database._select(table="screener_groups", id=group_id)
            stock_filters = self.database._select(table="screener_stock_filters", group_id=group_id)
            factor_filters = self.database._select(table="screener_factor_filters", group_id=group_id)
            return {
                "name": group[0].name,
                "stock_filters": [
                    {
                        "factor": FACTOR_MAP[stock_filter.factor],
                        "value": stock_filter.value if stock_filter.value else None,
                        "above": stock_filter.above if stock_filter.above else None,
                        "below": stock_filter.below if stock_filter.below else None,
                    }
                    for stock_filter in stock_filters
                ],
                "factor_filters": [FACTOR_MAP[factor_filter.factor] for factor_filter in factor_filters],
            }
        except Exception as e:
            logger.error(f"Error in get_group_filters: {e}")
            raise e

    def reorder_groups(self, groups: List[int]) -> bool:
        try:
            # 각 그룹 ID에 대해 새로운 순서를 포함하는 데이터 리스트 생성
            update_data = [{"id": group_id, "order": index + 1} for index, group_id in enumerate(groups)]

            # bulk update 실행
            self.database._bulk_update(table="screener_groups", data=update_data, key_column="id")
            return True
        except Exception as e:
            logger.error(f"Error in reorder_groups: {e}")
            raise e

    def get_columns(self, group_id: Optional[int] = None) -> List[str]:
        try:
            if not group_id:
                return []
            group = self.database._select(table="screener_groups", columns=["id"], id=group_id)[0]
            factor_filters = self.database._select(table="screener_factor_filters", columns=["factor"], group_id=group.id)
            return [FACTOR_MAP[factor_filter.factor] for factor_filter in factor_filters]

        except Exception as e:
            logger.error(f"Error in get_columns: {e}")
            raise e

    def update_group_name(self, group_id: int, name: str) -> str:
        try:
            existing_groups = self.database._select(table="screener_groups", name=name)
            if existing_groups and any(group.id != group_id for group in existing_groups):
                raise CustomException(status_code=409, message="Group name already exists for this type")

            self.database._update(table="screener_groups", id=group_id, sets={"name": name})
            updated_group_name = self.database._select(table="screener_groups", id=group_id)[0].name
            if name == updated_group_name:
                return updated_group_name
            else:
                raise CustomException(status_code=500, message="Failed to update group name")
        except Exception as e:
            logger.error(f"Error in update_group_name: {e}")
            raise e

    def get_available_sectors(self, lang: str = "kr") -> List[str]:
        kr_df = pd.read_parquet("parquet/kr_stock_factors.parquet")
        us_df = pd.read_parquet("parquet/us_stock_factors.parquet")
        sector_lang = "sector" if lang == "kr" else "sector_en"
        kr_sectors = kr_df[sector_lang].unique().tolist()
        us_sectors = us_df[sector_lang].unique().tolist()

        sectors = list(set(kr_sectors + us_sectors))

        return sectors


def get_screener_service():
    return ScreenerService()
