from app.database.crud import database
from typing import Optional, List, Dict
import logging
from app.utils.factor_utils import filter_stocks, get_filtered_stocks_df, MarketEnum
from app.utils.score_utils import calculate_factor_score
from app.utils.pandas_utils import df_to_dict

logger = logging.getLogger(__name__)


class ScreenerService:
    def __init__(self):
        self.database = database

    def get_factors(self):
        try:
            factors = self.database._select(table="factors")

            # SQLAlchemy -> Dict
            if isinstance(factors, (list, tuple)):
                return [
                    {
                        "factor": factor.factor,
                        "description": factor.description,
                        "unit": str(factor.unit).lower(),
                        "category": str(factor.category).lower(),
                    }
                    for factor in factors
                ]
            else:
                factor = factors
                return {
                    "factor": factor.factor,
                    "description": factor.description,
                    "unit": str(factor.unit).lower(),
                    "category": str(factor.category).lower(),
                }

        except Exception as e:
            logger.error(f"Error in get_factors: {e}")
            raise e

    def get_filtered_stocks(
        self,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        columns: Optional[List[str]] = None,
    ) -> List[Dict]:
        try:
            stocks = filter_stocks(market_filter, sector_filter, custom_filters)
            filtered_df = get_filtered_stocks_df(market_filter, stocks, columns)
            scored_df = calculate_factor_score(filtered_df)
            sorted_df = filtered_df.merge(scored_df, on="Code", how="inner")
            sorted_df = sorted_df.sort_values(by="score", ascending=False)
            stocks_data = df_to_dict(sorted_df)
            return stocks_data
        except Exception as e:
            logger.error(f"Error in get_filtered_stocks: {e}")
            raise e

    def create_filter(self, user_id: int, name: str, conditions: List[Dict]) -> bool:
        try:
            last_filter = self.database._select(table="screener_filters", user_id=user_id, order="order", ascending=False)
            if last_filter:
                order = last_filter[0].order + 1
            else:
                order = 1
            self.database._insert(table="screener_filters", sets={"user_id": user_id, "name": name, "order": order})
            filter_id = self.database._select(table="screener_filters", user_id=user_id, name=name)[0].id
            for condition in conditions:
                self.database._insert(
                    table="screener_filter_conditions",
                    sets={
                        "filter_id": filter_id,
                        "factor": condition.factor,
                        "above": condition.above,
                        "below": condition.below,
                    },
                )
            return True
        except Exception as e:
            logger.error(f"Error in create_filter: {e}")
            raise e

    def update_filter(self, filter_id: int, name: Optional[str] = None, conditions: Optional[List[Dict]] = None) -> bool:
        try:
            if name:
                self.database._update(table="screener_filters", filter_id=filter_id, name=name)
            if conditions:
                self.database._delete(table="screener_filter_conditions", filter_id=filter_id)
                for condition in conditions:
                    self.database._insert(
                        table="screener_filter_conditions",
                        sets={
                            "filter_id": filter_id,
                            "factor": condition.factor,
                            "above": condition.above,
                            "below": condition.below,
                        },
                    )
            return True
        except Exception as e:
            logger.error(f"Error in update_filter: {e}")
            raise e

    def delete_filter(self, filter_id: int) -> bool:
        try:
            self.database._delete(table="screener_filters", filter_id=filter_id)
            return True
        except Exception as e:
            logger.error(f"Error in delete_filter: {e}")
            raise e

    def get_saved_filters(self, user_id: str) -> List[Dict]:
        try:
            filters = self.database._select(table="screener_filters", user_id=user_id, order="order", ascending=True)
            return filters
        except Exception as e:
            logger.error(f"Error in get_saved_filters: {e}")
            raise e

    def reorder_filters(self, filters: List[int]) -> bool:
        try:
            for index, filter_id in enumerate(filters):
                self.database._update(table="screener_filters", filter_id=filter_id, order=index + 1)
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

    def create_column_set(self, columns: List[str], user_id: str, name: str) -> bool:
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
            self.database._update(table="screener_column_sets", column_set_id=column_set_id, name=name)
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


screener_service = ScreenerService()
