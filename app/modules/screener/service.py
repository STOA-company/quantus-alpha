from app.database.crud import database
from typing import Optional, List, Dict
import logging
from app.models.models_factors import CategoryEnum
from fastapi import HTTPException
from app.utils.factor_utils import filter_stocks, get_stocks_data

logger = logging.getLogger(__name__)


class ScreenerService:
    def __init__(self):
        self.database = database

    def get_factors(self, category: Optional[CategoryEnum] = None):
        try:
            if category is None:
                factors = self.database._select(table="factors")
            else:
                factors = self.database._select(table="factors", category=category)

            # SQLAlchemy -> Dict
            if isinstance(factors, (list, tuple)):
                return [
                    {
                        "factor": factor.factor,
                        "description": factor.description,
                        "unit": str(factor.unit).lower(),
                        "sort_direction": str(factor.sort_direction).lower(),
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
                    "sort_direction": str(factor.sort_direction).lower(),
                    "category": str(factor.category).lower(),
                }

        except Exception as e:
            logger.error(f"Error in get_factors: {e}")
            raise e

    def search_factors(self, query: str) -> list:
        try:
            search_conditions = {"factor__like": f"%{query}%"}
            results = self.database._select(table="factors", **search_conditions)

            # SQLAlchemy -> Dict
            if isinstance(results, (list, tuple)):
                return [
                    {
                        "factor": factor.factor,
                        "description": factor.description,
                        "unit": str(factor.unit).lower(),
                        "sort_direction": str(factor.sort_direction).lower(),
                        "category": str(factor.category).lower(),
                    }
                    for factor in results
                ]
            else:
                factor = results
                return {
                    "factor": factor.factor,
                    "description": factor.description,
                    "unit": str(factor.unit).lower(),
                    "sort_direction": str(factor.sort_direction).lower(),
                    "category": str(factor.category).lower(),
                }
        except Exception as e:
            logger.error(f"Error in search_factors: {e}")
            raise e

    def get_factor(self, factor: str):
        try:
            result = self.database._select(table="factors", factor=factor)[0]
            if not result:
                raise HTTPException(status_code=404, detail="Factor not found")

            return {
                "factor": result.factor,
                "description": result.description,
                "unit": str(result.unit).lower(),
                "sort_direction": str(result.sort_direction).lower(),
                "category": str(result.category).lower(),
            }
        except Exception as e:
            logger.error(f"Error in get_factor: {e}")
            raise e

    def get_filtered_stocks(self, filters: List[Dict]) -> List[Dict]:
        try:
            stocks = filter_stocks(filters)
            stocks_data = get_stocks_data(stocks)
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

    def get_filter(self, filter_id: int) -> List[Dict]:
        try:
            conditions = self.database._select(table="screener_filter_conditions", filter_id=filter_id)
            if not conditions:
                raise HTTPException(status_code=404, detail="Filter not found")
            return [
                {
                    "factor": condition.factor,
                    "above": float(condition.above) if condition.above is not None else None,
                    "below": float(condition.below) if condition.below is not None else None,
                }
                for condition in conditions
            ]
        except Exception as e:
            logger.error(f"Error in get_filter: {e}")
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


screener_service = ScreenerService()
