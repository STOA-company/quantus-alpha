from app.database.crud import database
from typing import Optional, List, Dict
import logging
from app.models.models_factors import CategoryEnum
from fastapi import HTTPException
from app.utils.factor_utils import filter_stocks, get_stocks_data
from app.modules.screener.schemas import FilterRequest

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

    def get_filtered_stocks(self, filters: List[FilterRequest]) -> List[Dict]:
        try:
            stocks = filter_stocks(filters)
            stocks_data = get_stocks_data(stocks)
            return stocks_data
        except Exception as e:
            logger.error(f"Error in get_filtered_stocks: {e}")
            raise e


screener_service = ScreenerService()
