from app.database.crud import database
from typing import Optional
import logging
from app.models.models_factors import CategoryEnum

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


screener_service = ScreenerService()
