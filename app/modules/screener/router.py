from fastapi import APIRouter, HTTPException, Query
from typing import List
from app.modules.screener.service import screener_service
from app.modules.screener.schemas import FactorResponse
from app.models.models_factors import CategoryEnum
from typing import Optional
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/factors", response_model=List[FactorResponse])
def get_factors(
    category: Optional[CategoryEnum] = Query(
        default=None, description="Factor category filter", enum=[e.value for e in CategoryEnum]
    ),
):
    try:
        factors = screener_service.get_factors(category)
        return factors
    except Exception as e:
        logger.error(f"Error getting factors: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factors/search", response_model=List[FactorResponse])
def search_factors(query: str):
    try:
        factors = screener_service.search_factors(query)
        return factors
    except Exception as e:
        logger.error(f"Error searching factors: {e}")
        raise HTTPException(status_code=500, detail=str(e))
