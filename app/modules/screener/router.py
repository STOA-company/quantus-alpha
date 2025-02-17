from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict
from app.modules.screener.service import screener_service
from app.modules.screener.schemas import FactorResponse, FilterRequest
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


@router.get("/factors/{factor}", response_model=FactorResponse)
def get_factor(factor: str):
    try:
        factor = screener_service.get_factor(factor)
        return factor
    except Exception as e:
        logger.error(f"Error getting single factor: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factors/filter", response_model=List[Dict])
def get_filtered_stocks(filters: List[FilterRequest] = None):
    try:
        stock_datas = screener_service.get_filtered_stocks(filters)
        return stock_datas
    except Exception as e:
        logger.error(f"Error getting filtered stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))
