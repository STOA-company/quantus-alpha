from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Dict
from app.modules.screener.service import screener_service
from app.modules.screener.schemas import FactorResponse, FilterCondition, Filter
from app.models.models_factors import CategoryEnum
from typing import Optional
import logging
from app.utils.oauth_utils import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/factors", response_model=List[FactorResponse])
def get_factors(
    category: Optional[CategoryEnum] = Query(
        default=None, description="Factor category filter", enum=[e.value for e in CategoryEnum]
    ),
):
    """
    팩터 카테고리별 팩터 목록 조회 (미선택 시 모든 팩터 조회)
    """
    try:
        factors = screener_service.get_factors(category)
        return [
            FactorResponse(
                factor=factor["factor"],
                description=factor["description"],
                unit=factor["unit"],
                sort_direction=factor["sort_direction"],
                category=factor["category"],
            )
            for factor in factors
        ]
    except Exception as e:
        logger.error(f"Error getting factors: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factors/search", response_model=List[FactorResponse])
def search_factors(query: str):
    """
    팩터 이름 검색
    """
    try:
        factors = screener_service.search_factors(query)
        return [
            FactorResponse(
                factor=factor["factor"],
                description=factor["description"],
                unit=factor["unit"],
                sort_direction=factor["sort_direction"],
                category=factor["category"],
            )
            for factor in factors
        ]
    except Exception as e:
        logger.error(f"Error searching factors: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factors/{factor}", response_model=FactorResponse)
def get_factor(factor: str):
    """
    단일 팩터 정보 조회
    """
    try:
        factor = screener_service.get_factor(factor)
        return FactorResponse(
            factor=factor["factor"],
            description=factor["description"],
            unit=factor["unit"],
            sort_direction=factor["sort_direction"],
            category=factor["category"],
        )
    except Exception as e:
        logger.error(f"Error getting single factor: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stocks", response_model=List[Dict])
def get_filtered_stocks(filters: List[FilterCondition] = None):
    """
    필터링된 종목들 조회
    필터에 해당하는 팩터들만 응답?
    """
    try:
        filters = [
            {"factor": condition.factor, "above": condition.above, "below": condition.below} for condition in filters
        ]
        stocks_data = screener_service.get_filtered_stocks(filters)
        return stocks_data
    except Exception as e:
        logger.error(f"Error getting filtered stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/filters", response_model=List[Dict])
def get_filters(current_user: str = Depends(get_current_user)):
    """
    저장된 필터 목록 조회
    """
    try:
        filters = screener_service.get_saved_filters(current_user.id)
        return filters
    except Exception as e:
        logger.error(f"Error getting saved filters: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/filters", response_model=Dict)
def create_filter(filter: Filter, current_user: str = Depends(get_current_user)):
    """
    필터 생성
    """
    try:
        is_success = screener_service.create_filter(current_user.id, filter.name, filter.conditions)
        if is_success:
            return {"message": "Filter created successfully"}
    except Exception as e:
        logger.error(f"Error creating filter: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/filters/{filter_id}", response_model=List[FilterCondition])
def get_filter(filter_id: int):
    """
    필터 조회
    """
    try:
        filter = screener_service.get_filter(filter_id)
        return [
            FilterCondition(factor=condition["factor"], above=condition["above"], below=condition["below"])
            for condition in filter
        ]
    except Exception as e:
        logger.error(f"Error getting filter: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/filters/{filter_id}", response_model=Dict)
def update_filter(filter_id: int, filter: Filter):
    """
    필터 수정
    """
    try:
        is_success = screener_service.update_filter(filter_id, filter.name, filter.conditions)
        if is_success:
            return {"message": "Filter updated successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to update filter")
    except Exception as e:
        logger.error(f"Error updating filter: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/filters/{filter_id}", response_model=Dict)
def delete_filter(filter_id: int):
    """
    필터 삭제
    """
    try:
        is_success = screener_service.delete_filter(filter_id)
        if is_success:
            return {"message": "Filter deleted successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to delete filter")
    except Exception as e:
        logger.error(f"Error deleting filter: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/filters/reorder", response_model=Dict)
def reorder_filters(filters: List[int]):
    """
    필터 순서 업데이트
    """
    try:
        is_success = screener_service.reorder_filters(filters)
        if is_success:
            return {"message": "Filter reordered successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to reorder filters")
    except Exception as e:
        logger.error(f"Error reordering filters: {e}")
        raise HTTPException(status_code=500, detail=str(e))
