from fastapi import APIRouter, HTTPException, Depends, Response
from typing import List, Dict, Optional
import io
from app.modules.screener.service import screener_service
from app.modules.screener.schemas import (
    FactorResponse,
    FilterGroup,
    FilteredStocks,
    ColumnSetUpdate,
    ColumnSet,
    ColumnsResponse,
)
import logging
from app.utils.oauth_utils import get_current_user
from app.utils.factor_utils import factor_utils
from app.models.models_factors import CategoryEnum

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/factors", response_model=List[FactorResponse])
def get_factors():
    """
    모든 팩터 조회
    """
    try:
        factors = screener_service.get_factors()
        return [
            FactorResponse(
                factor=factor["factor"],
                description=factor["description"],
                unit=factor["unit"],
                category=factor["category"],
            )
            for factor in factors
        ]
    except Exception as e:
        logger.error(f"Error getting factors: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stocks", response_model=Dict)
def get_filtered_stocks(filtered_stocks: FilteredStocks):
    """
    필터링된 종목들 조회

    market_filter : ["us", "kr", "S&P 500", "NASDAQ", "KOSPI", "KOSDAQ"] 중 하나
    """
    try:
        custom_filters = []
        if filtered_stocks.custom_filters:
            custom_filters = [
                {
                    "factor": condition.factor,
                    "above": condition.above,
                    "below": condition.below,
                }
                for condition in filtered_stocks.custom_filters
            ]
        stocks_data, has_next = screener_service.get_filtered_stocks(
            filtered_stocks.market_filter,
            filtered_stocks.sector_filter,
            custom_filters,
            filtered_stocks.columns,
            filtered_stocks.limit,
            filtered_stocks.offset,
        )

        result = {"has_next": has_next, "data": stocks_data}
        return result
    except Exception as e:
        logger.error(f"Error getting filtered stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stocks/download")
def download_filtered_stocks(filtered_stocks: FilteredStocks):
    custom_filters = []
    if filtered_stocks.custom_filters:
        custom_filters = [
            {
                "factor": condition.factor,
                "above": condition.above,
                "below": condition.below,
            }
            for condition in filtered_stocks.custom_filters
        ]
    sorted_df = screener_service.get_filtered_stocks(
        filtered_stocks.market_filter, filtered_stocks.sector_filter, custom_filters, filtered_stocks.columns
    )

    stream = io.StringIO()
    sorted_df.to_csv(stream, index=False, encoding="utf-8-sig")  # 한글 인코딩

    return Response(
        content=stream.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="filtered_stocks.csv"'},
    )


@router.get("/filter-groups", response_model=List[Dict])
def get_filter_groups(current_user: str = Depends(get_current_user)):
    """
    저장된 필터 목록 조회
    """
    try:
        filters = screener_service.get_saved_filter_groups(current_user.id)
        return filters
    except Exception as e:
        logger.error(f"Error getting saved filters: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/filter-groups", response_model=Dict)
def create_or_update_filter_group(filter_group: FilterGroup, current_user: str = Depends(get_current_user)):
    """
    필터 생성 또는 업데이트
    """
    try:
        if filter_group.id:
            is_success = screener_service.update_filter_group(
                filter_group.id,
                filter_group.name,
                filter_group.market_filter,
                filter_group.sector_filter,
                filter_group.custom_filters,
            )
            message = "Filter updated successfully"
        else:
            is_success = screener_service.create_filter_group(
                current_user.id,
                filter_group.name,
                filter_group.market_filter,
                filter_group.sector_filter,
                filter_group.custom_filters,
            )
            message = "Filter created successfully"
        if is_success:
            return {"message": message}
    except Exception as e:
        logger.error(f"Error creating filter: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/filter-groups/{filter_group_id}", response_model=Dict)
def delete_filter_group(filter_group_id: int):
    """
    필터 삭제
    """
    try:
        is_success = screener_service.delete_filter_group(filter_group_id)
        if is_success:
            return {"message": "Filter deleted successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to delete filter")
    except Exception as e:
        logger.error(f"Error deleting filter: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/filter-groups/reorder", response_model=Dict)
def reorder_filter_groups(filter_groups: List[int]):
    """
    필터 순서 업데이트
    """
    try:
        is_success = screener_service.reorder_filter_groups(filter_groups)
        if is_success:
            return {"message": "Filter reordered successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to reorder filters")
    except Exception as e:
        logger.error(f"Error reordering filters: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/column-sets", response_model=Dict)
def create_or_update_column_set(column_set: ColumnSet, current_user: str = Depends(get_current_user)):
    """
    컬럼 세트 생성 또는 업데이트
    """
    try:
        if column_set.id:
            is_success = screener_service.update_column_set(column_set.id, column_set.name, column_set.columns)
            message = "Column updated successfully"
        else:
            is_success = screener_service.create_column_set(current_user.id, column_set.name, column_set.columns)
            message = "Column created successfully"
        if is_success:
            return {"message": message}
    except Exception as e:
        logger.error(f"Error creating column: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/column-sets", response_model=List[ColumnSet])
def get_column_sets(current_user: str = Depends(get_current_user)):
    """
    컬럼 세트 조회
    """
    try:
        columns = screener_service.get_column_sets(current_user.id)
        return [ColumnSet(id=column["id"], name=column["name"], columns=column["columns"]) for column in columns]
    except Exception as e:
        logger.error(f"Error getting columns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/column-sets/{column_set_id}", response_model=Dict)
def update_column_set(column_set_update: ColumnSetUpdate):
    """
    컬럼 세트 수정
    """
    try:
        is_success = screener_service.update_column_set(
            column_set_update.column_set_id, column_set_update.name, column_set_update.columns
        )
        if is_success:
            return {"message": "Column updated successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to update column")
    except Exception as e:
        logger.error(f"Error updating column: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/column-sets/{column_set_id}", response_model=Dict)
def delete_column_set(column_set_id: int):
    """
    컬럼 세트 삭제
    """
    try:
        is_success = screener_service.delete_column_set(column_set_id)
        if is_success:
            return {"message": "Column deleted successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to delete column")
    except Exception as e:
        logger.error(f"Error deleting column: {e}")


@router.get("/columns", response_model=ColumnsResponse)
def get_columns(category: Optional[CategoryEnum] = None, id: Optional[int] = None):
    """
    컬럼 목록 조회
    """
    try:
        columns = screener_service.get_columns(category, id)
        return ColumnsResponse(columns=columns)
    except Exception as e:
        logger.error(f"Error getting columns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/parquet")
def update_parquet():
    """
    파퀴 업데이트
    """
    try:
        factor_utils.process_us_factor_data()
        factor_utils.process_kr_factor_data()
        return {"message": "Parquet updated successfully"}
    except Exception as e:
        logger.error(f"Error updating parquet: {e}")
        raise HTTPException(status_code=500, detail=str(e))
