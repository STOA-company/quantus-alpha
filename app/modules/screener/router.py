from fastapi import APIRouter, HTTPException, Depends, Response
from typing import List, Dict
import io
from app.modules.screener.service import screener_service
from app.modules.screener.schemas import (
    FactorResponse,
    Filter,
    FilterUpdate,
    FilteredStocks,
    ColumnSetCreate,
    ColumnUpdate,
    ColumnSet,
)
import logging
from app.utils.oauth_utils import get_current_user
from app.utils.factor_utils import process_kr_factor_data, process_us_factor_data

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


@router.patch("/filters/{filter_id}", response_model=Dict)
def update_filter(filter_update: FilterUpdate):
    """
    필터 수정
    """
    try:
        is_success = screener_service.update_filter(
            filter_update.filter_id, filter_update.filter.name, filter_update.filter.conditions
        )
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


@router.post("/columns", response_model=Dict)
def create_column(column_set: ColumnSetCreate, current_user: str = Depends(get_current_user)):
    """
    컬럼 생성
    """
    try:
        is_success = screener_service.create_column_set(column_set.columns, current_user.id, column_set.name)
        if is_success:
            return {"message": "Column created successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to create column")
    except Exception as e:
        logger.error(f"Error creating column: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/columns", response_model=List[ColumnSet])
def get_columns(current_user: str = Depends(get_current_user)):
    """
    컬럼 세트 조회
    """
    try:
        columns = screener_service.get_column_sets(current_user.id)
        return [ColumnSet(id=column["id"], name=column["name"], columns=column["columns"]) for column in columns]
    except Exception as e:
        logger.error(f"Error getting columns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/columns/{column_set_id}", response_model=Dict)
def update_column(column_update: ColumnUpdate):
    """
    컬럼 수정
    """
    try:
        is_success = screener_service.update_column_set(
            column_update.column_set_id, column_update.name, column_update.columns
        )
        if is_success:
            return {"message": "Column updated successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to update column")
    except Exception as e:
        logger.error(f"Error updating column: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/columns/{column_set_id}", response_model=Dict)
def delete_column(column_set_id: int):
    """
    컬럼 삭제
    """
    try:
        is_success = screener_service.delete_column_set(column_set_id)
        if is_success:
            return {"message": "Column deleted successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to delete column")
    except Exception as e:
        logger.error(f"Error deleting column: {e}")


@router.get("/parquet")
def update_parquet():
    """
    파퀴 업데이트
    """
    try:
        process_us_factor_data()
        process_kr_factor_data()
        return {"message": "Parquet updated successfully"}
    except Exception as e:
        logger.error(f"Error updating parquet: {e}")
        raise HTTPException(status_code=500, detail=str(e))
