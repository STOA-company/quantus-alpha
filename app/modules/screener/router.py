from fastapi import APIRouter, HTTPException, Depends, Response
from typing import List, Dict, Optional
import io
from app.modules.screener.service import get_screener_service, ScreenerService
from app.modules.screener.schemas import (
    FactorResponse,
    GroupMetaData,
    FilteredStocks,
    GroupFilter,
)
import logging
from app.utils.oauth_utils import get_current_user
from app.utils.factor_utils import factor_utils
from app.models.models_factors import CategoryEnum
from app.common.constants import REVERSE_FACTOR_MAP, UNIT_MAP, DEFAULT_COLUMNS
from app.modules.screener.schemas import MarketEnum
from app.core.exception.custom import CustomException

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/factors/{market}", response_model=List[FactorResponse])
def get_factors(market: MarketEnum, screener_service: ScreenerService = Depends(get_screener_service)):
    """
    모든 팩터 조회
    """
    try:
        factors = screener_service.get_factors()
        if market in [MarketEnum.US, MarketEnum.SNP500, MarketEnum.NASDAQ]:
            nation = "us"
        else:
            nation = "kr"

        result = []
        for factor in factors:
            if factor["unit"] == "small_price":
                unit = "원" if nation == "kr" else "$"
            elif factor["unit"] == "big_price":
                unit = "억원" if nation == "kr" else "K$"
            else:
                unit = UNIT_MAP[factor["unit"]]
            result.append(
                FactorResponse(
                    factor=factor["factor"],
                    description=factor["description"],
                    unit=unit,
                    category=factor["category"],
                    direction=factor["direction"],
                    min_value=factor["min_value"],
                    max_value=factor["max_value"],
                )
            )
        return result
    except Exception as e:
        logger.error(f"Error getting factors: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stocks", response_model=Dict)
def get_filtered_stocks(
    filtered_stocks: FilteredStocks, screener_service: ScreenerService = Depends(get_screener_service)
):
    """
    필터링된 종목들 조회

    market_filter : ["us", "kr", "S&P 500", "NASDAQ", "KOSPI", "KOSDAQ"] 중 하나
    """
    try:
        custom_filters = []
        if filtered_stocks.custom_filters:
            custom_filters = [
                {
                    "factor": REVERSE_FACTOR_MAP[condition.factor],
                    "above": condition.above,
                    "below": condition.below,
                }
                for condition in filtered_stocks.custom_filters
            ]

        request_columns = DEFAULT_COLUMNS
        for column in [REVERSE_FACTOR_MAP[column] for column in filtered_stocks.columns]:
            if column not in request_columns:
                request_columns.append(column)

        sort_by = REVERSE_FACTOR_MAP[filtered_stocks.sort_by]

        stocks_data, total_count = screener_service.get_filtered_stocks(
            filtered_stocks.market_filter,
            filtered_stocks.sector_filter,
            custom_filters,
            request_columns,
            filtered_stocks.limit,
            filtered_stocks.offset,
            sort_by,
            filtered_stocks.ascending,
        )

        has_next = filtered_stocks.offset * filtered_stocks.limit + filtered_stocks.limit < total_count

        result = {"data": stocks_data, "has_next": has_next}
        return result

    except CustomException as e:
        logger.error(f"Error getting filtered stocks: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)

    except Exception as e:
        logger.error(f"Error getting filtered stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stocks/count", response_model=Dict)
def get_filtered_stocks_count(
    filtered_stocks: FilteredStocks, screener_service: ScreenerService = Depends(get_screener_service)
):
    """
    필터링된 종목들 조회

    market_filter : ["us", "kr", "S&P 500", "NASDAQ", "KOSPI", "KOSDAQ"] 중 하나
    """
    try:
        custom_filters = []
        if filtered_stocks.custom_filters:
            custom_filters = [
                {
                    "factor": REVERSE_FACTOR_MAP[condition.factor],
                    "above": condition.above,
                    "below": condition.below,
                }
                for condition in filtered_stocks.custom_filters
            ]

        total_count = screener_service.get_filtered_stocks_count(
            filtered_stocks.market_filter,
            filtered_stocks.sector_filter,
            custom_filters,
            [REVERSE_FACTOR_MAP[column] for column in filtered_stocks.columns],
        )

        result = {"count": total_count}
        return result

    except Exception as e:
        logger.error(f"Error getting filtered stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stocks/description", response_model=Dict)
def get_filtered_stocks_with_description(
    filtered_stocks: FilteredStocks, screener_service: ScreenerService = Depends(get_screener_service)
):
    """
    필터링된 종목들 조회

    market_filter : ["us", "kr", "S&P 500", "NASDAQ", "KOSPI", "KOSDAQ"] 중 하나
    """
    try:
        custom_filters = []
        if filtered_stocks.custom_filters:
            custom_filters = [
                {
                    "factor": REVERSE_FACTOR_MAP[condition.factor],
                    "above": condition.above,
                    "below": condition.below,
                }
                for condition in filtered_stocks.custom_filters
            ]

        stocks_data, has_next = screener_service.get_filtered_stocks_with_description(
            filtered_stocks.market_filter,
            filtered_stocks.sector_filter,
            custom_filters,
            [REVERSE_FACTOR_MAP[column] for column in filtered_stocks.columns],
            filtered_stocks.limit,
            filtered_stocks.offset,
        )

        result = {"has_next": has_next, "data": stocks_data}
        return result

    except Exception as e:
        logger.error(f"Error getting filtered stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stocks/download")
def download_filtered_stocks(
    filtered_stocks: FilteredStocks, screener_service: ScreenerService = Depends(get_screener_service)
):
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


@router.get("/groups", response_model=List[GroupMetaData])
def get_groups(
    current_user: str = Depends(get_current_user), screener_service: ScreenerService = Depends(get_screener_service)
):
    """
    저장된 필터 목록 조회
    """
    try:
        groups = screener_service.get_groups(current_user.id)
        return [GroupMetaData(id=group["id"], name=group["name"], type=group["type"]) for group in groups]
    except Exception as e:
        logger.error(f"Error getting groups: {e}")
        return []


@router.post("/groups", response_model=Dict)
def create_or_update_group(
    group_filter: GroupFilter,
    current_user: str = Depends(get_current_user),
    screener_service: ScreenerService = Depends(get_screener_service),
):
    """
    필터 생성 또는 업데이트
    """
    try:
        if group_filter.id:
            is_success = screener_service.update_group(
                group_id=group_filter.id,
                name=group_filter.name,
                market_filter=group_filter.market_filter,
                sector_filter=group_filter.sector_filter,
                custom_filters=group_filter.custom_filters,
                factor_filters=group_filter.factor_filters,
            )
            message = "Filter updated successfully"
        else:
            is_success = screener_service.create_group(
                user_id=current_user.id,
                name=group_filter.name,
                market_filter=group_filter.market_filter,
                sector_filter=group_filter.sector_filter,
                custom_filters=group_filter.custom_filters,
                factor_filters=group_filter.factor_filters,
                type=group_filter.type,
            )
            message = "Group created successfully"
        if is_success:
            return {"message": message}
    except CustomException as e:
        logger.error(f"Error creating group: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error creating group: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/groups/{group_id}", response_model=Dict)
def delete_group(group_id: int, screener_service: ScreenerService = Depends(get_screener_service)):
    """
    필터 삭제
    """
    try:
        is_success = screener_service.delete_group(group_id)
        if is_success:
            return {"message": "Group deleted successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to delete filter")
    except Exception as e:
        logger.error(f"Error deleting group: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups/reorder", response_model=Dict)
def reorder_groups(groups: List[int], screener_service: ScreenerService = Depends(get_screener_service)):
    """
    필터 순서 업데이트
    """
    try:
        is_success = screener_service.reorder_groups(groups)
        if is_success:
            return {"message": "Group reordered successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to reorder groups")
    except Exception as e:
        logger.error(f"Error reordering groups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/groups/{group_id}", response_model=GroupFilter)
def get_group_filters(group_id: int, screener_service: ScreenerService = Depends(get_screener_service)):
    """
    필터 목록 조회
    """
    try:
        group_filters = screener_service.get_group_filters(group_id)
        stock_filters = group_filters["stock_filters"]

        market_filter = None
        sector_filter = []
        custom_filters = []

        for stock_filter in stock_filters:
            if stock_filter["factor"] == "시장":
                market_filter = stock_filter["value"]
            elif stock_filter["factor"] == "산업":
                sector_filter.append(stock_filter["value"])
            else:
                custom_filters.append(stock_filter)

        factor_filters = group_filters["factor_filters"]
        return GroupFilter(
            id=group_id,
            market_filter=market_filter,
            sector_filter=sector_filter,
            custom_filters=custom_filters,
            factor_filters=factor_filters,
        )
    except Exception as e:
        logger.error(f"Error getting group filters: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/columns", response_model=dict)
def get_columns(
    category: CategoryEnum,
    group_id: Optional[int] = None,
    screener_service: ScreenerService = Depends(get_screener_service),
):
    """
    컬럼 목록 조회
    """
    try:
        columns = []
        if category == CategoryEnum.CUSTOM:
            columns = screener_service.get_columns(group_id)
        elif category == CategoryEnum.TECHNICAL:
            columns = ["베타 (52주)", "RSI (14일)", "샤프 비율 (52주)", "모멘텀 (6개월)", "변동성 (52주)"]
        elif category == CategoryEnum.FUNDAMENTAL:
            columns = ["ROE", "F-score", "부채 비율", "영업 이익", "Altman Z-score"]
        elif category == CategoryEnum.VALUATION:
            columns = ["PBR", "PCR", "PER", "POR", "PSR"]

        result = ["티커", "종목명", "국가", "시장", "산업", "스코어"] + columns
        return {"columns": result}
    except Exception as e:
        logger.error(f"Error getting columns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/parquet")
def update_parquet(screener_service: ScreenerService = Depends(get_screener_service)):
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
