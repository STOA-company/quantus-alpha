from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict
from app.modules.screener.stock.service import ScreenerStockService
from app.modules.screener.stock.schemas import (
    FactorResponse,
    GroupMetaData,
    FilteredStocks,
    GroupFilter,
    GroupFilterResponse,
    FactorCodeValue,
)
import logging
from app.utils.oauth_utils import get_current_user
from app.modules.screener.utils import screener_utils
from app.cache.factors import factors_cache
from app.models.models_factors import CategoryEnum
from app.common.constants import (
    FACTOR_MAP,
    FACTOR_MAP_EN,
)
from app.modules.screener.stock.schemas import MarketEnum, StockType
from app.core.exception.custom import CustomException

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/factors/{market}", response_model=List[FactorResponse])
def get_factors(market: MarketEnum, screener_service: ScreenerStockService = Depends(ScreenerStockService)):
    """
    모든 팩터 조회
    """
    try:
        factors = screener_service.get_factors(market)
        result = [FactorResponse(**factor) for factor in factors]
        return result
    except Exception as e:
        logger.error(f"Error getting factors: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stocks", response_model=Dict)
def get_filtered_stocks(
    filtered_stocks: FilteredStocks, screener_service: ScreenerStockService = Depends(ScreenerStockService)
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
                    "factor": condition.factor,
                    "above": condition.above,
                    "below": condition.below,
                }
                for condition in filtered_stocks.custom_filters
            ]

        request_columns = ["Code", "Name", "country"]

        for column in [column for column in filtered_stocks.factor_filters]:
            if column not in request_columns:
                request_columns.append(column)

        sort_by = "score"
        ascending = False
        if filtered_stocks.sort_info:
            sort_by = filtered_stocks.sort_info.sort_by
            ascending = filtered_stocks.sort_info.ascending

        stocks_data, total_count = screener_service.get_filtered_data(
            market_filter=filtered_stocks.market_filter,
            sector_filter=filtered_stocks.sector_filter,
            custom_filters=custom_filters,
            columns=request_columns,
            limit=filtered_stocks.limit,
            offset=filtered_stocks.offset,
            sort_by=sort_by,
            ascending=ascending,
            lang=filtered_stocks.lang,
        )

        has_next = filtered_stocks.offset * filtered_stocks.limit + filtered_stocks.limit < total_count

        factor_map = FACTOR_MAP if filtered_stocks.lang == "kr" else FACTOR_MAP_EN
        for stock in stocks_data:
            keys = list(stock.keys())
            for key in keys:
                if key in factor_map:
                    stock[factor_map[key]] = stock[key]

            for key in keys:
                if key in factor_map:
                    del stock[key]

        result = {"data": stocks_data, "has_next": has_next}
        return result

    except CustomException as e:
        logger.exception(f"Error getting filtered stocks: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)

    except Exception as e:
        logger.exception(f"Error getting filtered stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stocks/count", response_model=Dict)
def get_filtered_stocks_count(
    filtered_stocks: FilteredStocks, screener_service: ScreenerStockService = Depends(ScreenerStockService)
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
                    "factor": condition.factor,
                    "above": condition.above,
                    "below": condition.below,
                }
                for condition in filtered_stocks.custom_filters
            ]

        total_count = screener_service.get_filtered_data_count(
            market_filter=filtered_stocks.market_filter,
            sector_filter=filtered_stocks.sector_filter,
            custom_filters=custom_filters,
            columns=filtered_stocks.factor_filters,
        )

        result = {"count": total_count}
        return result

    except Exception as e:
        logger.exception(f"Error getting filtered stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/groups", response_model=List[GroupMetaData])
def get_groups(
    current_user: str = Depends(get_current_user),
    screener_service: ScreenerStockService = Depends(ScreenerStockService),
):
    """
    저장된 필터 목록 조회
    """
    try:
        if current_user is None:
            return []
        groups = screener_service.get_groups(current_user.id, type=StockType.STOCK)
        return [GroupMetaData(id=group["id"], name=group["name"], type=group["type"]) for group in groups]
    except Exception as e:
        logger.exception(f"Error getting groups: {e}")
        return []


@router.post("/groups", response_model=Dict)
async def create_or_update_group(
    group_filter: GroupFilter,
    current_user: str = Depends(get_current_user),
    screener_service: ScreenerStockService = Depends(ScreenerStockService),
):
    """
    필터 생성 또는 업데이트
    """
    try:
        if group_filter.id:
            is_success = await screener_service.update_group(
                group_id=group_filter.id,
                name=group_filter.name,
                market_filter=group_filter.market_filter,
                sector_filter=group_filter.sector_filter,
                custom_filters=group_filter.custom_filters,
                factor_filters=group_filter.factor_filters,
                category=group_filter.category,
                sort_info=group_filter.sort_info,
            )
            message = "Filter updated successfully"
        else:
            is_success = await screener_service.create_group(
                user_id=current_user.id,
                name=group_filter.name,
                market_filter=group_filter.market_filter,
                sector_filter=group_filter.sector_filter,
                custom_filters=group_filter.custom_filters,
                type=group_filter.type,
            )
            message = "Group created successfully"
        if is_success:
            return {"message": message}
    except CustomException as e:
        logger.exception(f"Error creating group: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.exception(f"Error creating group: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/groups/{group_id}", response_model=GroupFilterResponse)
def get_group_filters(
    group_id: int = -1, lang: str = "kr", screener_service: ScreenerStockService = Depends(ScreenerStockService)
):
    """
    필터 목록 조회
    """
    try:
        technical_columns = screener_service.get_columns(group_id, CategoryEnum.TECHNICAL)
        fundamental_columns = screener_service.get_columns(group_id, CategoryEnum.FUNDAMENTAL)
        valuation_columns = screener_service.get_columns(group_id, CategoryEnum.VALUATION)

        technical_sort_info = screener_service.get_sort_info(group_id, CategoryEnum.TECHNICAL)
        fundamental_sort_info = screener_service.get_sort_info(group_id, CategoryEnum.FUNDAMENTAL)
        valuation_sort_info = screener_service.get_sort_info(group_id, CategoryEnum.VALUATION)
        custom_sort_info = screener_service.get_sort_info(group_id, CategoryEnum.CUSTOM)

        group_name = "기본"
        market_filter = MarketEnum.US
        has_custom = False
        custom_factor_filters = []
        custom_filters = []

        if group_id == -1:
            sector_filter = screener_service.get_available_sectors()
        else:
            group_filters = screener_service.get_group_filters(group_id)
            stock_filters = group_filters["stock_filters"]

            group_name = group_filters["name"]
            has_custom = group_filters["has_custom"]
            custom_factor_filters = group_filters["custom_factor_filters"]

            sector_filter = []
            for stock_filter in stock_filters:
                if stock_filter["factor"] == "market":
                    market_filter = stock_filter["value"]
                elif stock_filter["factor"] == "sector":
                    sector_filter.append(stock_filter["value"])
                else:
                    custom_filters.append(stock_filter)

        technical = []
        fundamental = []
        valuation = []
        custom = []

        factor_map = FACTOR_MAP if lang == "kr" else FACTOR_MAP_EN

        for column in technical_columns:
            technical.append(FactorCodeValue(code=column, value=factor_map[column]))

        for column in fundamental_columns:
            fundamental.append(FactorCodeValue(code=column, value=factor_map[column]))

        for column in valuation_columns:
            valuation.append(FactorCodeValue(code=column, value=factor_map[column]))

        for column in custom_factor_filters:
            custom.append(FactorCodeValue(code=column, value=factor_map[column]))

        technical_sort_info.sort_by = FactorCodeValue(
            code=technical_sort_info.sort_by, value=factor_map[technical_sort_info.sort_by]
        )
        fundamental_sort_info.sort_by = FactorCodeValue(
            code=fundamental_sort_info.sort_by, value=factor_map[fundamental_sort_info.sort_by]
        )
        valuation_sort_info.sort_by = FactorCodeValue(
            code=valuation_sort_info.sort_by, value=factor_map[valuation_sort_info.sort_by]
        )
        custom_sort_info.sort_by = FactorCodeValue(
            code=custom_sort_info.sort_by, value=factor_map[custom_sort_info.sort_by]
        )

        return GroupFilterResponse(
            id=group_id,
            name=group_name,
            market_filter=market_filter,
            has_custom=has_custom,
            sector_filter=sector_filter,
            custom_filters=custom_filters,
            factor_filters={
                "technical": technical,
                "fundamental": fundamental,
                "valuation": valuation,
                "custom": custom,
            },
            sort_info={
                CategoryEnum.TECHNICAL: technical_sort_info,
                CategoryEnum.FUNDAMENTAL: fundamental_sort_info,
                CategoryEnum.VALUATION: valuation_sort_info,
                CategoryEnum.CUSTOM: custom_sort_info,
            },
        )
    except Exception as e:
        logger.exception(f"Error getting group filters: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/groups/{group_id}", response_model=Dict)
def delete_group(group_id: int, screener_service: ScreenerStockService = Depends(ScreenerStockService)):
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
        logger.exception(f"Error deleting group: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups/reorder", response_model=Dict)
def reorder_groups(groups: List[int], screener_service: ScreenerStockService = Depends(ScreenerStockService)):
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
        logger.exception(f"Error reordering groups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups/name")
def update_group_name(group_id: int, name: str, screener_service: ScreenerStockService = Depends(ScreenerStockService)):
    """
    그룹 이름 수정
    """
    try:
        updated_group_name = screener_service.update_group_name(group_id, name)
        return {"message": f"Group name updated to {updated_group_name}"}
    except CustomException as e:
        logger.exception(f"Error updating group name: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.exception(f"Error updating group name: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/parquet/{country}")
def update_parquet(country: str):
    try:
        if country == "kr":
            screener_utils.process_kr_factor_data()
        elif country == "us":
            screener_utils.process_us_factor_data()
        else:
            raise HTTPException(status_code=400, detail="Invalid country")
        screener_utils.process_global_factor_data()
        screener_utils.archive_parquet(country)
        factors_cache.force_update()
        return {"message": "Parquet updated successfully"}
    except Exception as e:
        logger.exception(f"Error updating parquet: {e}")
        raise HTTPException(status_code=500, detail=str(e))
