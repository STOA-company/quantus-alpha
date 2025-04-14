from datetime import datetime
from io import StringIO
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse, Response

from app.cache.factors import factors_cache
from app.common.constants import (
    FACTOR_KOREAN_TO_ENGLISH_MAP,
    FACTOR_MAP,
    MARKET_KOREAN_TO_ENGLISH_MAP,
    REVERSE_FACTOR_MAP,
    REVERSE_FACTOR_MAP_EN,
)
from app.core.exception.custom import CustomException
from app.core.logger import setup_logger
from app.models.models_factors import CategoryEnum
from app.models.models_users import AlphafinderUser
from app.modules.screener.stock.schemas import (
    FactorResponse,
    FilteredStocks,
    GroupFilter,
    GroupFilterResponse,
    GroupMetaData,
    MarketEnum,
    PaginatedFilteredStocks,
    StockType,
)
from app.modules.screener.stock.service import ScreenerStockService
from app.modules.screener.utils import screener_utils
from app.modules.user.schemas import DataDownloadHistory
from app.modules.user.service import UserService, get_user_service
from app.utils.oauth_utils import get_current_user

logger = setup_logger(__name__)

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
                    "factor": REVERSE_FACTOR_MAP[condition.factor],
                    "above": condition.above,
                    "below": condition.below,
                    "values": condition.values,
                }
                for condition in filtered_stocks.custom_filters
            ]

        request_columns = ["Code", "Name", "country"]
        reverse_factor_map = REVERSE_FACTOR_MAP
        if filtered_stocks.lang == "en":
            reverse_factor_map = REVERSE_FACTOR_MAP_EN

        for column in [reverse_factor_map[column] for column in filtered_stocks.factor_filters]:
            if column not in request_columns:
                request_columns.append(column)

        sort_by = "score"
        ascending = False
        if filtered_stocks.sort_info:
            sort_by = reverse_factor_map[filtered_stocks.sort_info.sort_by]
            ascending = filtered_stocks.sort_info.ascending

        stocks_data, total_count = screener_service.get_filtered_data(
            market_filter=filtered_stocks.market_filter,
            sector_filter=filtered_stocks.sector_filter,
            exclude_filters=filtered_stocks.exclude_filters,
            custom_filters=custom_filters,
            columns=request_columns,
            limit=filtered_stocks.limit,
            offset=filtered_stocks.offset,
            sort_by=sort_by,
            ascending=ascending,
            lang=filtered_stocks.lang,
        )

        has_next = filtered_stocks.offset * filtered_stocks.limit + filtered_stocks.limit < total_count

        if filtered_stocks.lang == "en":
            for stock in stocks_data:
                stock["Market"] = MARKET_KOREAN_TO_ENGLISH_MAP[stock["Market"]]

        result = {"data": stocks_data, "has_next": has_next}
        return result

    except CustomException as e:
        logger.exception(f"Error getting filtered stocks: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)

    except Exception as e:
        logger.exception(f"Error getting filtered stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stocks/paginated", response_model=Dict)
def get_paginated_stocks(
    filtered_stocks: PaginatedFilteredStocks, screener_service: ScreenerStockService = Depends(ScreenerStockService)
):
    """
    page: 페이지 번호 (1부터 시작)
    """
    try:
        custom_filters = []
        if filtered_stocks.custom_filters:
            custom_filters = [
                {
                    "factor": REVERSE_FACTOR_MAP[condition.factor],
                    "above": condition.above,
                    "below": condition.below,
                    "values": condition.values,
                }
                for condition in filtered_stocks.custom_filters
            ]

        request_columns = ["Code", "Name", "country"]
        reverse_factor_map = REVERSE_FACTOR_MAP
        if filtered_stocks.lang == "en":
            reverse_factor_map = REVERSE_FACTOR_MAP_EN

        for column in [reverse_factor_map[column] for column in filtered_stocks.factor_filters]:
            if column not in request_columns:
                request_columns.append(column)

        sort_by = "score"
        ascending = False
        if filtered_stocks.sort_info:
            sort_by = reverse_factor_map[filtered_stocks.sort_info.sort_by]
            ascending = filtered_stocks.sort_info.ascending

        page = filtered_stocks.page if filtered_stocks.page > 0 else 1
        offset = page - 1

        stocks_data, total_count = screener_service.get_filtered_data(
            market_filter=filtered_stocks.market_filter,
            sector_filter=filtered_stocks.sector_filter,
            exclude_filters=filtered_stocks.exclude_filters,
            custom_filters=custom_filters,
            columns=request_columns,
            limit=filtered_stocks.limit,
            offset=offset,
            sort_by=sort_by,
            ascending=ascending,
            lang=filtered_stocks.lang,
        )

        total_pages = (total_count + filtered_stocks.limit - 1) // filtered_stocks.limit if total_count > 0 else 0

        if filtered_stocks.lang == "en":
            for stock in stocks_data:
                stock["Market"] = MARKET_KOREAN_TO_ENGLISH_MAP[stock["Market"]]

        result = {
            "data": stocks_data,
            "total_count": total_count,
            "total_pages": total_pages,
            "current_page": page,
        }
        return result

    except CustomException as e:
        logger.exception(f"Error getting paginated stocks: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)

    except Exception as e:
        logger.exception(f"Error getting paginated stocks: {e}")
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
                    "factor": REVERSE_FACTOR_MAP[condition.factor],
                    "above": condition.above,
                    "below": condition.below,
                    "values": condition.values,
                }
                for condition in filtered_stocks.custom_filters
            ]

        total_count = screener_service.get_filtered_data_count(
            market_filter=filtered_stocks.market_filter,
            sector_filter=filtered_stocks.sector_filter,
            exclude_filters=filtered_stocks.exclude_filters,
            custom_filters=custom_filters,
            columns=[REVERSE_FACTOR_MAP[column] for column in filtered_stocks.factor_filters],
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
        group_id = None
        if group_filter.id:
            group_id = await screener_service.update_group(
                group_id=group_filter.id,
                name=group_filter.name,
                market_filter=group_filter.market_filter,
                exclude_filters=group_filter.exclude_filters,
                sector_filter=group_filter.sector_filter,
                custom_filters=group_filter.custom_filters,
                factor_filters=group_filter.factor_filters,
                category=group_filter.category,
                sort_info=group_filter.sort_info,
            )
        else:
            group_id = await screener_service.create_group(
                user_id=current_user.id,
                name=group_filter.name,
                market_filter=group_filter.market_filter,
                exclude_filters=group_filter.exclude_filters,
                sector_filter=group_filter.sector_filter,
                factor_filters=group_filter.factor_filters,
                custom_filters=group_filter.custom_filters,
                sort_info=group_filter.sort_info,
                type=group_filter.type,
            )

        return {"group_id": group_id}

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
        dividend_columns = screener_service.get_columns(group_id, CategoryEnum.DIVIDEND)
        growth_columns = screener_service.get_columns(group_id, CategoryEnum.GROWTH)

        if lang == "en":
            technical_columns = [FACTOR_KOREAN_TO_ENGLISH_MAP[factor] for factor in technical_columns]
            fundamental_columns = [FACTOR_KOREAN_TO_ENGLISH_MAP[factor] for factor in fundamental_columns]
            valuation_columns = [FACTOR_KOREAN_TO_ENGLISH_MAP[factor] for factor in valuation_columns]
            dividend_columns = [FACTOR_KOREAN_TO_ENGLISH_MAP[factor] for factor in dividend_columns]
            growth_columns = [FACTOR_KOREAN_TO_ENGLISH_MAP[factor] for factor in growth_columns]

        technical_sort_info = screener_service.get_sort_info(group_id, CategoryEnum.TECHNICAL)
        fundamental_sort_info = screener_service.get_sort_info(group_id, CategoryEnum.FUNDAMENTAL)
        valuation_sort_info = screener_service.get_sort_info(group_id, CategoryEnum.VALUATION)
        dividend_sort_info = screener_service.get_sort_info(group_id, CategoryEnum.DIVIDEND)
        growth_sort_info = screener_service.get_sort_info(group_id, CategoryEnum.GROWTH)
        custom_sort_info = screener_service.get_sort_info(group_id, CategoryEnum.CUSTOM)

        if group_id == -1:
            all_sectors = screener_service.get_available_sectors()
            custom_filters = screener_service.get_default_custom_filters()
            for filter in custom_filters:
                filter["factor"] = FACTOR_MAP.get(filter["factor"], filter["factor"])
            return GroupFilterResponse(
                id=-1,
                name="기본",
                market_filter=MarketEnum.ALL,
                has_custom=False,
                exclude_filters=[],
                sector_filter=all_sectors,
                custom_filters=custom_filters,
                factor_filters={
                    "technical": technical_columns,
                    "fundamental": fundamental_columns,
                    "valuation": valuation_columns,
                    "dividend": dividend_columns,
                    "growth": growth_columns,
                    "custom": [],
                },
                sort_info={
                    CategoryEnum.TECHNICAL: technical_sort_info,
                    CategoryEnum.FUNDAMENTAL: fundamental_sort_info,
                    CategoryEnum.VALUATION: valuation_sort_info,
                    CategoryEnum.DIVIDEND: dividend_sort_info,
                    CategoryEnum.GROWTH: growth_sort_info,
                    CategoryEnum.CUSTOM: custom_sort_info,
                },
            )

        group_filters = screener_service.get_group_filters(group_id)
        stock_filters = group_filters["stock_filters"]

        market_filter = None
        sector_filter = []
        exclude_filters = []
        custom_filters = []

        for stock_filter in stock_filters:
            if stock_filter["factor"] == "시장":
                market_filter = stock_filter["values"][0]
            elif stock_filter["factor"] == "산업":
                sector_filter.extend(stock_filter["values"])
            elif stock_filter["factor"] == "제외":
                exclude_filters.extend(stock_filter["values"])
            else:
                custom_filters.append(stock_filter)

        custom_factor_filters = group_filters["custom_factor_filters"]

        if lang == "en":
            # sector_filter = [MARKET_KOREAN_TO_ENGLISH_MAP[sector] for sector in sector_filter]
            custom_factor_filters = [FACTOR_KOREAN_TO_ENGLISH_MAP[factor] for factor in custom_factor_filters]

        return GroupFilterResponse(
            id=group_id,
            name=group_filters["name"],
            market_filter=market_filter,
            sector_filter=sector_filter,
            custom_filters=custom_filters,
            exclude_filters=exclude_filters if exclude_filters else [],
            factor_filters={
                "technical": technical_columns,
                "fundamental": fundamental_columns,
                "valuation": valuation_columns,
                "dividend": dividend_columns,
                "growth": growth_columns,
                "custom": custom_factor_filters,
            },
            sort_info={
                CategoryEnum.TECHNICAL: technical_sort_info,
                CategoryEnum.FUNDAMENTAL: fundamental_sort_info,
                CategoryEnum.VALUATION: valuation_sort_info,
                CategoryEnum.DIVIDEND: dividend_sort_info,
                CategoryEnum.GROWTH: growth_sort_info,
                CategoryEnum.CUSTOM: custom_sort_info,
            },
            has_custom=group_filters["has_custom"],
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


@router.post("/download")
def download_filtered_stocks(
    filtered_stocks: FilteredStocks,
    screener_service: ScreenerStockService = Depends(ScreenerStockService),
    user: AlphafinderUser = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
):
    if user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    if user.subscription_level == 1:
        raise HTTPException(status_code=403, detail="구독 레벨이 낮습니다.")
    custom_filters = []
    if filtered_stocks.custom_filters:
        custom_filters = [
            {
                "factor": REVERSE_FACTOR_MAP[condition.factor],
                "above": condition.above,
                "below": condition.below,
                "values": condition.values,
            }
            for condition in filtered_stocks.custom_filters
        ]

    request_columns = ["Code", "Name", "country"]
    reverse_factor_map = REVERSE_FACTOR_MAP
    if filtered_stocks.lang == "en":
        reverse_factor_map = REVERSE_FACTOR_MAP_EN

    for column in [reverse_factor_map[column] for column in filtered_stocks.factor_filters]:
        if column not in request_columns:
            request_columns.append(column)

    sort_by = "score"
    ascending = False
    if filtered_stocks.sort_info:
        sort_by = reverse_factor_map[filtered_stocks.sort_info.sort_by]
        ascending = filtered_stocks.sort_info.ascending

    df = screener_service.get_filtered_stocks_download(
        market_filter=filtered_stocks.market_filter,
        sector_filter=filtered_stocks.sector_filter,
        custom_filters=custom_filters,
        columns=request_columns,
        sort_by=sort_by,
        ascending=ascending,
        lang=filtered_stocks.lang,
    )

    if df is None or df.empty:
        return JSONResponse(content={"error": "데이터가 없습니다"}, status_code=404)

    csv_data = StringIO()
    df.to_csv(csv_data, index=False, encoding="utf-8-sig")

    data_download_history = DataDownloadHistory(
        user_id=user.id,
        data_type="screener",
        data_detail="stock",
        download_datetime=datetime.now(),
    )
    user_service.save_data_download_history(data_download_history)

    market_str = (
        filtered_stocks.market_filter.value
        if hasattr(filtered_stocks.market_filter, "value")
        else str(filtered_stocks.market_filter)
    )
    filename = f"stock_export_{market_str}.csv"

    return Response(
        content=csv_data.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.post("/init")
async def init_screener(screener_service: ScreenerStockService = Depends(ScreenerStockService)):
    await screener_service.initialize()
    return {"message": "Screener initialized successfully"}


@router.get("/multi")
def get_multi_select_factors(screener_service: ScreenerStockService = Depends(ScreenerStockService)):
    return screener_service.get_multi_select_factors()
