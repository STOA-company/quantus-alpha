import io
from typing import Dict, List, Literal
from fastapi import APIRouter, Depends, HTTPException, Response

from app.batches.run_etf_screener import run_etf_screener_data
from app.enum.type import StockType
from app.models.models_factors import CategoryEnum
from app.modules.screener.stock.schemas import FactorResponse, GroupFilter, GroupFilterResponse, GroupMetaData
from app.modules.screener.etf.enum import ETFMarketEnum
from app.modules.screener.etf.schemas import FilteredETF
from app.modules.screener.etf.service import ScreenerETFService
from app.utils.oauth_utils import get_current_user
from app.core.logging.config import get_logger
from app.common.constants import FACTOR_KOREAN_TO_ENGLISH_MAP, REVERSE_FACTOR_MAP, REVERSE_FACTOR_MAP_EN, ETF_MARKET_MAP
from app.core.exception.base import CustomException

router = APIRouter()
logger = get_logger(__name__)


@router.get("/factors/{market}", response_model=List[FactorResponse])
def get_factors(market: ETFMarketEnum, screener_etf_service: ScreenerETFService = Depends(ScreenerETFService)):
    """
    모든 팩터 조회
    """
    try:
        factors = screener_etf_service.get_factors(market)
        result = [FactorResponse(**factor) for factor in factors]
        result = [factor_response for factor_response in result if factor_response.factor != "총 수수료"]
        return result
    except Exception as e:
        logger.error(f"Error getting factors: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("", response_model=Dict)
def get_filtered_etfs(filtered_etf: FilteredETF, screener_etf_service: ScreenerETFService = Depends(ScreenerETFService)):
    try:
        custom_filters = []
        if filtered_etf.custom_filters:
            custom_filters = [
                {
                    "factor": REVERSE_FACTOR_MAP[condition.factor],
                    "above": condition.above,
                    "below": condition.below,
                }
                for condition in filtered_etf.custom_filters
            ]

        request_columns = ["Code", "Name", "manager", "country"]
        reverse_factor_map = REVERSE_FACTOR_MAP
        if filtered_etf.lang == "en":
            reverse_factor_map = REVERSE_FACTOR_MAP_EN

        for column in [reverse_factor_map[column] for column in filtered_etf.factor_filters]:
            if column not in request_columns:
                request_columns.append(column)

        sort_by = "score"
        ascending = False
        if filtered_etf.sort_info:
            sort_by = reverse_factor_map[filtered_etf.sort_info.sort_by]
            ascending = filtered_etf.sort_info.ascending

        etfs_data, total_count = screener_etf_service.get_filtered_data(
            market_filter=filtered_etf.market_filter,
            custom_filters=custom_filters,
            columns=request_columns,
            limit=filtered_etf.limit,
            offset=filtered_etf.offset,
            sort_by=sort_by,
            ascending=ascending,
            lang=filtered_etf.lang,
        )

        has_next = filtered_etf.offset * filtered_etf.limit + filtered_etf.limit < total_count

        print("ETF", etfs_data[0].keys())
        if filtered_etf.lang == "kr":
            for etf in etfs_data:
                etf["시장"] = ETF_MARKET_MAP[etf["시장"]]

        result = {"data": etfs_data, "has_next": has_next}
        return result
    except CustomException as e:
        logger.exception(f"Error getting filtered etfs: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.exception(f"Error getting filtered etfs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/count", response_model=Dict)
def get_filtered_etfs_count(
    filtered_etf: FilteredETF, screener_etf_service: ScreenerETFService = Depends(ScreenerETFService)
):
    custom_filters = []
    if filtered_etf.custom_filters:
        custom_filters = [
            {
                "factor": REVERSE_FACTOR_MAP[condition.factor],
                "above": condition.above,
                "below": condition.below,
            }
            for condition in filtered_etf.custom_filters
        ]

    total_count = screener_etf_service.get_filtered_data_count(
        market_filter=filtered_etf.market_filter,
        custom_filters=custom_filters,
        sector_filter=None,
        columns=[REVERSE_FACTOR_MAP[column] for column in filtered_etf.factor_filters],
    )

    return {"count": total_count}


@router.post("/download", response_model=Dict)
def download_filtered_etfs(
    filtered_etf: FilteredETF, screener_etf_service: ScreenerETFService = Depends(ScreenerETFService)
):
    custom_filters = []
    if filtered_etf.custom_filters:
        custom_filters = [
            {
                "factor": condition.factor,
                "above": condition.above,
                "below": condition.below,
            }
            for condition in filtered_etf.custom_filters
        ]
    sorted_df = screener_etf_service.get_filtered_data(
        market_filter=filtered_etf.market_filter,
        sector_filter=filtered_etf.sector_filter,
        custom_filters=custom_filters,
        columns=[REVERSE_FACTOR_MAP[column] for column in filtered_etf.factor_filters],
    )

    stream = io.StringIO()
    sorted_df.to_csv(stream, index=False, encoding="utf-8-sig")  # 한글 인코딩

    return Response(
        content=stream.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="filtered_etfs.csv"'},
    )


@router.get("/groups", response_model=List[GroupMetaData])
def get_groups(
    current_user: str = Depends(get_current_user),
    screener_service: ScreenerETFService = Depends(ScreenerETFService),
):
    """
    저장된 필터 목록 조회
    """
    try:
        if current_user is None:
            return []
        groups = screener_service.get_groups(current_user.id, type=StockType.ETF)
        return [GroupMetaData(id=group["id"], name=group["name"], type=group["type"]) for group in groups]
    except Exception as e:
        logger.exception(f"Error getting groups: {e}")
        return []


@router.post("/groups", response_model=Dict)
async def create_or_update_group(
    group_filter: GroupFilter,
    current_user: str = Depends(get_current_user),
    screener_etf_service: ScreenerETFService = Depends(ScreenerETFService),
):
    """
    필터 생성 또는 업데이트
    """
    try:
        if group_filter.id:
            is_success = await screener_etf_service.update_group(
                group_id=group_filter.id,
                name=group_filter.name,
                market_filter=group_filter.market_filter,
                sector_filter=group_filter.sector_filter,
                custom_filters=group_filter.custom_filters,
                factor_filters=group_filter.factor_filters,
                category=group_filter.category,
                sort_info=group_filter.sort_info,
                type=group_filter.type,
            )
            message = "Filter updated successfully"
        else:
            is_success = await screener_etf_service.create_group(
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
        logger.error(f"Error creating group: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error creating group: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/groups/{group_id}", response_model=GroupFilterResponse)
def get_group_filters(
    group_id: int = -1,
    lang: str = "kr",
    screener_etf_service: ScreenerETFService = Depends(ScreenerETFService),
):
    """
    필터 목록 조회
    """
    try:
        technical_columns = screener_etf_service.get_columns(group_id, CategoryEnum.TECHNICAL, type=StockType.ETF)
        dividend_columns = screener_etf_service.get_columns(group_id, CategoryEnum.DIVIDEND, type=StockType.ETF)

        if lang == "en":
            technical_columns = [FACTOR_KOREAN_TO_ENGLISH_MAP[factor] for factor in technical_columns]
            dividend_columns = [FACTOR_KOREAN_TO_ENGLISH_MAP[factor] for factor in dividend_columns]

        technical_sort_info = screener_etf_service.get_sort_info(group_id, CategoryEnum.TECHNICAL)
        dividend_sort_info = screener_etf_service.get_sort_info(group_id, CategoryEnum.DIVIDEND)
        custom_sort_info = screener_etf_service.get_sort_info(group_id, CategoryEnum.CUSTOM)

        if group_id == -1:
            return GroupFilterResponse(
                id=-1,
                name="기본",
                market_filter=ETFMarketEnum.US,
                type=StockType.ETF,
                has_custom=False,
                sector_filter=[],
                custom_filters=[],
                factor_filters={"technical": technical_columns, "dividend": dividend_columns, "custom": []},
                sort_info={
                    CategoryEnum.TECHNICAL: technical_sort_info,
                    CategoryEnum.DIVIDEND: dividend_sort_info,
                    CategoryEnum.CUSTOM: custom_sort_info,
                },
            )

        group_filters = screener_etf_service.get_group_filters(group_id)
        stock_filters = group_filters["stock_filters"]

        market_filter = None
        sector_filter = []
        custom_filters = []

        for stock_filter in stock_filters:
            if stock_filter["factor"] == "시장":
                market_filter = stock_filter["value"]
            elif stock_filter["factor"] == "산업":
                continue
            else:
                custom_filters.append(stock_filter)

        custom_factor_filters = group_filters["custom_factor_filters"]

        if lang == "en":
            custom_factor_filters = [FACTOR_KOREAN_TO_ENGLISH_MAP[factor] for factor in custom_factor_filters]

        return GroupFilterResponse(
            id=group_id,
            name=group_filters["name"],
            market_filter=market_filter if market_filter else ETFMarketEnum.US,
            type=StockType.ETF,
            sector_filter=sector_filter,
            custom_filters=custom_filters,
            factor_filters={
                "technical": technical_columns,
                "dividend": dividend_columns,
                "custom": custom_factor_filters,
            },
            sort_info={
                CategoryEnum.TECHNICAL: technical_sort_info,
                CategoryEnum.DIVIDEND: dividend_sort_info,
                CategoryEnum.CUSTOM: custom_sort_info,
            },
            has_custom=group_filters["has_custom"],
        )
    except Exception as e:
        logger.error(f"Error getting group filters: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/groups/{group_id}", response_model=Dict)
def delete_group(
    group_id: int,
    current_user: str = Depends(get_current_user),
    screener_etf_service: ScreenerETFService = Depends(ScreenerETFService),
):
    if current_user.id is None:
        raise HTTPException(status_code=401, detail="Unauthorized")

    check_owner = screener_etf_service.check_owner(group_id=group_id, user_id=current_user.id)
    if not check_owner:
        raise HTTPException(status_code=403, detail="Forbidden")

    result = screener_etf_service.delete_group(group_id=group_id)
    if result:
        return {"message": "Group deleted successfully"}
    else:
        raise HTTPException(status_code=500, detail="Failed to delete group")


@router.post("/groups/reorder", response_model=Dict)
def reorder_groups(
    groups: List[int],
    current_user: str = Depends(get_current_user),
    screener_etf_service: ScreenerETFService = Depends(ScreenerETFService),
):
    screener_etf_service.validate_group(group_ids=groups)
    check_owner = screener_etf_service.check_owner(group_id=groups, user_id=current_user.id)
    if check_owner is False:
        raise HTTPException(status_code=403, detail="Forbidden")

    is_success = screener_etf_service.reorder_groups(groups=groups)
    if is_success:
        return {"message": "Group reordered successfully"}
    else:
        raise HTTPException(status_code=500, detail="Failed to reorder groups")


@router.post("/groups/name")
def update_group_name(group_id: int, name: str, screener_etf_service: ScreenerETFService = Depends(ScreenerETFService)):
    """
    그룹 이름 수정
    """
    try:
        updated_group_name = screener_etf_service.update_group_name(group_id, name)
        return {"message": f"Group name updated to {updated_group_name}"}
    except Exception as e:
        logger.error(f"Error updating group name: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/parquet/{ctry}")
def update_parquet(ctry: Literal["KR", "US"], screener_etf_service: ScreenerETFService = Depends(ScreenerETFService)):
    try:
        result = screener_etf_service.update_parquet(ctry=ctry)
        return result
    except Exception as e:
        logger.exception(e)
        raise e


@router.get("/old/parquet")
def get_old_parquet(screener_etf_service: ScreenerETFService = Depends(ScreenerETFService)):
    try:
        run_etf_screener_data()
        return True
    except Exception as e:
        logger.exception(e)
        raise e
