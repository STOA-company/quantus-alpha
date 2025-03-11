import io
from typing import Dict, List, Literal, Optional
from fastapi import APIRouter, Depends, HTTPException, Response

from app.batches.run_etf_screener import run_etf_screener_data
from app.models.models_factors import CategoryEnum
from app.modules.screener.schemas import ColumnsResponse, FactorResponse, GroupFilter, GroupFilterResponse, GroupMetaData
from app.modules.screener_etf.enum import ETFCategoryEnum, ETFMarketEnum
from app.modules.screener_etf.schemas import FilteredETF
from app.modules.screener_etf.service import ScreenerETFService
from app.utils.oauth_utils import get_current_user
from app.core.logging.config import get_logger


router = APIRouter()
logger = get_logger(__name__)


@router.get("/factors/{market}", response_model=List[FactorResponse])
def get_factors(market: ETFMarketEnum, screener_etf_service: ScreenerETFService = Depends(ScreenerETFService)):
    result = screener_etf_service.get_etf_factors(market=market)

    return result


@router.post("", response_model=Dict)
def get_filtered_etfs(filtered_etf: FilteredETF, screener_etf_service: ScreenerETFService = Depends(ScreenerETFService)):
    result, has_next = screener_etf_service.get_filtered_etfs(filtered_etf=filtered_etf)
    result = {"data": result, "has_next": has_next}
    return result


@router.post("/count", response_model=Dict)
def get_filtered_etfs_count(
    filtered_etf: FilteredETF, screener_etf_service: ScreenerETFService = Depends(ScreenerETFService)
):
    result = screener_etf_service.get_filtered_etfs_count(filtered_etf=filtered_etf)
    return {"count": result}


@router.post("/description", response_model=Dict)
def get_filtered_etfs_description(
    filtered_etf: FilteredETF, screener_etf_service: ScreenerETFService = Depends(ScreenerETFService)
):
    result, has_next = screener_etf_service.get_filtered_etfs_description(filtered_etf=filtered_etf)
    return {"data": result, "has_next": has_next}


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
    sorted_df = screener_etf_service.get_filtered_etfs(
        filtered_etf.market_filter, filtered_etf.sector_filter, custom_filters, filtered_etf.columns
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
    current_user: str = Depends(get_current_user), screener_etf_service: ScreenerETFService = Depends(ScreenerETFService)
):
    if current_user.id is None:
        raise HTTPException(status_code=401, detail="Unauthorized")

    result = screener_etf_service.get_groups(current_user.id, type="ETF")

    return result


@router.post("/groups", response_model=Dict)
def create_or_update_group(
    group_filter: GroupFilter,
    current_user: str = Depends(get_current_user),
    screener_etf_service: ScreenerETFService = Depends(ScreenerETFService),
):
    result = screener_etf_service.create_or_update_group(current_user=current_user, group_filter=group_filter, type="ETF")

    return result


@router.get("/groups/{group_id}", response_model=GroupFilterResponse)
def get_group_filter(
    group_id: int = -1,
    category: CategoryEnum = CategoryEnum.TECHNICAL,
    screener_etf_service: ScreenerETFService = Depends(ScreenerETFService),
):
    """
    필터 목록 조회
    """
    try:
        if group_id == -1:
            return GroupFilterResponse(
                id=-1,
                name="기본",
                market_filter=ETFMarketEnum.US,
                category=category,
                has_custom=False,
                stock_filters=[],
                custom_filters=[],
                factor_filters=screener_etf_service.get_columns(group_id, category),
            )

        group_filters = screener_etf_service.get_group_filters(group_id, category)
        stock_filters = group_filters["stock_filters"]

        market_filter = None
        custom_filters = []

        for stock_filter in stock_filters:
            if stock_filter["factor"] == "시장":
                market_filter = stock_filter["value"]
            elif stock_filter["factor"] == "산업":
                continue
            else:
                custom_filters.append(stock_filter)

        factor_filters = group_filters["factor_filters"]
        return GroupFilterResponse(
            id=group_id,
            name=group_filters["name"],
            market_filter=market_filter,
            sector_filter=[],
            custom_filters=custom_filters,
            factor_filters=factor_filters,
            category=category,
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


@router.get("/columns", response_model=ColumnsResponse)
def get_columns(
    category: Optional[ETFCategoryEnum] = None,
    id: Optional[int] = None,
    screener_etf_service: ScreenerETFService = Depends(ScreenerETFService),
):
    result = screener_etf_service.get_columns(category=category, group_id=id)
    return ColumnsResponse(columns=result)


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
