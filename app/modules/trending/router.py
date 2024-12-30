from typing import List
from fastapi import APIRouter, Depends, Query
from app.modules.common.enum import TrendingPeriod, TrendingType
from app.modules.trending.new_schemas import TrendingStockRequest, TrendingStock as NewTrendingStock
from app.modules.trending.new_service import NewTrendingService, new_get_trending_service
from app.modules.common.schemas import BaseResponse
# from app.modules.trending.old_service import (
#     TrendingService as OldTrendingService,
#     get_trending_service as get_old_trending_service,
# )


router = APIRouter()


# @router.get("", summary="급상승 종목 조회")
# def get_trending_stocks(
#     service: TrendingService = Depends(get_trending_service),
# ) -> TrendingStockResponse:
#     return service.get_trending_stocks()

# @router.put("us", summary="미국 종목 트렌드 업데이트")
# def update_us_trending_stocks(
#     service: TrendingService = Depends(get_trending_service),
# ) -> TrendingStockResponse:
#     return service.insert_us_tickers_to_trend()


@router.get("/new", summary="실시간 차트")
def get_us_trending_stocks(
    ctry: str = Query("us", description="국가 코드"),
    type: TrendingType = Query(TrendingType.UP, description="트렌딩 타입"),
    period: TrendingPeriod = Query(TrendingPeriod.REALTIME, description="기간"),
    service: NewTrendingService = Depends(new_get_trending_service),
) -> BaseResponse[List[NewTrendingStock]]:
    request = TrendingStockRequest(ctry=ctry, type=type, period=period)
    data = service.get_trending_stocks(request)
    return BaseResponse(status_code=200, message="success", data=data)
