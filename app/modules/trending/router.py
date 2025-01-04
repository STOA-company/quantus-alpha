from typing import List
from fastapi import APIRouter, Depends
from app.modules.trending.schemas import TrendingStockRequest, TrendingStock
from app.modules.trending.service import TrendingService, get_trending_service
from app.modules.common.schemas import BaseResponse


router = APIRouter()


@router.get("", summary="실시간 차트")
def get_trending_stocks(
    request: TrendingStockRequest = Depends(),
    service: TrendingService = Depends(get_trending_service),
) -> BaseResponse[List[TrendingStock]]:
    data = service.get_trending_stocks(request)
    return BaseResponse(status_code=200, message="success", data=data)
