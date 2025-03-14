from typing import Annotated, List
from fastapi import APIRouter, Depends, Query
from app.modules.common.enum import TranslateCountry
from app.modules.trending.schemas import TrendingStockRequest, TrendingStock
from app.modules.trending.service import TrendingService, get_trending_service
from app.modules.common.schemas import BaseResponse


router = APIRouter()


@router.get("", summary="실시간 차트")
def get_trending_stocks(
    request: TrendingStockRequest = Depends(),
    service: TrendingService = Depends(get_trending_service),
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = None,
) -> BaseResponse[List[TrendingStock]]:
    data = service.get_trending_stocks(request, lang)
    return BaseResponse(status_code=200, message="success", data=data)
