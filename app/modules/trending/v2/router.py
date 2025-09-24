from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from app.cache.cache_decorator import one_minute_cache
from app.modules.common.enum import TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.trending.v2.schemas import TrendingStock, TrendingStockRequest
from app.modules.trending.v2.service import TrendingService, get_trending_service

router = APIRouter()


@router.get("", summary="실시간 차트", response_model=BaseResponse[List[TrendingStock]])
# @one_minute_cache(prefix="trending")
async def get_trending_stocks(
    request: TrendingStockRequest = Depends(),
    service: TrendingService = Depends(get_trending_service),
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = None,
):
    data = await service.get_trending_stocks(request, lang)
    return BaseResponse(status_code=200, message="success", data=data)
