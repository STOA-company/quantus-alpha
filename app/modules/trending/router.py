from fastapi import APIRouter, Depends
from app.modules.trending.schemas import TrendingStock
from app.modules.trending.service import TrendingService, get_trending_service


router = APIRouter()


@router.get("", summary="급상승 종목 조회")
async def get_trending_stocks(
    service: TrendingService = Depends(get_trending_service),
) -> TrendingStock:
    return await service.get_trending_stocks()
