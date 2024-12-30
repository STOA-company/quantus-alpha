from fastapi import APIRouter, Depends
from app.modules.trending.schemas import TrendingStockResponse
from app.modules.trending.old_service import (
    TrendingService as OldTrendingService,
    get_trending_service as get_old_trending_service,
)


router = APIRouter()


@router.get("old", summary="급상승 종목 조회 - 옛날 버전")
def old_get_trending_stocks(
    old_service: OldTrendingService = Depends(get_old_trending_service),
) -> TrendingStockResponse:
    return old_service.get_trending_stocks()


# @router.get("", summary="급상승 종목")
# def get_trending_stocks(
#     ctry: str = Query(default="us"),
#     service: TrendingService = Depends(get_trending_service),
#     db: Session = Depends(db.get_db),
# ) -> TrendingStockResponse:
#     return service.get_tranding_stocks(db, ctry)
