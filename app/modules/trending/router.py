from fastapi import APIRouter, Depends
from app.modules.trending.old_schemas import TrendingStock
from app.modules.trending.old_service import TrendingService, get_trending_service


router = APIRouter()


@router.get("", summary="급상승 종목 조회 - 옛날 버전")
def get_trending_stocks(
    service: TrendingService = Depends(get_trending_service),
) -> TrendingStock:
    return service.get_trending_stocks()


# @router.get("", summary="급상승 종목")
# def get_trending_stocks(
#     ctry: str = Query(default="us"),
#     service: TrendingService = Depends(get_trending_service),
#     db: Session = Depends(db.get_db),
# ) -> TrendingStockResponse:
#     return service.get_tranding_stocks(db, ctry)
