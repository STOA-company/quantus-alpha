from fastapi import APIRouter, Depends
from .service import StockIndicesService

router = APIRouter()


@router.get("/five", summary="코스피/코스닥 지수 조회 5분 간격")
async def get_stock_indices_five(
    service: StockIndicesService = Depends(StockIndicesService),
):
    result = await service.get_indices_data()

    if not result:
        return {"status_code": 404, "message": "오늘의 거래 데이터가 아직 없습니다.", "data": None}

    return {"status_code": 200, "message": "코스피/코스닥 지수를 성공적으로 조회했습니다.", "data": result}
