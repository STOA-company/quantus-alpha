from fastapi import APIRouter, Depends

from app.modules.stock_indices.schemas import IndexSummary, IndicesData

from .services import StockIndicesService

router = APIRouter()


@router.get("", summary="코스피/코스닥/나스닥/S&P500 지수 조회 일봉 간격", response_model=IndicesData)
async def get_stock_indices(
    service: StockIndicesService = Depends(StockIndicesService),
) -> IndicesData:
    try:
        result = await service.get_indices_data()
        return result
    except Exception:
        empty_summary = IndexSummary(
            prev_close=0.0, change=0.0, change_percent=0.0, rise_ratio=0, fall_ratio=0, unchanged_ratio=0
        )

        return IndicesData(
            status_code=404,
            message="오늘의 거래 데이터가 아직 없습니다.",
            kospi=empty_summary,
            kosdaq=empty_summary,
            nasdaq=empty_summary,
            sp500=empty_summary,
            data=None,
        )


# @router.get("/market-status")
# def get_market_status(
#     service: StockIndicesService = Depends(StockIndicesService),
# ):
#     nasdaq = service.get_nasdaq_ticker()
#     sp500 = service.get_snp500_ticker()
#     return nasdaq, snp500
