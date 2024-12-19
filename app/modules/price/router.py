from fastapi import APIRouter, Depends, Query
from app.modules.common.schemas import BaseResponse
from app.modules.price.services import PriceService, get_price_service
from app.modules.price.schemas import ResponsePriceDataItem
from datetime import date
from typing import Annotated, Optional
from app.modules.common.enum import Country, Frequency

router = APIRouter()


@router.get("", response_model=BaseResponse[ResponsePriceDataItem])
async def get_price_data(
    ctry: Annotated[Country, Query(description="Country code (kr/us)")],
    ticker: Annotated[str, Query(description="Stock ticker symbol")],
    frequency: Annotated[Frequency, Query(description="Frequency (daily/minute)")],
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    service: PriceService = Depends(get_price_service),
):
    """
    Get price data for a specific country and ticker from database.
    """

    if ctry == Country.KR and frequency == Frequency.MINUTE:
        return BaseResponse(status_code=400, message=f"{ctry.value}의 분 단위 데이터는 없습니다.", data=None)

    return await service.read_price_data(
        ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date, frequency=frequency
    )


# @router.get("/v2", response_model=BaseResponse[ResponsePriceDataItem])
# async def get_price_data_v2(
#     ctry: Annotated[Country, Query(description="Country code (kr/us)")],
#     ticker: Annotated[str, Query(description="Stock ticker symbol")],
#     frequency: Annotated[Frequency, Query(description="Frequency (daily/minute)")],
#     # market: Annotated[Market, Query(description="Market type (stock/crypto/forex)")] = Market.STOCK,
#     service: services_v2.PriceService = Depends(services_v2.get_price_service),
#     start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
#     end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
# ):
#     """
#     Get price data for a specific country and ticker from database.

#     Args:
#         ctry: 국가 코드 (kr/us)
#         ticker: 종목 티커
#         frequency: 데이터 주기 (daily/minute)
#         start_date: 시작일 (optional)
#         end_date: 종료일 (optional)
#     """
#     if ctry == Country.KR and frequency == Frequency.MINUTE:
#         return BaseResponse(status_code=400, message=f"{ctry.value}의 분 단위 데이터는 없습니다.", data=None)
#     if ctry in [Country.JPN, Country.HKG]:
#         return BaseResponse(status_code=400, message=f"{ctry.value}의 분/일 단위 데이터는 없습니다.", data=None)

#     return await service.read_price_data(
#         country=ctry,
#         ticker=ticker,
#         frequency=frequency,
#         start_date=start_date,
#         end_date=end_date
#     )
