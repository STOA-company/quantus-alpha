from fastapi import APIRouter, Depends, Query, Request
from app.modules.common.schemas import BaseResponse
from app.modules.price.services import PriceService, get_price_service
from app.modules.price.schemas import RealTimePriceDataItem, ResponsePriceDataItem
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


@router.get("/real_time", response_model=BaseResponse[RealTimePriceDataItem])
async def get_real_time_price_data(
    ticker: Annotated[str, Query(description="예시 : AAPL, A000020")],
    request: Request,
    service: PriceService = Depends(get_price_service),
):
    """
    일회성 실시간 가격 데이터 조회 엔드포인트
    현재는 데이터가 전일 데이터를 기준으로 조회됩니다.
    """
    return await service.get_real_time_price_data(ticker, request)


@router.get("/real_time/stream")
async def stream_real_time_price_data(
    ticker: Annotated[str, Query(description="Stock ticker symbol")],
    request: Request,
    service: PriceService = Depends(get_price_service),
):
    """
    실시간 가격 데이터 스트림 엔드포인트
    현재는 데이터가 전일 데이터를 기준으로 조회됩니다.
    """
    return await service.stream_real_time_price_data(ticker, request)
