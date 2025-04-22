from datetime import date
from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, Query

from app.modules.common.enum import Country
from app.modules.common.schemas import BaseResponse
from app.modules.price.schemas import PriceDailyItem
from app.modules.price.services_v2 import PriceService, get_price_service

router = APIRouter()


# @router.get("/minute", response_model=BaseResponse[PriceSummaryResponse])
# async def get_price_data_minute(
#     ctry: Annotated[Country, Query(description="국가 코드 (kr/us)")],
#     ticker: Annotated[str, Query(description="종목 티커")],
#     service: PriceService = Depends(get_price_service),
# ):
#     data = service.get_price_data_minute(ctry, ticker)


#     return BaseResponse(status_code=200, message="Success", data=data)


@router.get("/daily", response_model=BaseResponse[List[PriceDailyItem]])
async def get_price_data_daily(
    ctry: Annotated[Country, Query(description="국가 코드 (kr/us)")],
    ticker: Annotated[str, Query(description="종목 티커")],
    start_date: Annotated[Optional[date], Query(description="시작 날짜")] = None,
    end_date: Annotated[Optional[date], Query(description="종료 날짜")] = None,
    service: PriceService = Depends(get_price_service),
):
    data = await service.get_price_data_daily(ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date)
    return BaseResponse(status_code=200, message="Success", data=data)


# @router.get("/summary", response_model=BaseResponse[PriceSummaryItem])
async def get_price_data_summary(
    ctry: Annotated[Country, Query(description="국가 코드 (kr/us)")],
    ticker: Annotated[str, Query(description="종목 티커")],
    service: PriceService = Depends(get_price_service),
):
    data = await service.get_price_data_summary(ctry=ctry, ticker=ticker)
    return BaseResponse(status_code=200, message="Success", data=data)
