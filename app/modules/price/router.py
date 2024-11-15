from fastapi import APIRouter, Depends, Query
from app.modules.common.schemas import BaseResponse
from app.modules.price.services import PriceService, get_price_service
from app.modules.price.schemas import PriceDataItem, VolumeDataItem
from datetime import date
from typing import Annotated, List, Optional
from app.modules.common.enum import Country

router = APIRouter()


@router.get("/", response_model=BaseResponse[List[PriceDataItem]])
async def get_price_data(
    ctry: Annotated[Country, Query(description="Country code (kr/us)")],
    ticker: Annotated[str, Query(description="Stock ticker symbol")],
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    service: PriceService = Depends(get_price_service),
):
    """
    Get price data for a specific country and ticker from database.
    """
    return await service.read_price_data(ctry, ticker, start_date, end_date)


@router.get("/volume", response_model=BaseResponse[List[VolumeDataItem]])  # 여기를 수정
async def get_volume_data(
    ctry: Annotated[Country, Query(..., description="Country code (kr/us)")],
    ticker: Annotated[str, Query(..., description="Stock ticker symbol")],
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    service: PriceService = Depends(get_price_service),
):
    """
    Get volume data for a specific country and ticker from database.
    """
    return await service.read_volume_data(ctry, ticker, start_date, end_date)
