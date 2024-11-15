from fastapi import APIRouter, Depends, Query

from app.modules.price.services import PriceService, get_price_service
from app.modules.price.schemas import PriceDataResponse
from datetime import date
from typing import Optional
from app.modules.common.enum import Country

router = APIRouter()


@router.get("/", response_model=PriceDataResponse)
async def get_price_data(
    ctry: Country = Query(..., description="Country code (kr/us)"),
    ticker: str = Query(..., description="Stock ticker symbol"),
    start_date: Optional[date] = Query(None, description="Start date for price data (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date for price data (YYYY-MM-DD)"),
    service: PriceService = Depends(get_price_service),
):
    """
    Get price data for a specific country and ticker from database.
    """
    return await service.read_price_data(ctry, ticker, start_date, end_date)
