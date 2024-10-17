from fastapi import APIRouter, Depends, Query
from app.modules.price.services import get_price_service, PriceService
from app.modules.price.schemas import PriceDataResponse
from datetime import date
from typing import Optional

router = APIRouter()

@router.get("/", response_model=PriceDataResponse)
async def get_price_data(
    ctry: str = Query(..., description="Country code"),
    ticker: str = Query(..., description="Stock ticker symbol"),
    start_date: Optional[date] = Query(None, description="Start date for price data (format: YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date for price data (format: YYYY-MM-DD)"),
    service: PriceService = Depends(get_price_service)
):
    """
    Get price data for a specific country and ticker.
    """
    return await service.read_price_data(ctry, ticker, start_date, end_date)