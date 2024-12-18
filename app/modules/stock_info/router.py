from fastapi import APIRouter, Depends

from app.modules.common.enum import Country
from app.modules.common.schemas import BaseResponse
from app.modules.common.utils import check_ticker_country_len_2
from app.modules.price.services_v2 import PriceService, get_price_service
from app.modules.stock_info.schemas import Indicators, StockInfo
from .services import StockInfoService, get_stock_info_service
from app.database.conn import db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


@router.get("", response_model=BaseResponse[StockInfo], summary="주식 정보 조회")
async def get_stock_info(
    ctry: Country,
    ticker: str,
    service: StockInfoService = Depends(get_stock_info_service),
):
    data = await service.get_stock_info(ctry, ticker)
    return BaseResponse(status_code=200, message="주식 정보를 성공적으로 조회했습니다.", data=data)


@router.get("/indicators", response_model=BaseResponse[Indicators], summary="지표 조회")
async def get_indicators(
    ticker: str,
    service: StockInfoService = Depends(get_stock_info_service),
):
    ctry = check_ticker_country_len_2(ticker)
    data = await service.get_indicators(ctry, ticker)
    return BaseResponse(status_code=200, message="지표 정보를 성공적으로 조회했습니다.", data=data)


@router.get("/combined", summary="종목 정보, 지표, 기업 정보 전체 조회")
async def get_combined(
    ticker: str,
    stock_service: StockInfoService = Depends(get_stock_info_service),
    summary_service: PriceService = Depends(get_price_service),
    db: AsyncSession = Depends(db.get_async_db),
):
    ctry = check_ticker_country_len_2(ticker)

    stock_info = await stock_service.get_stock_info(ctry, ticker, db)
    indicators = await stock_service.get_indicators(ctry, ticker, db)
    summary = await summary_service.get_price_data_summary(ctry, ticker, db)

    data = {
        "summary": summary,
        "indicators": indicators,
        "stock_info": stock_info,
    }

    return BaseResponse(status_code=200, message="종목 정보, 지표, 기업 정보를 성공적으로 조회했습니다.", data=data)
