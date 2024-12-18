from fastapi import APIRouter, Depends

from app.modules.common.enum import Country
from app.modules.common.schemas import BaseResponse
from app.modules.stock_info.schemas import Indicators, StockInfo
from .services import StockInfoService, get_stock_info_service

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
    ctry: Country,
    ticker: str,
    service: StockInfoService = Depends(get_stock_info_service),
):
    data = await service.get_indicators(ctry, ticker)
    return BaseResponse(status_code=200, message="지표 정보를 성공적으로 조회했습니다.", data=data)
