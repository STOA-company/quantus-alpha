import csv
import io
from datetime import datetime
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from requests import Session

from app.core.exception.handler import exception_handler
from app.core.logger import setup_logger
from app.database.conn import db
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import FinancialCountry, TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.common.utils import async_check_ticker_country_len_3, check_ticker_country_len_3, contry_mapping

from app.modules.financial.v2.schemas import IncomePerformanceResponse
from app.modules.financial.v2.services import FinancialService, get_financial_service


logger = setup_logger(__name__)
router = APIRouter()

@router.get(
    "/income-performance",
    response_model=BaseResponse[IncomePerformanceResponse],
    summary="실적 부분 조회 api",
)
async def get_income_performance_data(
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    lang: Annotated[TranslateCountry, Query(description="언어, 예시: KO, EN")] = TranslateCountry.KO,
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
):
    try: 
        ctry = await async_check_ticker_country_len_3(ticker)
        ctry = ctry.upper()
        result = await financial_service.get_income_performance_data(
            ctry=ctry, ticker=ticker, lang=lang, start_date=start_date, end_date=end_date
        )
        return result
    except Exception as error:
        logger.error(f"Income performance data 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        raise HTTPException(status_code=500, detail=error)

