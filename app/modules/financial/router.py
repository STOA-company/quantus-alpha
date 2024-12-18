import logging
from fastapi import Request, HTTPException
from app.core.exception.handler import exception_handler
from app.modules.common.enum import FinancialCountry
from app.modules.common.schemas import BaseResponse
from fastapi import APIRouter, Depends, Query
from app.modules.common.utils import check_ticker_contry_len_3
from app.modules.financial.services import FinancialService, get_financial_service
from .schemas import (
    CashFlowResponse,
    FinPosResponse,
    IncomePerformanceResponse,
    IncomeStatementResponse,
    RatioResponse,
)
from typing import Optional, Annotated
from app.modules.common.utils import contry_mapping

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/income-performance",
    response_model=BaseResponse[IncomePerformanceResponse],
    summary="실적 부분 조회 api",
)
async def get_income_performance_data(
    request: Request,
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
) -> BaseResponse[IncomePerformanceResponse]:
    try:
        ctry = check_ticker_contry_len_3(ticker).upper()
        return await financial_service.get_income_performance_data(
            ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date
        )
    except HTTPException as http_error:
        logger.error(
            f"Income performance data 조회 실패: {http_error.status_code}: {http_error.detail}, ticker: {ticker}, country: {ctry}"
        )
        raise http_error
    except Exception as error:
        logger.error(f"Income performance data 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        raise HTTPException(status_code=500, detail="내부 서버 오류")


@router.get(
    "/income",
    response_model=BaseResponse[IncomeStatementResponse],
    summary="손익계산서",
)
async def get_income_analysis(
    request: Request,
    ctry: Annotated[FinancialCountry, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
) -> BaseResponse[IncomeStatementResponse]:
    try:
        result = await financial_service.get_income_analysis(
            ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date
        )
        return result

    except Exception as error:
        logger.error(f"Income analysis 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        return await exception_handler(request, error)


@router.get(
    "/cashflow",
    response_model=BaseResponse[CashFlowResponse],
    summary="현금흐름",
)
async def get_cashflow_analysis(
    request: Request,
    ctry: Annotated[FinancialCountry, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
) -> BaseResponse[CashFlowResponse]:
    try:
        result = await financial_service.get_cashflow_analysis(
            ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date
        )
        return result

    except Exception as error:
        logger.error(f"Cashflow analysis 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        return await exception_handler(request, error)


@router.get(
    "/finpos",
    response_model=BaseResponse[FinPosResponse],
    summary="재무상태표",
)
async def get_finpos_analysis(
    request: Request,
    ctry: Annotated[FinancialCountry, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
) -> BaseResponse[FinPosResponse]:
    try:
        result = await financial_service.get_finpos_analysis(
            ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date
        )
        return result

    except Exception as error:
        logger.error(f"Financial position analysis 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        return await exception_handler(request, error)


@router.get(
    "/financial-ratio",
    response_model=BaseResponse[RatioResponse],
    summary="재무 api",
)
async def get_financial_ratio(
    request: Request,
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    financial_service: FinancialService = Depends(get_financial_service),
) -> BaseResponse[RatioResponse]:
    try:
        ctry = check_ticker_contry_len_3(ticker).upper()
        company_name, result1 = await financial_service.get_financial_ratio(ctry=ctry, ticker=ticker)
        result2 = await financial_service.get_liquidity_ratio(ctry=ctry, ticker=ticker)
        result3 = await financial_service.get_interest_coverage_ratio(ctry=ctry, ticker=ticker)
        ctry_two = contry_mapping.get(ctry)

        return BaseResponse[RatioResponse](
            status_code=200,
            message="재무 데이터를 성공적으로 조회했습니다.",
            data=RatioResponse(
                code=ticker,
                name=company_name,
                ctry=ctry_two,
                financial_ratios=result1.data,
                liquidity_ratios=result2.data,
                interest_coverage_ratios=result3.data,
            ),
        )

    except Exception as error:
        logger.error(f"Financial ratio 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        return await exception_handler(request, error)
