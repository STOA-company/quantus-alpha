from datetime import date
import logging
from fastapi import Request
from app.core.exception.handler import exception_handler
from app.enum.financial import FinancialSelect
from app.modules.common.enum import Country
from app.modules.common.schemas import BaseResponse, PandasStatistics
from fastapi import APIRouter, Depends, Query
from app.modules.financial.services import FinancialService, get_financial_service
from .schemas import (
    CashFlowDetail,
    FinPosDetail,
    IncomeStatementDetail,
    NetIncomeStatement,
    OperatingProfitStatement,
    RevenueStatement,
)
from typing import List, Optional, Annotated, Union

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/income-performance",
    response_model=Union[
        BaseResponse[List[RevenueStatement]],
        BaseResponse[List[OperatingProfitStatement]],
        BaseResponse[List[NetIncomeStatement]],
        BaseResponse[List[IncomeStatementDetail]],
    ],
    summary="실적 부분 조회 api",
)
async def get_income_performance_data(
    request: Request,
    ctry: Annotated[Country, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    select: Annotated[
        Optional[FinancialSelect], Query(description="조회 항목 선택 (revenue - 기본, operating_profit, net_income)")
    ] = FinancialSelect.REVENUE,
    start_date: Annotated[Optional[date], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[date], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
):
    try:
        result = await financial_service.get_income_performance_data(
            ctry=ctry, ticker=ticker, select=select, start_date=start_date, end_date=end_date
        )
        return result

    except Exception as error:
        logger.error(f"Income performance data 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        return await exception_handler(request, error)


# @router.get("/income", response_model=BaseResponse[List[IncomeStatementDetail]], summary="손익계산서 분기별 조회")
# async def get_income_data(
#     request: Request,
#     ctry: Annotated[Country, Query(description="국가 코드")],
#     ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
#     start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
#     end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
#     financial_service: FinancialService = Depends(get_financial_service),
# ):
#     try:
#         result = await financial_service.get_income_data(
#             ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date
#         )
#         return result

#     except Exception as error:
#         logger.error(f"Income statement 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
#         return await exception_handler(request, error)


# @router.get("/cashflow", response_model=BaseResponse[List[CashFlowDetail]], summary="현금흐름 분기별 조회")
# async def get_cashflow_data(
#     request: Request,
#     ctry: Annotated[Country, Query(description="국가 코드")],
#     ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
#     start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
#     end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
#     financial_service: FinancialService = Depends(get_financial_service),
# ) -> BaseResponse[List[CashFlowDetail]]:
#     try:
#         result = await financial_service.get_cashflow_data(
#             ctry=ctry,
#             ticker=ticker,
#             start_date=start_date,
#             end_date=end_date,
#         )
#         return result

#     except Exception as error:
#         logger.error(f"Cashflow 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
#         return await exception_handler(request, error)


# @router.get("/finpos", response_model=BaseResponse[List[FinPosDetail]], summary="재무제표 분기별 조회")
# async def get_finpos_data(
#     request: Request,
#     ctry: Annotated[Country, Query(description="국가 코드")],
#     ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
#     start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
#     end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
#     financial_service: FinancialService = Depends(get_financial_service),
# ) -> BaseResponse[List[FinPosDetail]]:
#     try:
#         result = await financial_service.get_finpos_data(
#             ctry=ctry,
#             ticker=ticker,
#             start_date=start_date,
#             end_date=end_date,
#         )
#         return result

#     except Exception as error:
#         logger.error(f"Financial position 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
#         return await exception_handler(request, error)


@router.get(
    "/income-pandas",
    response_model=PandasStatistics[List[IncomeStatementDetail]],
    summary="손익계산서 pandas 사용한 분석",
)
async def get_income_analysis(
    request: Request,
    ctry: Annotated[Country, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
) -> PandasStatistics[List[IncomeStatementDetail]]:
    try:
        result = await financial_service.get_income_analysis(
            ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date
        )
        return result

    except Exception as error:
        logger.error(f"Income analysis 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        return await exception_handler(request, error)


@router.get(
    "/cashflow-pandas",
    response_model=PandasStatistics[List[CashFlowDetail]],
    summary="현금흐름 pandas 사용한 분석",
)
async def get_cashflow_analysis(
    request: Request,
    ctry: Annotated[Country, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
) -> PandasStatistics[List[CashFlowDetail]]:
    try:
        result = await financial_service.get_cashflow_analysis(
            ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date
        )
        return result

    except Exception as error:
        logger.error(f"Cashflow analysis 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        return await exception_handler(request, error)


@router.get(
    "/finpos-pandas",
    response_model=PandasStatistics[List[FinPosDetail]],
    summary="재무상태표 pandas 사용한 분석",
)
async def get_finpos_analysis(
    request: Request,
    ctry: Annotated[Country, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
) -> PandasStatistics[List[FinPosDetail]]:
    try:
        result = await financial_service.get_finpos_analysis(
            ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date
        )
        return result

    except Exception as error:
        logger.error(f"Financial position analysis 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        return await exception_handler(request, error)
