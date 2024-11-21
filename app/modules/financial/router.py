from datetime import date
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
import pandas as pd

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
    ctry: Annotated[Country, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    select: Annotated[
        Optional[FinancialSelect], Query(description="조회 항목 선택 (revenue - 기본, operating_profit, net_income)")
    ] = FinancialSelect.REVENUE,
    start_date: Annotated[Optional[date], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[date], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
):
    result = await financial_service.get_income_performance_data(
        ctry=ctry, ticker=ticker, select=select, start_date=start_date, end_date=end_date
    )
    return result


@router.get("/income", response_model=BaseResponse[List[IncomeStatementDetail]], summary="손익계산서 분기별 조회")
async def get_income_data(
    ctry: Annotated[Country, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
):
    result = await financial_service.get_income_data(ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date)
    return result


@router.get("/cashflow", response_model=BaseResponse[List[CashFlowDetail]], summary="현금흐름 분기별 조회")
async def get_cashflow_data(
    ctry: Annotated[Country, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
) -> BaseResponse[List[CashFlowDetail]]:
    result = await financial_service.get_cashflow_data(
        ctry=ctry,
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
    )
    return result


@router.get("/finpos", response_model=BaseResponse[List[FinPosDetail]], summary="재무제표 분기별 조회")
async def get_finpos_data(
    ctry: Annotated[Country, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
) -> BaseResponse[List[FinPosDetail]]:
    result = await financial_service.get_finpos_data(
        ctry=ctry,
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
    )
    return result


@router.get(
    "/income-pandas/timeseries",
    response_model=PandasStatistics[List[IncomeStatementDetail]],
    summary="손익계산서 시계열 분석",
)
async def get_income_timeseries_analysis(
    ctry: Annotated[Country, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
):
    result = PandasStatistics(status="200", message="Success", data=[], statistics={})

    income_data = await financial_service.get_income_data(
        ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date
    )

    result.data = income_data.data

    # DataFrame 생성 및 시계열 인덱스 설정
    df = pd.DataFrame([item.dict() for item in result.data])
    if df.empty:
        return result

    # period_q를 datetime으로 변환하고 정렬
    df["period_q"] = pd.to_datetime(df["period_q"], format="%Y%m")
    df = df.sort_values("period_q")

    # 시계열 통계 계산
    time_series_stats = {
        "trend_analysis": {
            # 성장률 계산
            "revenue_growth": {
                "qoq": float(df["rev"].pct_change().iloc[-1] * 100) if len(df) > 1 else None,
                "yoy": float(df["rev"].pct_change(4).iloc[-1] * 100) if len(df) > 4 else None,
                "cagr": float(((df["rev"].iloc[-1] / df["rev"].iloc[0]) ** (4 / len(df)) - 1) * 100)
                if len(df) > 4 and df["rev"].iloc[0] != 0
                else None,
            },
            "operating_income_growth": {
                "qoq": float(df["operating_income"].pct_change().iloc[-1] * 100) if len(df) > 1 else None,
                "yoy": float(df["operating_income"].pct_change(4).iloc[-1] * 100) if len(df) > 4 else None,
            },
        },
        "seasonal_analysis": {
            # 분기별 평균 실적
            "quarterly_average": {
                "Q1": float(df[df["period_q"].dt.quarter == 1]["rev"].mean())
                if not df[df["period_q"].dt.quarter == 1].empty
                else None,
                "Q2": float(df[df["period_q"].dt.quarter == 2]["rev"].mean())
                if not df[df["period_q"].dt.quarter == 2].empty
                else None,
                "Q3": float(df[df["period_q"].dt.quarter == 3]["rev"].mean())
                if not df[df["period_q"].dt.quarter == 3].empty
                else None,
                "Q4": float(df[df["period_q"].dt.quarter == 4]["rev"].mean())
                if not df[df["period_q"].dt.quarter == 4].empty
                else None,
            }
        },
        "rolling_metrics": {
            # 이동평균 (4분기)
            "revenue_ma": float(df["rev"].rolling(window=4).mean().iloc[-1]) if len(df) >= 4 else None,
            "operating_income_ma": float(df["operating_income"].rolling(window=4).mean().iloc[-1])
            if len(df) >= 4
            else None,
        },
        "volatility": {
            # 변동성 지표
            "revenue_std": float(df["rev"].std()),
            "revenue_cv": float(df["rev"].std() / df["rev"].mean()) if df["rev"].mean() != 0 else None,
        },
        "ttm_analysis": {
            # Trailing Twelve Months (최근 4분기 합계)
            "revenue_ttm": float(df["rev"].tail(4).sum()) if len(df) >= 4 else None,
            "operating_income_ttm": float(df["operating_income"].tail(4).sum()) if len(df) >= 4 else None,
            "net_income_ttm": float(df["net_income"].tail(4).sum()) if len(df) >= 4 else None,
        },
        "profitability_trends": {
            # 수익성 지표 추이를 리스트로 변환
            "gross_margin": [float(x) if pd.notnull(x) else None for x in (df["gross_profit"] / df["rev"] * 100)],
            "operating_margin": [float(x) if pd.notnull(x) else None for x in (df["operating_income"] / df["rev"] * 100)],
            "net_margin": [float(x) if pd.notnull(x) else None for x in (df["net_income"] / df["rev"] * 100)],
        },
    }

    # 기본 통계량도 포함
    basic_stats = df.select_dtypes(include=["float64", "int64"]).describe()
    stats_dict = {
        col: {index: float(value) if pd.notnull(value) else None for index, value in series.items()}
        for col, series in basic_stats.items()
    }

    result.statistics = {"basic_statistics": stats_dict, "time_series_analysis": time_series_stats}

    return result
