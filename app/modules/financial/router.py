from datetime import date
from app.modules.common.enum import Country
from app.modules.common.schemas import ResponseDTO
from fastapi import APIRouter, Depends
from app.modules.financial.services import FinancialService, get_financial_service
from .schemas import CashFlowDetail, CashFlowResponse, FinPosDetail, FinPosResponse, IncomeStatementDetail, IncomeStatementResponse
from typing import List, Optional

router = APIRouter()

# @router.get("/income", response_model=FinancialDataResponse)
# async def get_income_data(
#     ctry: str = Query(..., description="Country code"),
#     ticker: str = Query(..., description="Stock ticker symbol"),
#     service: FinancialService = Depends(get_financial_service)
# ):
#     """
#     Get income statement data for a specific country and ticker.
#     """
#     df = await service.read_financial_data("income", ctry, ticker)
#     return {"data": df.to_dict(orient="records")}

# @router.get("/finpos", response_model=FinancialDataResponse)
# async def get_finpos_data(
#     ctry: str = Query(..., description="Country code"),
#     ticker: str = Query(..., description="Stock ticker symbol"),
#     service: FinancialService = Depends(get_financial_service)
# ):
#     """
#     Get financial position data for a specific country and ticker.
#     """
#     df = await service.read_financial_data("finpos", ctry, ticker)
#     return {"data": df.to_dict(orient="records")}

# @router.get("/cashflow", response_model=FinancialDataResponse)
# async def get_cashflow_data(
#     ctry: str = Query(..., description="Country code"),
#     ticker: str = Query(..., description="Stock ticker symbol"),
#     service: FinancialService = Depends(get_financial_service)
# ):
#     """
#     Get cash flow data for a specific country and ticker.
#     """
#     df = await service.read_financial_data("cashflow", ctry, ticker)
#     return {"data": df.to_dict(orient="records")}

@router.get(
    "/income", 
    response_model=ResponseDTO[List[IncomeStatementDetail]], 
    summary="손익계산서 분기별 조회"
)
async def get_income_data(
    ctry: Country,
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    financial_service: FinancialService = Depends(get_financial_service),
) -> ResponseDTO[List[IncomeStatementDetail]]:
    result = await financial_service.get_income_data(
        ctry=ctry,
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
    )
    return result

@router.get(
    "/cashflow", 
    response_model=ResponseDTO[List[CashFlowDetail]], 
    summary="현금흐름 분기별 조회"
)
async def get_cashflow_data(
    ctry: Country,
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    financial_service: FinancialService = Depends(get_financial_service),
) -> ResponseDTO[List[CashFlowDetail]]:
    result = await financial_service.get_cashflow_data(
        ctry=ctry,
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
    )
    return result

@router.get(
    "/finpos", 
    response_model=ResponseDTO[List[FinPosDetail]], 
    summary="재무제표 분기별 조회"
)
async def get_finpos_data(
    ctry: Country,
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    financial_service: FinancialService = Depends(get_financial_service),
) -> ResponseDTO[List[FinPosDetail]]:
    result = await financial_service.get_finpos_data(
        ctry=ctry,
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
    )
    return result