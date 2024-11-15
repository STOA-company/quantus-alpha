from datetime import date
from typing import Optional, Dict, List, Any
from app.modules.common.enum import Country
from fastapi import APIRouter, Depends, Query
from app.modules.financial.services import FinancialService, get_financial_service
from .schemas import FinancialDataResponse

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

@router.get("", response_model=FinancialDataResponse)
async def get_income_data(
    ctry: Country,
    ticker: str,
    financial_service: FinancialService = Depends(get_financial_service),
) -> FinancialDataResponse:
    result = await financial_service.read_financial_data(
        ctry=ctry,
        ticker=ticker,
    )
    return {"data": result.data}