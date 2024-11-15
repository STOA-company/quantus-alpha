from decimal import Decimal
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

# class FinancialDataResponse(BaseModel):
#     data: List[Dict[str, Any]] = Field(
#         ...,
#         example=[
#             {"date": "2023-01-01", "revenue": 1000000, "expenses": 800000},
#             {"date": "2023-02-01", "revenue": 1100000, "expenses": 850000}
#         ]
#     )

class FinancialDataResponse(BaseModel):
    data: List[Dict[str, Any]] = Field(
        ...,
        example=[{
            "code": "005930",
            "name": "삼성전자",
            "period": "2023Q4",
            "revenue": 1000000000000,
            "operating_income": 100000000000,
            "net_income": 80000000000,
            "gross_profit": 300000000000,
            "operating_margin": 10.0,
            "net_margin": 8.0,
            "rnd_ratio": 8.5,
            "yoy_revenue_growth": 5.2,
            "yoy_operating_income_growth": 7.1,
            "yoy_net_income_growth": 6.8,
            "currency": "KRW"
        }]
    )
    
class IncomeStatementDetail(BaseModel):
    period: str
    revenue: Decimal
    costOfSales: Decimal
    grossProfit: Decimal
    sellAdminCost: Decimal
    rndExpense: Decimal
    operatingIncome: Decimal
    otherRevGains: Decimal
    otherExpLosses: Decimal
    equityMethodGain: Decimal
    finProfit: Decimal
    finCost: Decimal
    pbt: Decimal
    corpTaxCost: Decimal
    profitContinuingOps: Decimal
    netIncomeTotal: Decimal
    netIncome: Decimal
    netIncomeNotControl: Decimal

class IncomeStatementResponse(BaseModel):
    code: str
    name: str
    statements: List[IncomeStatementDetail]

class CashFlowDetail(BaseModel):
    period: str
    operatingCashflow: Decimal
    nonControllingChanges: Decimal
    workingCapitalChanges: Decimal
    financeCashflow: Decimal
    dividends: Decimal
    investingCashflow: Decimal
    depreciation: Decimal
    freeCashFlow1: Decimal
    freeCashFlow2: Decimal
    cashEarnings: Decimal
    capex: Decimal
    otherCashFlows: Decimal
    cashIncrement: Decimal

class CashFlowResponse(BaseModel):
    code: str
    name: str
    statements: List[CashFlowDetail]