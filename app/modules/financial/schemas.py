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
    period_q: str = Field(max_length=20)
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
    period_q: str = Field(max_length=20)
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
    
class FinPosDetail(BaseModel):
    period_q: str = Field(max_length=20)
    total_asset: float
    current_asset: float
    stock_asset: float
    trade_and_other_receivables: float
    cash_asset: float
    assets_held_for_sale: float
    non_current_asset: float
    tangible_asset: float
    intangible_asset: float
    investment_asset: float
    non_current_trade_and_other_receivables: float
    deferred_tax_asset: float
    extra_intangible: float
    total_dept: float
    current_dept: float
    trade_and_other_payables: float
    liabilities_held_for_sale: float
    non_current_liability: float
    debenture: float
    non_current_trade_and_other_payables: float
    deferred_tax_liability: float
    equity: float
    total_equity: float
    controlling_equity: float
    capital: float
    preferred_cap_stock: float
    cap_stock_common: float
    new_cap_security: float
    capital_surplus: float
    other_capital: float
    comp_income: float
    retained_earnings: float
    non_ctrl_shrhld_eq: float
    

class FinPosResponse(BaseModel):
    code: str
    name: str
    statements: List[FinPosDetail]