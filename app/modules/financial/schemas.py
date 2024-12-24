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
        example=[
            {
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
                "currency": "KRW",
            }
        ],
    )


class IncomeStatementDetail(BaseModel):
    period_q: Optional[str] = Field(max_length=20)
    rev: float
    cost_of_sales: float
    gross_profit: float
    sell_admin_cost: float
    rnd_expense: float
    operating_income: float
    other_rev_gains: float
    other_exp_losses: float
    equity_method_gain: float
    fin_profit: float
    fin_cost: float
    pbt: float
    corp_tax_cost: float
    profit_continuing_ops: float
    net_income_total: float
    net_income: float
    net_income_not_control: float


class IncomeStatementResponse(BaseModel):
    code: str = Field(max_length=20)
    name: str = Field(max_length=100)
    ctry: str = Field(max_length=20)
    ttm: IncomeStatementDetail
    details: List[IncomeStatementDetail]


class CashFlowDetail(BaseModel):
    period_q: Optional[str] = Field(max_length=20)
    operating_cashflow: float
    non_controlling_changes: float
    working_capital_changes: float
    finance_cashflow: float
    dividends: float
    investing_cashflow: float
    depreciation: float
    free_cash_flow1: float
    free_cash_flow2: float
    cash_earnings: float
    capex: float
    other_cash_flows: float
    cash_increment: float


class CashFlowResponse(BaseModel):
    code: str = Field(max_length=20)
    name: str = Field(max_length=100)
    ctry: str = Field(max_length=20)
    ttm: CashFlowDetail
    details: List[CashFlowDetail]


class FinPosDetail(BaseModel):
    period_q: Optional[str] = Field(max_length=20)
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
    code: str = Field(max_length=20)
    name: str = Field(max_length=100)
    ctry: str = Field(max_length=20)
    ttm: FinPosDetail
    details: List[FinPosDetail]


class IncomeMetric(BaseModel):
    company: Decimal = Field(
        description="해당 기업의 지표 값",
        example=1234.56,
        json_schema_extra={"type": "number", "format": "float", "multipleOf": 0.01},
    )
    industry_avg: Decimal = Field(
        description="해당 업종의 평균 값",
        example=789.12,
        json_schema_extra={"type": "number", "format": "float", "multipleOf": 0.01},
    )

    class Config:
        json_encoders = {Decimal: lambda v: round(float(v), 2)}


class QuarterlyIncome(BaseModel):
    """분기별 실적 데이터"""

    period_q: str = Field(max_length=20)
    rev: IncomeMetric
    operating_income: IncomeMetric
    net_income: IncomeMetric
    eps: IncomeMetric


class IncomePerformanceResponse(BaseModel):
    """실적 응답 스키마"""

    code: str = Field(max_length=20)
    name: str = Field(max_length=100)
    ctry: str = Field(max_length=20)
    sector: Optional[str]
    quarterly: List[QuarterlyIncome]  # 분기별 데이터
    yearly: List[QuarterlyIncome]  # 연간 데이터

    class Config:
        json_encoders = {Decimal: lambda v: str(v)}


class FinancialRatioResponse(BaseModel):
    ratio: float
    industry_avg: Optional[float] = None  # 업종 평균


class LiquidityRatioResponse(BaseModel):
    ratio: float
    industry_avg: Optional[float] = None


class InterestCoverageRatioResponse(BaseModel):
    ratio: float
    industry_avg: Optional[float] = None


class RatioResponse(BaseModel):
    code: str = Field(max_length=20)
    name: str = Field(max_length=100)
    ctry: str = Field(max_length=20)
    financial_ratios: FinancialRatioResponse
    liquidity_ratios: LiquidityRatioResponse
    interest_coverage_ratios: InterestCoverageRatioResponse
