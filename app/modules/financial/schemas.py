from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

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
    period_q: Optional[str] = Field(default=None, max_length=20)
    rev: Optional[float] = None
    cost_of_sales: Optional[float] = None
    gross_profit: Optional[float] = None
    sell_admin_cost: Optional[float] = None
    rnd_expense: Optional[float] = None
    operating_income: Optional[float] = None
    other_rev_gains: Optional[float] = None
    other_exp_losses: Optional[float] = None
    equity_method_gain: Optional[float] = None
    fin_profit: Optional[float] = None
    fin_cost: Optional[float] = None
    pbt: Optional[float] = None
    corp_tax_cost: Optional[float] = None
    profit_continuing_ops: Optional[float] = None
    net_income_total: Optional[float] = None
    net_income: Optional[float] = None
    net_income_not_control: Optional[float] = None


class IncomeStatementResponse(BaseModel):
    code: str = Field(max_length=20)
    name: str = Field(max_length=100)
    ctry: str = Field(max_length=20)
    ttm: IncomeStatementDetail
    total: List[IncomeStatementDetail]
    details: List[IncomeStatementDetail]


class CashFlowDetail(BaseModel):
    period_q: Optional[str] = Field(default=None, max_length=20)
    operating_cashflow: Optional[float] = None
    non_controlling_changes: Optional[float] = None
    working_capital_changes: Optional[float] = None
    finance_cashflow: Optional[float] = None
    dividends: Optional[float] = None
    investing_cashflow: Optional[float] = None
    depreciation: Optional[float] = None
    free_cash_flow1: Optional[float] = None
    free_cash_flow2: Optional[float] = None
    cash_earnings: Optional[float] = None
    capex: Optional[float] = None
    other_cash_flows: Optional[float] = None
    cash_increment: Optional[float] = None


class CashFlowResponse(BaseModel):
    code: str = Field(max_length=20)
    name: str = Field(max_length=100)
    ctry: str = Field(max_length=20)
    ttm: CashFlowDetail
    total: List[CashFlowDetail]
    details: List[CashFlowDetail]


class FinPosDetail(BaseModel):
    period_q: Optional[str] = Field(max_length=20)
    total_asset: Optional[float] = None
    current_asset: Optional[float] = None
    stock_asset: Optional[float] = None
    trade_and_other_receivables: Optional[float] = None
    cash_asset: Optional[float] = None
    assets_held_for_sale: Optional[float] = None
    non_current_asset: Optional[float] = None
    tangible_asset: Optional[float] = None
    intangible_asset: Optional[float] = None
    investment_asset: Optional[float] = None
    non_current_trade_and_other_receivables: Optional[float] = None
    deferred_tax_asset: Optional[float] = None
    extra_intangible: Optional[float] = None
    total_dept: Optional[float] = None
    current_dept: Optional[float] = None
    trade_and_other_payables: Optional[float] = None
    liabilities_held_for_sale: Optional[float] = None
    non_current_liability: Optional[float] = None
    debenture: Optional[float] = None
    non_current_trade_and_other_payables: Optional[float] = None
    deferred_tax_liability: Optional[float] = None
    equity: Optional[float] = None
    total_equity: Optional[float] = None
    controlling_equity: Optional[float] = None
    capital: Optional[float] = None
    preferred_cap_stock: Optional[float] = None
    cap_stock_common: Optional[float] = None
    new_cap_security: Optional[float] = None
    capital_surplus: Optional[float] = None
    other_capital: Optional[float] = None
    comp_income: Optional[float] = None
    retained_earnings: Optional[float] = None
    non_ctrl_shrhld_eq: Optional[float] = None


class FinPosResponse(BaseModel):
    code: str = Field(max_length=20)
    name: str = Field(max_length=100)
    ctry: str = Field(max_length=20)
    ttm: FinPosDetail
    total: List[FinPosDetail]
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


class DebtRatioResponse(BaseModel):
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
    debt_ratios: DebtRatioResponse
    liquidity_ratios: LiquidityRatioResponse
    interest_coverage_ratios: InterestCoverageRatioResponse
