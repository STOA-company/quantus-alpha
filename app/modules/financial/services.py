from typing import Optional, Dict, List, Any
from datetime import date
from app.modules.common.services import CommonService, get_common_service
from app.modules.financial.schemas import (
    IncomeStatementResponse, 
    IncomeStatementDetail,
    CashFlowResponse,
    CashFlowDetail
)
from fastapi import HTTPException, Depends
import logging
from app.database.crud import database
from app.modules.common.enum import Country
from decimal import Decimal

logger = logging.getLogger(__name__)

class FinancialService:
    def __init__(self, common_service: CommonService):
        self.db = database
        self.common_service = common_service
        self.income_tables = {
            Country.KR: "KOR_income",
            Country.US: "USA_income"
        }
        self.cashflow_tables = {
            Country.KR: "KOR_cashflow",
            Country.US: "USA_cashflow"
        }

    def _convert_row_to_dict(self, row, ctry: Country) -> Dict[str, Any]:
        """
        SQLAlchemy Row를 딕셔너리로 변환
        """
        try:
            return {
                'code': str(row.code),
                'name': str(row.name),
                'period': str(row.period_q),
                'revenue': float(row.rev or 0) * (1_000_000 if ctry == Country.US else 1_000_000),
                'operating_income': float(row.operating_income or 0) * (1_000_000 if ctry == Country.US else 1_000_000),
                'net_income': float(row.net_income or 0) * (1_000_000 if ctry == Country.US else 1_000_000),
                'gross_profit': float(row.gross_profit or 0) * (1_000_000 if ctry == Country.US else 1_000_000),
                'operating_margin': round(float(row.operating_income or 0) / float(row.rev or 1) * 100, 2),
                'net_margin': round(float(row.net_income or 0) / float(row.rev or 1) * 100, 2),
                'rnd_ratio': round(float(row.rnd_expense or 0) / float(row.rev or 1) * 100, 2),
                'currency': 'USD' if ctry == Country.US else 'KRW'
            }
        except Exception as e:
            logger.error(f"Error converting row: {e}")
            logger.debug(f"Row data: {row}")
            raise

    # 재무제표 데이터 전체 조회
    async def read_financial_data(
        self, 
        ctry: Country, 
        ticker: str, 
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> IncomeStatementResponse:
        """
        국가별 재무제표 데이터를 조회하고 반환합니다.
        
        TODO) Figma 디자인 비교 후 필요한 response로 변경하는 작업 필요, 추후 성능개선 작업 예정
        """
        try:
            table_name = self.income_tables.get(ctry)
            if not table_name:
                raise HTTPException(status_code=400, detail="Invalid country code")

            conditions = {
                "Code": ticker
            }
            
            if start_date:
                conditions["period_q__gte"] = start_date.strftime("%Y")
            if end_date:
                conditions["period_q__lte"] = end_date.strftime("%Y")

            result = self.db._select(
                table=table_name,
                order='period_q',
                ascending=False,
                **conditions
            )

            if not result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No data found for ticker {ticker} in {ctry.value.upper()}"
                )

            columns = ['Code', 'Name', 'period_q', 'rev', 'cost_of_sales', 'gross_profit', 
                      'sell_admin_cost', 'rnd_expense', 'operating_income', 'other_rev_gains', 
                      'other_exp_losses', 'equity method gain', 'fin_profit', 'fin_cost', 
                      'pbt', 'corp_tax_cost', 'profit_continuing_ops', 'net_income_total', 
                      'net_income', 'net_income_not_control']
            
            statements = []
            first_row = None
            
            for row in result:
                row_dict = dict(zip(columns, row))
                if not first_row:
                    first_row = row_dict
                
                statements.append(IncomeStatementDetail(
                    period=row_dict["period_q"],
                    revenue=Decimal(str(row_dict["rev"] or 0)),
                    costOfSales=Decimal(str(row_dict["cost_of_sales"] or 0)),
                    grossProfit=Decimal(str(row_dict["gross_profit"] or 0)),
                    sellAdminCost=Decimal(str(row_dict["sell_admin_cost"] or 0)),
                    rndExpense=Decimal(str(row_dict["rnd_expense"] or 0)),
                    operatingIncome=Decimal(str(row_dict["operating_income"] or 0)),
                    otherRevGains=Decimal(str(row_dict["other_rev_gains"] or 0)),
                    otherExpLosses=Decimal(str(row_dict["other_exp_losses"] or 0)),
                    equityMethodGain=Decimal(str(row_dict["equity method gain"] or 0)),
                    finProfit=Decimal(str(row_dict["fin_profit"] or 0)),
                    finCost=Decimal(str(row_dict["fin_cost"] or 0)),
                    pbt=Decimal(str(row_dict["pbt"] or 0)),
                    corpTaxCost=Decimal(str(row_dict["corp_tax_cost"] or 0)),
                    profitContinuingOps=Decimal(str(row_dict["profit_continuing_ops"] or 0)),
                    netIncomeTotal=Decimal(str(row_dict["net_income_total"] or 0)),
                    netIncome=Decimal(str(row_dict["net_income"] or 0)),
                    netIncomeNotControl=Decimal(str(row_dict["net_income_not_control"] or 0))
                ))

            return IncomeStatementResponse(
                code=first_row["Code"],
                name=first_row["Name"],
                statements=statements
            )

        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def _get_latest_quarter(self, ctry: Country, ticker: str) -> str:
        """
        가장 최근 분기 데이터 조회
        """
        try:
            table_name = self.income_tables.get(ctry)
            if not table_name:
                raise HTTPException(status_code=400, detail="Invalid country code")

            result = self.db._select(
                table=table_name,
                columns=['period_q'],
                order='period_q',
                ascending=False,
                limit=1,
                code=ticker
            )
            
            if not result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No data found for {ticker}"
                )
                
            return result[0][0]
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting latest quarter: {e}")
            raise HTTPException(
                status_code=500, 
                detail=f"Internal server error: {str(e)}"
            )

    # 현금흐름 데이터 전체 조회
    async def get_cashflow_data(
        self, 
        ctry: Country, 
        ticker: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> CashFlowResponse:
        """
        국가별 현금흐름 데이터를 조회하고 반환합니다.
        
        TODO) Figma 디자인 비교 후 필요한 response로 변경하는 작업 필요, 추후 성능개선 작업 예정
        """
        try:
            table_name = self.cashflow_tables.get(ctry)
            if not table_name:
                raise HTTPException(status_code=400, detail="Invalid country code")

            conditions = {
                "Code": ticker
            }
            
            if start_date:
                conditions["period_q__gte"] = start_date.strftime("%Y")
            if end_date:
                conditions["period_q__lte"] = end_date.strftime("%Y")

            result = self.db._select(
                table=table_name,
                order='period_q',
                ascending=False,
                **conditions
            )

            if not result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No cashflow data found for ticker {ticker} in {ctry.value.upper()}"
                )

            columns = [
                'Code', 'Name', 'period_q', 
                'operating_cashflow', 'non_controlling_changes', 
                'working_capital_changes', 'finance_cashflow', 
                'dividends', 'investing_cashflow', 'depreciation',
                'free_cash_flow1', 'free_cash_flow2', 'cash_earnings',
                'capex', 'other_cash_flows', 'cash_increment'
            ]

            statements = []
            first_row = None
            
            for row in result:
                row_dict = dict(zip(columns, row))
                if not first_row:
                    first_row = row_dict
                
                statements.append(CashFlowDetail(
                    period=row_dict["period_q"],
                    operatingCashflow=Decimal(str(row_dict["operating_cashflow"] or 0)),
                    nonControllingChanges=Decimal(str(row_dict["non_controlling_changes"] or 0)),
                    workingCapitalChanges=Decimal(str(row_dict["working_capital_changes"] or 0)),
                    financeCashflow=Decimal(str(row_dict["finance_cashflow"] or 0)),
                    dividends=Decimal(str(row_dict["dividends"] or 0)),
                    investingCashflow=Decimal(str(row_dict["investing_cashflow"] or 0)),
                    depreciation=Decimal(str(row_dict["depreciation"] or 0)),
                    freeCashFlow1=Decimal(str(row_dict["free_cash_flow1"] or 0)),
                    freeCashFlow2=Decimal(str(row_dict["free_cash_flow2"] or 0)),
                    cashEarnings=Decimal(str(row_dict["cash_earnings"] or 0)),
                    capex=Decimal(str(row_dict["capex"] or 0)),
                    otherCashFlows=Decimal(str(row_dict["other_cash_flows"] or 0)),
                    cashIncrement=Decimal(str(row_dict["cash_increment"] or 0))
                ))

            return CashFlowResponse(
                code=first_row["Code"],
                name=first_row["Name"],
                statements=statements
            )

        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")
            
            
    
    

def get_financial_service(
    common_service: CommonService = Depends(get_common_service)
) -> FinancialService:
    return FinancialService(common_service=common_service)