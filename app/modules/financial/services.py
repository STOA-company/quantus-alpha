from datetime import date
from decimal import Decimal
from typing import Optional, Dict, List, Any
import logging

from fastapi import HTTPException, Depends

from app.database.crud import database
from app.modules.common.enum import Country
from app.modules.common.services import CommonService, get_common_service
from app.modules.financial.schemas import (
    FinPosDetail,
    FinPosResponse,
    IncomeStatementResponse, 
    IncomeStatementDetail,
    CashFlowResponse,
    CashFlowDetail
)
from app.modules.common.schemas import ResponseDTO

logger = logging.getLogger(__name__)

class FinancialService:
    def __init__(self, common_service: CommonService):
        self.db = database
        self.common_service = common_service
        self._setup_tables()

    def _setup_tables(self):
        """
        테이블 설정
        """
        self.income_tables = {
            Country.KR: "KOR_income",
            Country.US: "USA_income"
        }
        self.cashflow_tables = {
            Country.KR: "KOR_cashflow",
            Country.US: "USA_cashflow"
        }
        self.finpos_tables = {
            Country.KR: "KOR_finpos",
            Country.US: "USA_finpos"
        }

    def _get_date_conditions(self, start_date: Optional[str], end_date: Optional[str]) -> Dict:
        """
        날짜 조건 생성
        start_date (Optional[str]): YYYYMM 형식의 시작일
        end_date (Optional[str]): YYYYMM 형식의 종료일
        """
        from datetime import datetime
        
        conditions = {}
        current_year = datetime.now().year
        
        if not start_date:
            conditions["period_q__gte"] = f"{current_year - 3}01"
            conditions["period_q__lte"] = f"{current_year}04"
        else:
            conditions["period_q__gte"] = f"{start_date[:4]}01"
            conditions["period_q__lte"] = f"{end_date[:4]}04" if end_date else f"{current_year}04"
            
        return conditions

    async def get_income_data(
        self, 
        ctry: Country, 
        ticker: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> ResponseDTO[List[IncomeStatementDetail]]:
        """
        손익계산서 데이터 조회
        """
        try:
            table_name = self.income_tables.get(ctry)
            if not table_name:
                raise HTTPException(status_code=400, detail="존재하지 않는 국가입니다.")

            conditions = {"Code": ticker, **self._get_date_conditions(start_date, end_date)}

            result = self.db._select(
                table=table_name,
                order='period_q',
                ascending=False,
                **conditions
            )

            if not result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"{ticker} 종목에 대한 손익계산 데이터가 존재하지 않습니다."
                )

            statements = self._process_income_statement_result(result)
            
            return ResponseDTO[List[IncomeStatementDetail]](
                status="success",
                message="손익계산서 데이터를 성공적으로 조회했습니다.",
                data=statements
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_income_data: {str(e)}")
            raise HTTPException(status_code=500, detail="내부 서버 오류")

    def _process_income_statement_result(self, result) -> List[IncomeStatementDetail]:
        """
        손익계산 결과 처리
        """
        columns = [
            'Code', 'Name', 'period_q', 'rev', 'cost_of_sales', 'gross_profit', 
            'sell_admin_cost', 'rnd_expense', 'operating_income', 'other_rev_gains', 
            'other_exp_losses', 'equity method gain', 'fin_profit', 'fin_cost', 
            'pbt', 'corp_tax_cost', 'profit_continuing_ops', 'net_income_total', 
            'net_income', 'net_income_not_control'
        ]
        
        statements = []
        
        for row in result:
            row_dict = dict(zip(columns, row))
            statements.append(self._create_income_statement_detail(row_dict))

        return statements

    def _create_income_statement_detail(self, row_dict: Dict) -> IncomeStatementDetail:
        """
        손익계산서 상세 정보 생성
        """
        return IncomeStatementDetail(
            code=row_dict["Code"],
            name=row_dict["Name"],
            period_q=row_dict["period_q"],
            rev=self._to_decimal(row_dict["rev"]),
            cost_of_sales=self._to_decimal(row_dict["cost_of_sales"]),
            gross_profit=self._to_decimal(row_dict["gross_profit"]),
            sell_admin_cost=self._to_decimal(row_dict["sell_admin_cost"]),
            rnd_expense=self._to_decimal(row_dict["rnd_expense"]),
            operating_income=self._to_decimal(row_dict["operating_income"]),
            other_rev_gains=self._to_decimal(row_dict["other_rev_gains"]),
            other_exp_losses=self._to_decimal(row_dict["other_exp_losses"]),
            equity_method_gain=self._to_decimal(row_dict["equity method gain"]),
            fin_profit=self._to_decimal(row_dict["fin_profit"]),
            fin_cost=self._to_decimal(row_dict["fin_cost"]),
            pbt=self._to_decimal(row_dict["pbt"]),
            corp_tax_cost=self._to_decimal(row_dict["corp_tax_cost"]),
            profit_continuing_ops=self._to_decimal(row_dict["profit_continuing_ops"]),
            net_income_total=self._to_decimal(row_dict["net_income_total"]),
            net_income=self._to_decimal(row_dict["net_income"]),
            net_income_not_control=self._to_decimal(row_dict["net_income_not_control"])
        )

    async def get_cashflow_data(
        self, 
        ctry: Country, 
        ticker: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> ResponseDTO[List[CashFlowDetail]]:
        """
        국가별 현금흐름 데이터 조회
        """
        try:
            table_name = self.cashflow_tables.get(ctry)
            if not table_name:
                raise HTTPException(status_code=400, detail="존재하지 않는 국가입니다.")

            conditions = {"Code": ticker, **self._get_date_conditions(start_date, end_date)}

            result = self.db._select(
                table=table_name,
                order='period_q',
                ascending=False,
                **conditions
            )

            if not result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"{ticker} 종목에 대한 현금흐름 데이터가 존재하지 않습니다."
                )

            statements = self._process_cashflow_result(result)
            
            return ResponseDTO[List[CashFlowDetail]](
                status="success",
                message="현금흐름표 데이터를 성공적으로 조회했습니다.",
                data=statements
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_cashflow_data: {str(e)}")
            raise HTTPException(status_code=500, detail="내부 서버 오류")

    def _process_cashflow_result(self, result) -> List[CashFlowDetail]:
        """
        현금흐름 결과 처리
        """
        columns = [
            'Code', 'Name', 'period_q', 
            'operating_cashflow', 'non_controlling_changes', 
            'working_capital_changes', 'finance_cashflow', 
            'dividends', 'investing_cashflow', 'depreciation',
            'free_cash_flow1', 'free_cash_flow2', 'cash_earnings',
            'capex', 'other_cash_flows', 'cash_increment'
        ]

        statements = []
        for row in result:
            row_dict = dict(zip(columns, row))
            statements.append(self._create_cashflow_detail(row_dict))

        return statements

    def _create_cashflow_detail(self, row_dict: Dict) -> CashFlowDetail:
        """
        현금흐름 상세 정보 생성
        """
        return CashFlowDetail(
            code=row_dict["Code"],
            name=row_dict["Name"],
            period_q=row_dict["period_q"],
            operating_cashflow=self._to_decimal(row_dict["operating_cashflow"]),
            non_controlling_changes=self._to_decimal(row_dict["non_controlling_changes"]),
            working_capital_changes=self._to_decimal(row_dict["working_capital_changes"]),
            finance_cashflow=self._to_decimal(row_dict["finance_cashflow"]),
            dividends=self._to_decimal(row_dict["dividends"]),
            investing_cashflow=self._to_decimal(row_dict["investing_cashflow"]),
            depreciation=self._to_decimal(row_dict["depreciation"]),
            free_cash_flow1=self._to_decimal(row_dict["free_cash_flow1"]),
            free_cash_flow2=self._to_decimal(row_dict["free_cash_flow2"]),
            cash_earnings=self._to_decimal(row_dict["cash_earnings"]),
            capex=self._to_decimal(row_dict["capex"]),
            other_cash_flows=self._to_decimal(row_dict["other_cash_flows"]),
            cash_increment=self._to_decimal(row_dict["cash_increment"])
        )

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
                Code=ticker
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
            
    async def get_finpos_data(
        self, 
        ctry: Country, 
        ticker: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> ResponseDTO[List[FinPosDetail]]:
        """
        재무상태표 데이터 조회
        """
        try:
            table_name = self.finpos_tables.get(ctry)
            if not table_name:
                raise HTTPException(status_code=400, detail="존재하지 않는 국가입니다.")

            conditions = {"Code": ticker, **self._get_date_conditions(start_date, end_date)}

            result = self.db._select(
                table=table_name,
                order='period_q',
                ascending=False,
                **conditions
            )

            if not result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"{ticker} 종목에 대한 재무상태표 데이터가 존재하지 않습니다."
                )

            statements = self._process_finpos_result(result)
            
            return ResponseDTO[List[FinPosDetail]](
                status="success",
                message="재무상태표 데이터를 성공적으로 조회했습니다.",
                data=statements
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_finpos_data: {str(e)}")
            raise HTTPException(status_code=500, detail="내부 서버 오류")

    def _process_finpos_result(self, result) -> List[FinPosDetail]:
        """
        재무상태표 결과 처리
        """
        columns = [
            'Code', 'Name', 'period_q', 'total_asset', 'current_asset',
            'stock_asset', 'trade_and_other_receivables', 'cash_asset',
            'assets_held_for_sale', 'non_current_asset', 'tangible_asset',
            'intangible_asset', 'investment_asset', 
            'non_current_trade_and_other_receivables', 'deferred_tax_asset',
            'extra_intangible', 'total_dept', 'current_dept',
            'trade_and_other_payables', 'liabilities_held_for_sale',
            'non_current_liability', 'debenture',
            'non_current_trade_and_other_payables', 'deferred_tax_liability',
            'equity', 'total_equity', 'controlling_equity', 'capital',
            'preferred_cap_stock', 'cap_stock_common', 'new_cap_security',
            'capital_surplus', 'other_capital', 'comp_income',
            'retained_earnings', 'non_ctrl_shrhld_eq'
        ]
        
        statements = []
        for row in result:
            row_dict = dict(zip(columns, row))
            statements.append(self._create_finpos_detail(row_dict))

        return statements

    def _create_finpos_detail(self, row_dict: Dict) -> FinPosDetail:
        """
        재무상태표 상세 정보 생성
        """
        return FinPosDetail(
            code=row_dict["Code"],
            name=row_dict["Name"],
            period_q=row_dict["period_q"],
            total_asset=self._to_decimal(row_dict["total_asset"]),
            current_asset=self._to_decimal(row_dict["current_asset"]),
            stock_asset=self._to_decimal(row_dict["stock_asset"]),
            trade_and_other_receivables=self._to_decimal(row_dict["trade_and_other_receivables"]),
            cash_asset=self._to_decimal(row_dict["cash_asset"]),
            assets_held_for_sale=self._to_decimal(row_dict["assets_held_for_sale"]),
            non_current_asset=self._to_decimal(row_dict["non_current_asset"]),
            tangible_asset=self._to_decimal(row_dict["tangible_asset"]),
            intangible_asset=self._to_decimal(row_dict["intangible_asset"]),
            investment_asset=self._to_decimal(row_dict["investment_asset"]),
            non_current_trade_and_other_receivables=self._to_decimal(row_dict["non_current_trade_and_other_receivables"]),
            deferred_tax_asset=self._to_decimal(row_dict["deferred_tax_asset"]),
            extra_intangible=self._to_decimal(row_dict["extra_intangible"]),
            total_dept=self._to_decimal(row_dict["total_dept"]),
            current_dept=self._to_decimal(row_dict["current_dept"]),
            trade_and_other_payables=self._to_decimal(row_dict["trade_and_other_payables"]),
            liabilities_held_for_sale=self._to_decimal(row_dict["liabilities_held_for_sale"]),
            non_current_liability=self._to_decimal(row_dict["non_current_liability"]),
            debenture=self._to_decimal(row_dict["debenture"]),
            non_current_trade_and_other_payables=self._to_decimal(row_dict["non_current_trade_and_other_payables"]),
            deferred_tax_liability=self._to_decimal(row_dict["deferred_tax_liability"]),
            equity=self._to_decimal(row_dict["equity"]),
            total_equity=self._to_decimal(row_dict["total_equity"]),
            controlling_equity=self._to_decimal(row_dict["controlling_equity"]),
            capital=self._to_decimal(row_dict["capital"]),
            preferred_cap_stock=self._to_decimal(row_dict["preferred_cap_stock"]),
            cap_stock_common=self._to_decimal(row_dict["cap_stock_common"]),
            new_cap_security=self._to_decimal(row_dict["new_cap_security"]),
            capital_surplus=self._to_decimal(row_dict["capital_surplus"]),
            other_capital=self._to_decimal(row_dict["other_capital"]),
            comp_income=self._to_decimal(row_dict["comp_income"]),
            retained_earnings=self._to_decimal(row_dict["retained_earnings"]),
            non_ctrl_shrhld_eq=self._to_decimal(row_dict["non_ctrl_shrhld_eq"])
        )

    @staticmethod
    def _to_decimal(value) -> Decimal:
        """
        값을 Decimal로 변환
        """
        return Decimal(str(value or 0))


def get_financial_service(
    common_service: CommonService = Depends(get_common_service)
) -> FinancialService:
    return FinancialService(common_service=common_service)





