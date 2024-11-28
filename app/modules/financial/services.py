import pandas as pd
from app.core.logging.config import get_logger
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict, List, Tuple
from fastapi import HTTPException, Depends
import math

from app.database.crud import database
from app.modules.common.enum import Country
from app.modules.common.services import CommonService, get_common_service
from app.modules.financial.schemas import (
    FinPosDetail,
    IncomePerformanceResponse,
    IncomeStatement,
    IncomeStatementDetail,
    CashFlowDetail,
)
from app.modules.common.schemas import BaseResponse, PandasStatistics
from app.core.exception.custom import DataNotFoundException, InvalidCountryException, AnalysisException

logger = get_logger(__name__)


class FinancialService:
    def __init__(self, common_service: CommonService):
        self.db = database
        self.common_service = common_service
        self._setup_tables()

    def _setup_tables(self):
        """
        테이블 설정
        """
        self.income_tables = {Country.KR: "KOR_income", Country.US: "USA_income"}
        self.cashflow_tables = {Country.KR: "KOR_cashflow", Country.US: "USA_cashflow"}
        self.finpos_tables = {Country.KR: "KOR_finpos", Country.US: "USA_finpos"}

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
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> BaseResponse[List[IncomeStatementDetail]]:
        """손익계산서 데이터 조회"""
        try:
            table_name = self.income_tables.get(ctry)
            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException()

            conditions = {"Code": ticker, **self._get_date_conditions(start_date, end_date)}

            logger.debug(f"Querying income data for {ticker} with conditions: {conditions}")
            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                logger.warning(f"No income data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="손익계산")

            statements = self._process_income_statement_result(result)

            logger.info(f"Successfully retrieved income data for {ticker}")
            return BaseResponse[List[IncomeStatementDetail]](
                status_code=200, message="손익계산서 데이터를 성공적으로 조회했습니다.", data=statements
            )

        except (InvalidCountryException, DataNotFoundException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_income_data: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="손익계산서 조회", detail=str(e))

    async def get_income_performance_data(
        self,
        ctry: Country,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> BaseResponse[IncomePerformanceResponse]:
        """
        실적 데이터 조회
        """
        try:
            table_name = self.income_tables.get(ctry)
            if not table_name:
                raise HTTPException(status_code=400, detail="존재하지 않는 국가입니다.")

            conditions = {"Code": ticker, **self._get_date_conditions(start_date, end_date)}

            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                raise HTTPException(status_code=404, detail=f"{ticker} 종목에 대한 손익계산 데이터가 존재하지 않습니다.")

            quarterly_data, yearly_data = self._process_income_performance_statement_result(result)
            performance_data = IncomePerformanceResponse(quarterly=quarterly_data, yearly=yearly_data)

            return BaseResponse[IncomePerformanceResponse](
                status_code=200, message="실적 데이터를 성공적으로 조회했습니다.", data=performance_data
            )

        except HTTPException as http_error:
            raise http_error
        except Exception as e:
            logger.error(f"Unexpected error in get_income_data: {str(e)}")
            raise HTTPException(status_code=500, detail="내부 서버 오류")

    def _process_income_performance_statement_result(self, result) -> Tuple[List[IncomeStatement], List[IncomeStatement]]:
        """
        실적 결과 처리 - 분기별 및 연도별 데이터를 분리하여 처리
        """
        all_columns = [
            "Code",
            "Name",
            "period_q",
            "rev",
            "cost_of_sales",
            "gross_profit",
            "sell_admin_cost",
            "rnd_expense",
            "operating_income",
            "other_rev_gains",
            "other_exp_losses",
            "equity method gain",
            "fin_profit",
            "fin_cost",
            "pbt",
            "corp_tax_cost",
            "profit_continuing_ops",
            "net_income_total",
            "net_income",
            "net_income_not_control",
        ]

        df = pd.DataFrame(result, columns=all_columns)

        required_columns = [
            "Code",
            "Name",
            "period_q",
            "rev",
            "gross_profit",
            "operating_income",
            "net_income",
            "net_income_not_control",
            "net_income_total",
        ]
        df = df[required_columns]

        df["year"] = df["period_q"].astype(str).str[:4]

        yearly_sum = (
            df.groupby(["Code", "Name", "year"])
            .agg(
                {
                    "rev": "sum",
                    "gross_profit": "sum",
                    "operating_income": "sum",
                    "net_income": "sum",
                    "net_income_not_control": "sum",
                    "net_income_total": "sum",
                }
            )
            .reset_index()
        )

        yearly_sum["period_q"] = yearly_sum["year"] + "00"
        yearly_sum = yearly_sum.drop("year", axis=1)

        quarterly_statements = []
        for _, row in df.iterrows():
            quarterly_statements.append(self._create_comprehensive_income_statement(row))

        yearly_statements = []
        for _, row in yearly_sum.iterrows():
            yearly_statements.append(self._create_comprehensive_income_statement(row))

        quarterly_statements.sort(key=lambda x: x.period_q, reverse=True)
        yearly_statements.sort(key=lambda x: x.period_q, reverse=True)

        return quarterly_statements, yearly_statements

    def _process_income_statement_result(self, result) -> List[IncomeStatementDetail]:
        """
        손익계산 결과 처리
        """
        columns = [
            "Code",
            "Name",
            "period_q",
            "rev",
            "cost_of_sales",
            "gross_profit",
            "sell_admin_cost",
            "rnd_expense",
            "operating_income",
            "other_rev_gains",
            "other_exp_losses",
            "equity method gain",
            "fin_profit",
            "fin_cost",
            "pbt",
            "corp_tax_cost",
            "profit_continuing_ops",
            "net_income_total",
            "net_income",
            "net_income_not_control",
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
            net_income_not_control=self._to_decimal(row_dict["net_income_not_control"]),
        )

    def _create_comprehensive_income_statement(self, row_dict: Dict) -> IncomeStatement:
        """
        모든 실적 정보를 포함하는 통합 Statement 생성
        """

        def safe_get_value(key: str) -> Decimal:
            try:
                return self._to_decimal(row_dict[key])
            except Exception as e:
                logger.warning(f"Error converting {key}: {str(e)}")
                return Decimal("0")

        return IncomeStatement(
            code=row_dict["Code"],
            name=row_dict["Name"],
            period_q=row_dict["period_q"],
            rev=safe_get_value("rev"),
            gross_profit=safe_get_value("gross_profit"),
            operating_income=safe_get_value("operating_income"),
            net_income=safe_get_value("net_income"),
            net_income_not_control=safe_get_value("net_income_not_control"),
            net_income_total=safe_get_value("net_income_total"),
            is_yearly=False,
        )

    async def get_cashflow_data(
        self,
        ctry: Country,
        ticker: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> BaseResponse[List[CashFlowDetail]]:
        """
        국가별 현금흐름 데이터 조회
        """
        try:
            table_name = self.cashflow_tables.get(ctry)
            if not table_name:
                raise HTTPException(status_code=400, detail="존재하지 않는 국가입니다.")

            conditions = {"Code": ticker, **self._get_date_conditions(start_date, end_date)}

            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                raise HTTPException(status_code=404, detail=f"{ticker} 종목에 대한 현금흐름 데이터가 존재하지 않습니다.")

            statements = self._process_cashflow_result(result)

            return BaseResponse[List[CashFlowDetail]](
                status_code=200, message="현금흐름표 데이터를 성공적으로 조회했습니다.", data=statements
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
            "Code",
            "Name",
            "period_q",
            "operating_cashflow",
            "non_controlling_changes",
            "working_capital_changes",
            "finance_cashflow",
            "dividends",
            "investing_cashflow",
            "depreciation",
            "free_cash_flow1",
            "free_cash_flow2",
            "cash_earnings",
            "capex",
            "other_cash_flows",
            "cash_increment",
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
            cash_increment=self._to_decimal(row_dict["cash_increment"]),
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
                table=table_name, columns=["period_q"], order="period_q", ascending=False, limit=1, Code=ticker
            )

            if not result:
                raise HTTPException(status_code=404, detail=f"No data found for {ticker}")

            return result[0][0]

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting latest quarter: {e}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

    async def get_finpos_data(
        self, ctry: Country, ticker: str, start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> BaseResponse[List[FinPosDetail]]:
        """
        재무상태표 데이터 조회
        """
        try:
            table_name = self.finpos_tables.get(ctry)
            if not table_name:
                raise HTTPException(status_code=400, detail="존재하지 않는 국가입니다.")

            conditions = {"Code": ticker, **self._get_date_conditions(start_date, end_date)}

            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                raise HTTPException(
                    status_code=404, detail=f"{ticker} 종목에 대한 재무상태표 데이터가 존재하지 않습니다."
                )

            statements = self._process_finpos_result(result)

            return BaseResponse[List[FinPosDetail]](
                status_code=200, message="재무상태표 데이터를 성공적으로 조회했습니다.", data=statements
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
            "Code",
            "Name",
            "period_q",
            "total_asset",
            "current_asset",
            "stock_asset",
            "trade_and_other_receivables",
            "cash_asset",
            "assets_held_for_sale",
            "non_current_asset",
            "tangible_asset",
            "intangible_asset",
            "investment_asset",
            "non_current_trade_and_other_receivables",
            "deferred_tax_asset",
            "extra_intangible",
            "total_dept",
            "current_dept",
            "trade_and_other_payables",
            "liabilities_held_for_sale",
            "non_current_liability",
            "debenture",
            "non_current_trade_and_other_payables",
            "deferred_tax_liability",
            "equity",
            "total_equity",
            "controlling_equity",
            "capital",
            "preferred_cap_stock",
            "cap_stock_common",
            "new_cap_security",
            "capital_surplus",
            "other_capital",
            "comp_income",
            "retained_earnings",
            "non_ctrl_shrhld_eq",
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
            non_ctrl_shrhld_eq=self._to_decimal(row_dict["non_ctrl_shrhld_eq"]),
        )

    def _to_decimal(self, value) -> Decimal:
        """
        값을 Decimal로 변환하고 JSON 직렬화 가능한 값으로 처리
        """
        try:
            if value is None or (isinstance(value, str) and not value.strip()):
                return Decimal("0")
            if isinstance(value, (float, Decimal)):
                if isinstance(value, float) and math.isnan(value):
                    return Decimal("0")
                if isinstance(value, Decimal) and value.is_nan():
                    return Decimal("0")
                if isinstance(value, float) and math.isinf(value):
                    return Decimal("0")
                if isinstance(value, Decimal) and value.is_infinite():
                    return Decimal("0")

            return Decimal(str(value))

        except (ValueError, TypeError, InvalidOperation):
            logger.warning(f"Failed to convert value to Decimal: {value}")
            return Decimal("0")

    ############################손익계산서 시계열 분석 pandas##############################
    async def get_income_analysis(
        self,
        ctry: Country,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> PandasStatistics[List[IncomeStatementDetail]]:
        """손익계산서 시계열 분석"""
        logger.info(f"Starting income analysis for {ticker}")

        try:
            result = PandasStatistics(status_code=200, message="Success", data=[], statistics={})
            income_data = await self.get_income_data(ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date)

            result.data = income_data.data
            df = self._create_income_dataframe(result.data)

            if df.empty:
                logger.warning(f"Empty DataFrame for ticker: {ticker}")
                return result

            result.statistics = self._calculate_income_statistics(df)
            logger.info(f"Successfully completed income analysis for {ticker}")
            return result

        except Exception as e:
            logger.error(f"Error during income analysis for {ticker}: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="손익계산서 시계열", detail=str(e))

    def _create_income_dataframe(self, data: List[IncomeStatementDetail]) -> pd.DataFrame:
        """
        손익계산서 데이터를 DataFrame으로 변환
        """
        df = pd.DataFrame([item.dict() for item in data])
        if df.empty:
            return df

        df["period_q"] = pd.to_datetime(df["period_q"], format="%Y%m")
        return df.sort_values("period_q")

    def _calculate_income_statistics(self, df: pd.DataFrame) -> Dict:
        """
        손익계산서 통계 계산
        """
        time_series_stats = {
            "trend_analysis": self._calculate_trend_analysis(df),
            "seasonal_analysis": self._calculate_seasonal_analysis(df),
            "rolling_metrics": self._calculate_rolling_metrics(df),
            "volatility": self._calculate_volatility_metrics(df),
            "ttm_analysis": self._calculate_ttm_analysis(df),
            "profitability_trends": self._calculate_profitability_trends(df),
        }

        basic_stats = self._calculate_basic_statistics(df)

        return {"basic_statistics": basic_stats, "time_series_analysis": time_series_stats}

    def _calculate_trend_analysis(self, df: pd.DataFrame) -> Dict:
        """
        추세 분석 (성장률 계산)
        """
        return {
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
        }

    def _calculate_seasonal_analysis(self, df: pd.DataFrame) -> Dict:
        """
        계절성 분석 (분기별 평균)
        """
        return {
            "quarterly_average": {
                f"Q{q}": float(df[df["period_q"].dt.quarter == q]["rev"].mean())
                if not df[df["period_q"].dt.quarter == q].empty
                else None
                for q in range(1, 5)
            }
        }

    def _calculate_rolling_metrics(self, df: pd.DataFrame) -> Dict:
        """
        이동평균 지표 계산
        """
        return {
            "revenue_ma": float(df["rev"].rolling(window=4).mean().iloc[-1]) if len(df) >= 4 else None,
            "operating_income_ma": float(df["operating_income"].rolling(window=4).mean().iloc[-1])
            if len(df) >= 4
            else None,
        }

    def _calculate_volatility_metrics(self, df: pd.DataFrame) -> Dict:
        """
        변동성 지표 계산
        """
        return {
            "revenue_std": float(df["rev"].std()),
            "revenue_cv": float(df["rev"].std() / df["rev"].mean()) if df["rev"].mean() != 0 else None,
        }

    def _calculate_ttm_analysis(self, df: pd.DataFrame) -> Dict:
        """
        TTM(Trailing Twelve Months) 분석
        """
        return {
            "revenue_ttm": float(df["rev"].tail(4).sum()) if len(df) >= 4 else None,
            "operating_income_ttm": float(df["operating_income"].tail(4).sum()) if len(df) >= 4 else None,
            "net_income_ttm": float(df["net_income"].tail(4).sum()) if len(df) >= 4 else None,
        }

    def _calculate_profitability_trends(self, df: pd.DataFrame) -> Dict:
        """
        수익성 표 추이 계산
        """
        return {
            "gross_margin": [float(x) if pd.notnull(x) else None for x in (df["gross_profit"] / df["rev"] * 100)],
            "operating_margin": [float(x) if pd.notnull(x) else None for x in (df["operating_income"] / df["rev"] * 100)],
            "net_margin": [float(x) if pd.notnull(x) else None for x in (df["net_income"] / df["rev"] * 100)],
        }

    def _calculate_basic_statistics(self, df: pd.DataFrame) -> Dict:
        """
        기본 통계량 계산
        """
        basic_stats = df.select_dtypes(include=["float64", "int64"]).describe()
        return {
            col: {index: float(value) if pd.notnull(value) else None for index, value in series.items()}
            for col, series in basic_stats.items()
        }

    ############################현금흐름 시계열 분석 pandas##############################
    async def get_cashflow_analysis(
        self,
        ctry: Country,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> PandasStatistics[List[CashFlowDetail]]:
        """
        현금흐름 시계열 분석
        """
        try:
            result = PandasStatistics(status_code=200, message="Success", data=[], statistics={})

            cashflow_data = await self.get_cashflow_data(
                ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date
            )

            result.data = cashflow_data.data
            df = self._create_cashflow_dataframe(result.data)
            if df.empty:
                return result

            result.statistics = self._calculate_cashflow_statistics(df)
            return result

        except Exception as e:
            logger.error(f"Unexpected error in get_cashflow_timeseries_analysis: {str(e)}")
            raise HTTPException(status_code=500, detail="내부 서버 오류")

    def _create_cashflow_dataframe(self, data: List[CashFlowDetail]) -> pd.DataFrame:
        """
        현금흐름 데이터를 DataFrame으로 변환
        """
        df = pd.DataFrame([item.dict() for item in data])
        if df.empty:
            return df

        df["period_q"] = pd.to_datetime(df["period_q"], format="%Y%m")
        return df.sort_values("period_q")

    def _calculate_cashflow_statistics(self, df: pd.DataFrame) -> Dict:
        """
        현금흐름 통계 계산
        """
        time_series_stats = {
            "trend_analysis": self._calculate_cashflow_trend_analysis(df),
            "seasonal_analysis": self._calculate_cashflow_seasonal_analysis(df),
            "rolling_metrics": self._calculate_cashflow_rolling_metrics(df),
            "volatility": self._calculate_cashflow_volatility_metrics(df),
            "ttm_analysis": self._calculate_cashflow_ttm_analysis(df),
            "efficiency_metrics": self._calculate_cashflow_efficiency_metrics(df),
        }

        basic_stats = self._calculate_basic_statistics(df)

        return {"basic_statistics": basic_stats, "time_series_analysis": time_series_stats}

    def _calculate_cashflow_trend_analysis(self, df: pd.DataFrame) -> Dict:
        """
        현금흐름 추세 분석
        """
        return {
            "operating_cashflow_growth": {
                "qoq": float(df["operating_cashflow"].pct_change().iloc[-1] * 100) if len(df) > 1 else None,
                "yoy": float(df["operating_cashflow"].pct_change(4).iloc[-1] * 100) if len(df) > 4 else None,
                "cagr": float(
                    ((df["operating_cashflow"].iloc[-1] / df["operating_cashflow"].iloc[0]) ** (4 / len(df)) - 1) * 100
                )
                if len(df) > 4 and df["operating_cashflow"].iloc[0] != 0
                else None,
            },
            "free_cashflow_growth": {
                "qoq": float(df["free_cash_flow1"].pct_change().iloc[-1] * 100) if len(df) > 1 else None,
                "yoy": float(df["free_cash_flow1"].pct_change(4).iloc[-1] * 100) if len(df) > 4 else None,
            },
        }

    def _calculate_cashflow_seasonal_analysis(self, df: pd.DataFrame) -> Dict:
        """
        현금흐름 계절성 분석
        """
        return {
            "quarterly_average": {
                f"Q{q}": {
                    "operating_cashflow": float(df[df["period_q"].dt.quarter == q]["operating_cashflow"].mean())
                    if not df[df["period_q"].dt.quarter == q].empty
                    else None,
                    "free_cash_flow": float(df[df["period_q"].dt.quarter == q]["free_cash_flow1"].mean())
                    if not df[df["period_q"].dt.quarter == q].empty
                    else None,
                }
                for q in range(1, 5)
            }
        }

    def _calculate_cashflow_rolling_metrics(self, df: pd.DataFrame) -> Dict:
        """
        현금흐름 이동평균 지표
        """
        return {
            "operating_cashflow_ma": float(df["operating_cashflow"].rolling(window=4).mean().iloc[-1])
            if len(df) >= 4
            else None,
            "free_cash_flow_ma": float(df["free_cash_flow1"].rolling(window=4).mean().iloc[-1]) if len(df) >= 4 else None,
            "investing_cashflow_ma": float(df["investing_cashflow"].rolling(window=4).mean().iloc[-1])
            if len(df) >= 4
            else None,
            "finance_cashflow_ma": float(df["finance_cashflow"].rolling(window=4).mean().iloc[-1])
            if len(df) >= 4
            else None,
        }

    def _calculate_cashflow_volatility_metrics(self, df: pd.DataFrame) -> Dict:
        """
        현금흐름 변동성 지표
        """
        return {
            "operating_cashflow_std": float(df["operating_cashflow"].std()),
            "operating_cashflow_cv": float(df["operating_cashflow"].std() / df["operating_cashflow"].mean())
            if df["operating_cashflow"].mean() != 0
            else None,
            "free_cash_flow_std": float(df["free_cash_flow1"].std()),
            "free_cash_flow_cv": float(df["free_cash_flow1"].std() / df["free_cash_flow1"].mean())
            if df["free_cash_flow1"].mean() != 0
            else None,
        }

    def _calculate_cashflow_ttm_analysis(self, df: pd.DataFrame) -> Dict:
        """
        현금흐름 TTM 분석
        """
        return {
            "operating_cashflow_ttm": float(df["operating_cashflow"].tail(4).sum()) if len(df) >= 4 else None,
            "free_cash_flow_ttm": float(df["free_cash_flow1"].tail(4).sum()) if len(df) >= 4 else None,
            "investing_cashflow_ttm": float(df["investing_cashflow"].tail(4).sum()) if len(df) >= 4 else None,
            "finance_cashflow_ttm": float(df["finance_cashflow"].tail(4).sum()) if len(df) >= 4 else None,
            "capex_ttm": float(df["capex"].tail(4).sum()) if len(df) >= 4 else None,
        }

    def _calculate_cashflow_efficiency_metrics(self, df: pd.DataFrame) -> Dict:
        """
        현금흐름 효율성 지표
        """
        return {
            "operating_to_investing_ratio": [
                float(x) if pd.notnull(x) and y != 0 else None
                for x, y in zip(df["operating_cashflow"], df["investing_cashflow"].abs())
            ],
            "capex_to_operating_ratio": [
                float(x) if pd.notnull(x) and y != 0 else None
                for x, y in zip(df["capex"].abs(), df["operating_cashflow"])
            ],
            "free_cash_flow_to_operating_ratio": [
                float(x) if pd.notnull(x) and y != 0 else None
                for x, y in zip(df["free_cash_flow1"], df["operating_cashflow"])
            ],
        }

    ############################재무상태표 시계열 분석 pandas##############################
    async def get_finpos_analysis(
        self,
        ctry: Country,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> PandasStatistics[List[FinPosDetail]]:
        """
        재무상태표 시계열 분석
        """
        try:
            result = PandasStatistics(status_code=200, message="Success", data=[], statistics={})

            finpos_data = await self.get_finpos_data(ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date)

            result.data = finpos_data.data
            df = self._create_finpos_dataframe(result.data)
            if df.empty:
                return result

            result.statistics = self._calculate_finpos_statistics(df)
            return result

        except Exception as e:
            logger.error(f"Unexpected error in get_finpos_timeseries_analysis: {str(e)}")
            raise HTTPException(status_code=500, detail="내부 서버 오류")

    def _create_finpos_dataframe(self, data: List[FinPosDetail]) -> pd.DataFrame:
        """
        재무상태표 데이터를 DataFrame으로 변환
        """
        df = pd.DataFrame([item.dict() for item in data])
        if df.empty:
            return df

        df["period_q"] = pd.to_datetime(df["period_q"], format="%Y%m")
        return df.sort_values("period_q")

    def _calculate_finpos_statistics(self, df: pd.DataFrame) -> Dict:
        """
        재무상태표 통계 계산
        """
        time_series_stats = {
            "trend_analysis": self._calculate_finpos_trend_analysis(df),
            "seasonal_analysis": self._calculate_finpos_seasonal_analysis(df),
            "rolling_metrics": self._calculate_finpos_rolling_metrics(df),
            "volatility": self._calculate_finpos_volatility_metrics(df),
            "ttm_analysis": self._calculate_finpos_ttm_analysis(df),
            "financial_ratios": self._calculate_finpos_financial_ratios(df),
        }

        # 재무상태표용 기본 통계로 수정
        basic_stats = {
            "total_asset": {
                "mean": float(df["total_asset"].mean()),
                "median": float(df["total_asset"].median()),
                "std": float(df["total_asset"].std()),
                "min": float(df["total_asset"].min()),
                "max": float(df["total_asset"].max()),
                "latest": float(df["total_asset"].iloc[0]),
            },
            "total_equity": {
                "mean": float(df["total_equity"].mean()),
                "median": float(df["total_equity"].median()),
                "std": float(df["total_equity"].std()),
                "min": float(df["total_equity"].min()),
                "max": float(df["total_equity"].max()),
                "latest": float(df["total_equity"].iloc[0]),
            },
            "total_dept": {
                "mean": float(df["total_dept"].mean()),
                "median": float(df["total_dept"].median()),
                "std": float(df["total_dept"].std()),
                "min": float(df["total_dept"].min()),
                "max": float(df["total_dept"].max()),
                "latest": float(df["total_dept"].iloc[0]),
            },
        }

        return {"basic_statistics": basic_stats, "time_series_analysis": time_series_stats}

    def _calculate_finpos_trend_analysis(self, df: pd.DataFrame) -> Dict:
        """
        재무상태표 추세 분석
        """
        return {
            "total_asset_growth": {
                "qoq": float(df["total_asset"].pct_change().iloc[-1] * 100) if len(df) > 1 else None,
                "yoy": float(df["total_asset"].pct_change(4).iloc[-1] * 100) if len(df) > 4 else None,
                "cagr": float(((df["total_asset"].iloc[-1] / df["total_asset"].iloc[0]) ** (4 / len(df)) - 1) * 100)
                if len(df) > 4 and df["total_asset"].iloc[0] != 0
                else None,
            },
            "equity_growth": {
                "qoq": float(df["equity"].pct_change().iloc[-1] * 100) if len(df) > 1 else None,
                "yoy": float(df["equity"].pct_change(4).iloc[-1] * 100) if len(df) > 4 else None,
            },
        }

    def _calculate_finpos_seasonal_analysis(self, df: pd.DataFrame) -> Dict:
        """
        재무상태표 계절성 분석
        """
        return {
            "quarterly_average": {
                f"Q{q}": {
                    "total_asset": float(df[df["period_q"].dt.quarter == q]["total_asset"].mean())
                    if not df[df["period_q"].dt.quarter == q].empty
                    else None,
                    "total_equity": float(df[df["period_q"].dt.quarter == q]["total_equity"].mean())
                    if not df[df["period_q"].dt.quarter == q].empty
                    else None,
                }
                for q in range(1, 5)
            }
        }

    def _calculate_finpos_rolling_metrics(self, df: pd.DataFrame) -> Dict:
        """
        재무상태표 이동평균 지표
        """
        return {
            "total_asset_ma": float(df["total_asset"].rolling(window=4).mean().iloc[-1]) if len(df) >= 4 else None,
            "total_equity_ma": float(df["total_equity"].rolling(window=4).mean().iloc[-1]) if len(df) >= 4 else None,
            "current_asset_ma": float(df["current_asset"].rolling(window=4).mean().iloc[-1]) if len(df) >= 4 else None,
            "current_dept_ma": float(df["current_dept"].rolling(window=4).mean().iloc[-1]) if len(df) >= 4 else None,
        }

    def _calculate_finpos_volatility_metrics(self, df: pd.DataFrame) -> Dict:
        """
        재무상태표 변동성 지표
        """
        return {
            "total_asset_std": float(df["total_asset"].std()),
            "total_equity_std": float(df["total_equity"].std()),
            "current_asset_std": float(df["current_asset"].std()),
            "current_dept_std": float(df["current_dept"].std()),
        }

    def _calculate_finpos_ttm_analysis(self, df: pd.DataFrame) -> Dict:
        """
        재무상태표 TTM 분석
        """
        return {
            "total_asset_ttm": float(df["total_asset"].tail(4).sum()) if len(df) >= 4 else None,
            "total_equity_ttm": float(df["total_equity"].tail(4).sum()) if len(df) >= 4 else None,
            "current_asset_ttm": float(df["current_asset"].tail(4).sum()) if len(df) >= 4 else None,
            "current_dept_ttm": float(df["current_dept"].tail(4).sum()) if len(df) >= 4 else None,
        }

    def _calculate_finpos_financial_ratios(self, df: pd.DataFrame) -> Dict:
        """
        재무상태표 재무비율 계산
        - 유동비율 (current ratio)
        - 당좌비율 (quick ratio)
        - 현금비율 (cash ratio)
        - 부채비율 (debt to equity ratio)
        - 자기자본비율 (equity ratio)
        """
        return {
            # 유동비율 = 유동자산 / 유동부채 × 100
            "current_ratio": [
                float(x) if pd.notnull(x) and y != 0 else None for x, y in zip(df["current_asset"], df["current_dept"])
            ],
            # 당좌비율 = (유동자산 - 재고자산) / 유동부채 × 100
            "quick_ratio": [
                float(x) if pd.notnull(x) and y != 0 else None
                for x, y in zip(df["current_asset"] - df["stock_asset"], df["current_dept"])
            ],
            # 현금비율 = 현금성자산 / 유동부채 × 100
            "cash_ratio": [
                float(x) if pd.notnull(x) and y != 0 else None for x, y in zip(df["cash_asset"], df["current_dept"])
            ],
            # 부채비율 = 총부채 / 자기자본 × 100
            "debt_to_equity_ratio": [
                float(x) if pd.notnull(x) and y != 0 else None for x, y in zip(df["total_dept"], df["total_equity"])
            ],
            # 자기자본비율 = 자기자본 / 총자산 × 100
            "equity_ratio": [
                float(x) if pd.notnull(x) and y != 0 else None for x, y in zip(df["total_equity"], df["total_asset"])
            ],
        }


def get_financial_service(common_service: CommonService = Depends(get_common_service)) -> FinancialService:
    return FinancialService(common_service=common_service)
