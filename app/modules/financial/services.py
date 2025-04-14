import math
import statistics
from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Dict, List, Optional, Union

from fastapi import Depends, HTTPException
from requests import Session
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exception.custom import AnalysisException, DataNotFoundException, InvalidCountryException
from app.core.logger import setup_logger
from app.database.conn import db
from app.database.crud import database
from app.models.models_stock import StockInformation
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import FinancialCountry, TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.common.services import CommonService, get_common_service
from app.modules.common.utils import contry_mapping
from app.modules.financial.crud import FinancialCrud
from app.modules.financial.schemas import (
    CashFlowDetail,
    CashFlowResponse,
    DebtRatioResponse,
    FinancialRatioResponse,
    FinPosDetail,
    FinPosResponse,
    IncomeMetric,
    IncomePerformanceResponse,
    IncomeStatementDetail,
    IncomeStatementResponse,
    InterestCoverageRatioResponse,
    LiquidityRatioResponse,
    QuarterlyIncome,
)
from app.utils.oauth_utils import get_current_user

logger = setup_logger(__name__)


class FinancialService:
    def __init__(self, common_service: CommonService):
        self.db = database
        self.database = db
        self.common_service = common_service
        self._setup_tables()
        self.financial_crud = FinancialCrud(self.db)

    def _setup_tables(self):
        """
        테이블 설정 - 국가 코드를 기반으로 동적으로 테이블 이름 생성
        """

        def create_table_mapping(table_type: str) -> Dict[FinancialCountry, str]:
            return {country: f"{country.value}_{table_type}" for country in FinancialCountry}

        self.income_tables = create_table_mapping("income")
        self.cashflow_tables = create_table_mapping("cashflow")
        self.finpos_tables = create_table_mapping("finpos")

    def _get_date_conditions(self, start_date: Optional[str], end_date: Optional[str], user: AlphafinderUser) -> Dict:
        """
        날짜 조건 생성
        start_date (Optional[str]): YYYYMM 형식의 시작일
        end_date (Optional[str]): YYYYMM 형식의 종료일
        기본값은 최근 10년간의 데이터
        """
        from datetime import datetime

        conditions = {}
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month

        # 현재 월에 따른 가장 최근 분기 말월 계산
        latest_quarter_month = ((current_month - 1) // 3) * 3
        if latest_quarter_month == 0:
            latest_quarter_month = 12
            current_year -= 1
        latest_quarter_month = str(latest_quarter_month).zfill(2)  # 한 자리 월을 두 자리로 변환

        if not start_date:
            # 항상 10년치 데이터를 가져온 다음, 사용자 권한에 따라 처리
            conditions["period_q__gte"] = f"{current_year - 10}01"  # 10년 전부터 데이터 조회
            conditions["period_q__lte"] = f"{current_year}{latest_quarter_month}"  # 현재 연도의 마지막 분기
        else:
            conditions["period_q__gte"] = f"{start_date[:4]}01"  # 시작년도의 1월
            if end_date:
                conditions["period_q__lte"] = f"{end_date[:4]}{end_date[4:6]}"
            else:
                conditions["period_q__lte"] = f"{current_year}{latest_quarter_month}"

        return conditions

    async def _get_date_conditions_ten(self, start_date: Optional[str], end_date: Optional[str]) -> Dict:
        """
        날짜 조건 생성
        start_date (Optional[str]): YYYYMM 형식의 시작일
        end_date (Optional[str]): YYYYMM 형식의 종료일
        기본값은 2000년도부터 현재까지
        """
        from datetime import datetime

        conditions = {}
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month

        # 현재 월에 따른 가장 최근 분기 말월 계산
        latest_quarter_month = ((current_month - 1) // 3) * 3
        if latest_quarter_month == 0:
            latest_quarter_month = 12
            current_year -= 1
        latest_quarter_month = str(latest_quarter_month).zfill(2)  # 한 자리 월을 두 자리로 변환

        if not start_date:
            # 분기별 데이터는 2.5년(10분기)치, 연간 데이터는 10년치 조회를 위해 10년 전부터 데이터 조회
            conditions["period_q__gte"] = f"{current_year - 10}01"  # 10년 전부터
            conditions["period_q__lte"] = f"{current_year}{latest_quarter_month}"  # 현재 연도의 마지막 분기
        else:
            conditions["period_q__gte"] = f"{start_date[:4]}01"  # 시작년도의 1월
            if end_date:
                conditions["period_q__lte"] = f"{end_date[:4]}{end_date[4:6]}"
            else:
                conditions["period_q__lte"] = f"{current_year}{latest_quarter_month}"

        return conditions

    def _to_decimal(self, value) -> Optional[Decimal]:
        """
        값을 Decimal로 변환하고 JSON 직렬화 가능한 값으로 처리
        소수점 2자리까지 반올림
        None이나 빈 값은 None으로 반환
        """
        try:
            if value is None or (isinstance(value, str) and not value.strip()):
                return None

            if isinstance(value, (float, Decimal)):
                if isinstance(value, float) and math.isnan(value):
                    return None
                if isinstance(value, Decimal) and value.is_nan():
                    return None
                if isinstance(value, float) and math.isinf(value):
                    return None
                if isinstance(value, Decimal) and value.is_infinite():
                    return None

            # 값을 Decimal로 변환하고 소수점 2자리로 반올림
            return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        except (ValueError, TypeError, InvalidOperation):
            logger.warning(f"Failed to convert value to Decimal: {value}")
            return Decimal("0.00")

    ########################################## Router에서 호출하는 메서드 #########################################
    # 실적 데이터 조회
    async def get_income_performance_data(
        self,
        ctry: str,
        ticker: str,
        lang: TranslateCountry,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        db: Session = Depends(db.get_db),
        user: AlphafinderUser = Depends(get_current_user),
    ) -> BaseResponse[IncomePerformanceResponse]:
        """
        실적 데이터 조회
        """
        try:
            if ticker:
                if ctry == "KOR":
                    ticker = ticker

            if ctry == "USA":
                ticker = f"{ticker}-US"
            country = FinancialCountry(ctry)
            table_name = self.income_tables.get(country)

            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException()

            # 정보 조회
            company_name, sector, tickers = await self.get_stock_info_by_ticker(ticker, lang)

            if not sector:
                logger.warning(f"No sector information found for ticker {ticker}")
                # 섹터 정보가 없는 경우 해당 기업의 데이터만 조회
                tickers_with_suffix = [ticker]
            else:
                if ctry == "USA":
                    tickers_with_suffix = [f"{t}-US" for t in tickers]
                else:
                    tickers_with_suffix = tickers

            logger.info(f"Querying {table_name} with ticker: {ticker}, sector tickers count: {len(tickers_with_suffix)}")

            date_conditions = await self._get_date_conditions_ten(start_date, end_date)
            conditions = {
                "Code__in": tickers_with_suffix,
                **date_conditions,
            }

            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                logger.warning(f"No income performance data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="실적")

            all_shares = await self.get_all_shares_by_tickers(tickers, ctry)

            quarterly_statements = await self._process_income_performance_quarterly_result(
                result, ticker, ctry, tickers, all_shares, user
            )
            yearly_statements = await self._process_income_performance_yearly_result(
                result, ticker, ctry, tickers, all_shares, user
            )

            ctry = contry_mapping.get(ctry)

            performance_response = IncomePerformanceResponse(
                code=ticker,
                name=company_name,
                ctry=ctry,
                sector=sector if sector else "",
                quarterly=quarterly_statements,
                yearly=yearly_statements,
            )

            logger.info(f"Successfully retrieved income performance data for {ticker}")
            return BaseResponse[IncomePerformanceResponse](
                status_code=200, message="실적 데이터를 성공적으로 조회했습니다.", data=performance_response
            )

        except (InvalidCountryException, DataNotFoundException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_income_performance_data: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="실적 조회", detail=str(e))

    # 손익계산서
    def get_income_analysis(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user: AlphafinderUser = Depends(get_current_user),
    ) -> BaseResponse[IncomeStatementResponse]:
        """
        손익계산서 시계열 분석
        """
        logger.info(f"Starting income analysis for {ticker}")

        try:
            income_data = self.get_income_data(
                ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date, user=user
            )
            # data를 details로 변경
            if not income_data.data.details:
                logger.warning(f"No data found for ticker: {ticker}")
                return BaseResponse[IncomeStatementResponse](
                    status_code=404,
                    message="데이터를 찾을 수 없습니다.",
                    data=IncomeStatementResponse(code=ticker, name="", details=[]),
                )

            logger.info(f"Successfully completed income analysis for {ticker}")
            return income_data

        except Exception as e:
            logger.error(f"Error during income analysis for {ticker}: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="손익계산서 시계열", detail=str(e))

    # 현금흐름표
    def get_cashflow_analysis(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user: AlphafinderUser = Depends(get_current_user),
    ) -> BaseResponse[CashFlowResponse]:
        """
        현금흐름 시계열 분석
        """
        try:
            cashflow_data = self.get_cashflow_data(
                ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date, user=user
            )
            if not cashflow_data.data.details:
                logger.warning(f"No data found for ticker: {ticker}")
                return BaseResponse[CashFlowResponse](
                    status_code=404,
                    message="데이터를 찾을 수 없습니다.",
                    data=CashFlowResponse(code=ticker, name="", ttm=CashFlowDetail(), details=[]),
                )

            logger.info(f"Successfully completed cashflow analysis for {ticker}")
            return cashflow_data

        except Exception as e:
            logger.error(f"Error during cashflow analysis for {ticker}: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="현금흐름표 시계열", detail=str(e))

    # 재무상태표
    def get_finpos_analysis(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user: AlphafinderUser = Depends(get_current_user),
    ) -> BaseResponse[FinPosResponse]:
        """
        재무상태표 시계열 분석
        """
        try:
            finpos_data = self.get_finpos_data(
                ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date, user=user
            )

            if not finpos_data.data.details:
                return BaseResponse[FinPosResponse](
                    status_code=404,
                    message="데이터를 찾을 수 없습니다.",
                    data=FinPosResponse(code=ticker, name="", ttm=FinPosDetail(), details=[]),
                )

            logger.info(f"Successfully completed finpos analysis for {ticker}")
            return finpos_data

        except Exception as e:
            logger.error(f"Unexpected error in get_finpos_timeseries_analysis: {str(e)}")
            raise HTTPException(status_code=500, detail="내부 서버 오류")

    # 재무비율
    def get_financial_ratio(
        self, ctry: FinancialCountry, ticker: str, db: AsyncSession
    ) -> BaseResponse[FinancialRatioResponse]:
        """
        재무비율 조회
        """
        try:
            if ctry == "USA":
                ticker = f"{ticker}-US"
            country = FinancialCountry(ctry)
            # finpos 테이블에서 조회
            financial_ratio_data = self.get_financial_ratio_data(country, ticker, db)
            return financial_ratio_data
        except Exception as e:
            logger.error(f"Unexpected error in get_financial_ratio: {str(e)}")
            raise AnalysisException(analysis_type="재무비율 조회", detail=str(e))

    def get_debt_ratio(self, ctry: FinancialCountry, ticker: str, db: Session) -> BaseResponse[DebtRatioResponse]:
        """
        부채비율 조회
        """
        try:
            if ctry == "USA":
                ticker = f"{ticker}-US"
            country = FinancialCountry(ctry)
            # finpos 테이블에서 조회
            debt_ratio_data = self.get_debt_ratio_data(country, ticker, db)
            return debt_ratio_data
        except Exception as e:
            logger.error(f"Unexpected error in get_debt_ratio: {str(e)}")
            raise AnalysisException(analysis_type="부채비율 조회", detail=str(e))

    # 유동비율
    def get_liquidity_ratio(
        self, ctry: FinancialCountry, ticker: str, db: AsyncSession
    ) -> BaseResponse[LiquidityRatioResponse]:
        """
        유동비율 조회
        """
        try:
            if ctry == "USA":
                ticker = f"{ticker}-US"
            country = FinancialCountry(ctry)
            # finpos 테이블에서 조회
            liquidity_ratio_data = self.get_liquidity_ratio_data(country, ticker, db)
            return liquidity_ratio_data
        except Exception as e:
            logger.error(f"Unexpected error in get_liquidity_ratio: {str(e)}")
            raise AnalysisException(analysis_type="유동비율 조회", detail=str(e))

    # 이자보상배율
    def get_interest_coverage_ratio(
        self, ctry: FinancialCountry, ticker: str, db: Session
    ) -> BaseResponse[InterestCoverageRatioResponse]:
        """
        이자보상배율 조회
        """
        try:
            if ctry == "USA":
                ticker = f"{ticker}-US"
            country = FinancialCountry(ctry)
            # finpos 테이블에서 조회
            interest_coverage_ratio_data = self.get_interest_coverage_ratio_data(country, ticker, db)
            return interest_coverage_ratio_data
        except Exception as e:
            logger.error(f"Unexpected error in get_interest_coverage_ratio: {str(e)}")
            raise AnalysisException(analysis_type="이자보상배율 조회", detail=str(e))

    ########################################## 데이터 조회 메서드 #########################################
    # 손익계산서
    def get_income_data(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user: AlphafinderUser = Depends(get_current_user),
    ) -> BaseResponse[IncomeStatementResponse]:
        """
        손익계산서 데이터 조회
        """
        try:
            table_name = self.income_tables.get(ctry)
            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException(country=ctry)

            db_ticker = f"{ticker}-US" if ctry == FinancialCountry.USA else ticker
            conditions = {"Code": db_ticker, **self._get_date_conditions(start_date, end_date, user)}

            logger.debug(f"Querying income data for {ticker} with conditions: {conditions}")

            # 전체 데이터를 가져온 후 파이썬에서 정렬
            result = self.db._select(table=table_name, **conditions)

            if not result:
                target_en_name = self.db._select(table="stock_information", columns=["en_name"], ticker=ticker)
                same_company_tickers = self.db._select(
                    table="stock_information", columns=["ticker", "en_name"], en_name=target_en_name[0][0]
                )
                ticker_list = [ticker[0] for ticker in same_company_tickers]
                ticker_list = [f"{t}-US" if ctry == FinancialCountry.USA else t for t in ticker_list if t != ticker]
                if len(ticker_list) == 1:
                    conditions = {"Code": ticker_list[0], **self._get_date_conditions(start_date, end_date, user)}
                    result = self.db._select(table=table_name, **conditions)
                elif not ticker_list and len(ticker_list) > 1:
                    logger.warning(f"No income data found for ticker: {ticker}")
                    raise DataNotFoundException(ticker=ticker, data_type="손익계산")

            # 정렬: 연도, 분기 내림차순
            sorted_result = sorted(
                result,
                key=lambda x: (-int(x.period_q[:4]), -int(x.period_q[4:])),  # 연도, 분기 순으로 내림차순 정렬
            )

            name = sorted_result[0][1] if sorted_result else ""

            # 정렬된 결과로 처리 - 사용자 권한에 따라 데이터 필터링 적용
            statements = self._process_income_statement_result(sorted_result, user=user)
            ttm = self._process_income_ttm_result(sorted_result)
            total = self._process_income_total_result(sorted_result, user=user)

            # 구독 레벨 1인 경우, TTM 데이터에 대한 접근 권한 확인
            subscribed_level = getattr(user, "subscription_level", 1)
            if subscribed_level < 3:
                # TTM 데이터는 항상 접근 가능 (최신 데이터이므로)
                pass

            ctry_code = contry_mapping.get(ctry.value, "").lower()

            income_statement_response = IncomeStatementResponse(
                code=ticker,
                name=name,
                ctry=ctry_code,
                ttm=ttm,
                total=total,
                details=statements,
            )

            logger.info(f"Successfully retrieved income data for {ticker}")
            return BaseResponse[IncomeStatementResponse](
                status_code=200, message="손익계산서 데이터를 성공적으로 조회했습니다.", data=income_statement_response
            )

        except (InvalidCountryException, DataNotFoundException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_income_data: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="손익계산서 조회", detail=str(e))

    # 현금흐름표
    def get_cashflow_data(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user: AlphafinderUser = Depends(get_current_user),
    ) -> BaseResponse[CashFlowResponse]:
        """
        현금흐름표 데이터 조회
        """
        try:
            table_name = self.cashflow_tables.get(ctry)
            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException(country=ctry)

            db_ticker = f"{ticker}-US" if ctry == FinancialCountry.USA else ticker
            conditions = {"Code": db_ticker, **self._get_date_conditions(start_date, end_date, user)}

            logger.debug(f"Querying cashflow data for {ticker} with conditions: {conditions}")
            result = self.db._select(table=table_name, **conditions)

            if not result:
                target_en_name = self.db._select(table="stock_information", columns=["en_name"], ticker=ticker)
                same_company_tickers = self.db._select(
                    table="stock_information", columns=["ticker", "en_name"], en_name=target_en_name[0][0]
                )
                ticker_list = [ticker[0] for ticker in same_company_tickers]
                ticker_list = [f"{t}-US" if ctry == FinancialCountry.USA else t for t in ticker_list if t != ticker]
                if len(ticker_list) == 1:
                    conditions = {"Code": ticker_list[0], **self._get_date_conditions(start_date, end_date, user)}
                    result = self.db._select(table=table_name, **conditions)
                elif not ticker_list and len(ticker_list) > 1:
                    logger.warning(f"No cashflow data found for ticker: {ticker}")
                    raise DataNotFoundException(ticker=ticker, data_type="현금흐름")

            # 정렬: 연도, 분기 내림차순
            sorted_result = sorted(
                result,
                key=lambda x: (-int(x.period_q[:4]), -int(x.period_q[4:])),  # 연도, 분기 순으로 내림차순 정렬
            )

            # 사용자 권한에 따라 데이터 필터링 적용
            statements = self._process_cashflow_result(sorted_result, user=user)
            ttm = self._process_cashflow_ttm_result(sorted_result)
            name = sorted_result[0][1] if sorted_result else ""
            total = self._process_cashflow_total_result(sorted_result, user=user)

            # 구독 레벨 1인 경우, TTM 데이터에 대한 접근 권한은 항상 제공
            subscribed_level = getattr(user, "subscription_level", 1)
            if subscribed_level < 3:
                # TTM 데이터는 항상 접근 가능 (최신 데이터이므로)
                pass

            ctry_code = contry_mapping.get(ctry.value, "").lower()
            cashflow_response = CashFlowResponse(
                code=ticker, name=name, ctry=ctry_code, ttm=ttm, total=total, details=statements
            )

            logger.info(f"Successfully retrieved cashflow data for {ticker}")
            return BaseResponse[CashFlowResponse](
                status_code=200, message="현금흐름표 데이터를 성공적으로 조회했습니다.", data=cashflow_response
            )

        except (InvalidCountryException, DataNotFoundException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_cashflow_data: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="현금흐름표 조회", detail=str(e))

    # 재무상태표
    def get_finpos_data(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user: AlphafinderUser = Depends(get_current_user),
    ) -> BaseResponse[FinPosResponse]:
        """
        재무상태표 데이터 조회
        """
        try:
            table_name = self.finpos_tables.get(ctry)
            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException(country=ctry)

            db_ticker = f"{ticker}-US" if ctry == FinancialCountry.USA else ticker
            conditions = {"Code": db_ticker, **self._get_date_conditions(start_date, end_date, user)}

            logger.debug(f"Querying finpos data for {ticker} with conditions: {conditions}")
            result = self.db._select(table=table_name, **conditions)

            if not result:
                target_en_name = self.db._select(table="stock_information", columns=["en_name"], ticker=ticker)
                same_company_tickers = self.db._select(
                    table="stock_information", columns=["ticker", "en_name"], en_name=target_en_name[0][0]
                )
                ticker_list = [ticker[0] for ticker in same_company_tickers]
                ticker_list = [f"{t}-US" if ctry == FinancialCountry.USA else t for t in ticker_list if t != ticker]
                if len(ticker_list) == 1:
                    conditions = {"Code": ticker_list[0], **self._get_date_conditions(start_date, end_date, user)}
                    result = self.db._select(table=table_name, **conditions)
                elif not ticker_list and len(ticker_list) > 1:
                    logger.warning(f"No finpos data found for ticker: {ticker}")
                    raise DataNotFoundException(ticker=ticker, data_type="재무상태")

            # 정렬: 연도, 분기 내림차순
            sorted_result = sorted(
                result,
                key=lambda x: (-int(x.period_q[:4]), -int(x.period_q[4:])),  # 연도, 분기 순으로 내림차순 정렬
            )

            # 사용자 권한에 따라 데이터 필터링 적용
            statements = self._process_finpos_result(sorted_result, user=user)
            ttm = self._process_finpos_ttm_result(sorted_result)
            name = sorted_result[0][1] if sorted_result else ""
            total = self._process_finpos_total_result(sorted_result, user=user)

            # 구독 레벨 1인 경우, TTM 데이터에 대한 접근 권한은 항상 제공
            subscribed_level = getattr(user, "subscription_level", 1)
            if subscribed_level < 3:
                # TTM 데이터는 항상 접근 가능 (최신 데이터이므로)
                pass

            ctry_code = contry_mapping.get(ctry.value, "").lower()
            finpos_response = FinPosResponse(
                code=ticker, name=name, ctry=ctry_code, ttm=ttm, total=total, details=statements
            )

            logger.info(f"Successfully retrieved finpos data for {ticker}")
            return BaseResponse[FinPosResponse](
                status_code=200, message="재무상태표 데이터를 성공적으로 조회했습니다.", data=finpos_response
            )

        except (InvalidCountryException, DataNotFoundException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_finpos_data: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="재무상태표 조회", detail=str(e))

    def _get_latest_quarter(self, ctry: FinancialCountry, ticker: str) -> str:
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

    def get_name_by_ticker(self, ticker: str, lang: TranslateCountry) -> Optional[str]:
        """
        ticker로 StockInformation 테이블에서 한글로 된 기업이름 조회

        Args:
            db (AsyncSession): 데이터베이스 세션
            ticker (str): 종목 코드

        Returns:
        """
        if ticker.endswith("-US"):
            ticker = ticker[:-3]

        column = "kr_name" if lang == TranslateCountry.KO else "en_name"

        name = self.db._select(table="stock_information", columns=[column], ticker=ticker)

        return name[0][0]

    # 섹터 조회
    def get_sector_by_ticker(self, ticker: str) -> Optional[str]:
        """
        종목 섹터 조회
        """
        if ticker.endswith("-US"):
            ticker = ticker[:-3]

        query = select(StockInformation.sector_2).where(StockInformation.ticker == ticker)
        result = self.db._execute(query)
        sector = result.scalar_one_or_none()
        return sector

    # 섹터 내 종목 조회
    def get_ticker_by_sector(self, sector: str) -> List[str]:
        """섹터에 속한 종목 리스트 조회"""
        if not sector:  # sector가 None이거나 빈 문자열인 경우
            logger.warning("Sector is empty, returning empty list to prevent full table scan")
            return []  # 빈 리스트 반환하여 전체 종목 조회 방지

        result = self.db._select(table="stock_information", columns=["ticker"], sector_2=sector)

        return [row.ticker for row in result] if result else []

    async def get_stock_info_by_ticker(self, ticker: str, lang: TranslateCountry):
        """한 번의 쿼리로 stock 관련 정보 조회"""
        try:
            clean_ticker = ticker[:-3] if ticker.endswith("-US") else ticker
            name = "kr_name" if lang == TranslateCountry.KO else "en_name"
            sector = "sector_ko" if lang == TranslateCountry.KO else "sector_2"

            # 1. 기본 회사 정보 조회
            base_result = self.db._select(
                table="stock_information", columns=[name, sector, "ticker"], ticker=clean_ticker, limit=1
            )
            if not base_result:
                return clean_ticker, None, [ticker]

            company_name, company_sector, _ = base_result[0]

            # 2. 동일 섹터의 다른 티커 조회
            if company_sector:
                condition = {}
                condition[sector] = company_sector
                sector_result = self.db._select(
                    table="stock_information",
                    columns=["ticker"],
                    **condition,
                )
                sector_tickers = [row.ticker for row in sector_result]
            else:
                sector_tickers = [ticker]

            # None 체크 및 기본값 설정
            company_name = company_name if company_name else clean_ticker

            return company_name, company_sector, sector_tickers

        except Exception as e:
            logger.error(f"Error in get_stock_info_by_ticker: {e}")
            return clean_ticker, None, [ticker]

    def get_shares_by_ticker(self, ticker: str, country: str) -> float:
        try:
            country_enum = FinancialCountry(country)
            table_name = f"{country_enum.value}_stock_factors"

            result = self.db._select(table=table_name, ticker=ticker, limit=1)

            SHARED_OUTSTANDING_INDEX = 2

            if result and len(result) > 0:
                shares_value = result[0][SHARED_OUTSTANDING_INDEX]
                return float(shares_value) if shares_value is not None else 0.0

            return 0.0
        except Exception as e:
            logger.error(f"Error getting shares for {ticker}: {e}")
            return 0.0

    ########################################## 계산 메서드 #########################################
    # 부채비율 계산
    def get_financial_ratio_data(
        self, country: FinancialCountry, ticker: str, db: Session
    ) -> BaseResponse[FinancialRatioResponse]:
        """
        재무비율 데이터 조회 - 부채비율 (최근 4분기 평균)
        부채비율 = (총부채 / 자기자본) * 100
        회사명도 함께 반환
        """
        table_name = self.finpos_tables.get(country)
        if not table_name:
            logger.warning(f"잘못된 국가 코드: {country}")
            raise InvalidCountryException()

        quarters = self.financial_crud.get_financial_ratio_quarters(table_name, ticker, db)

        if not quarters:
            logger.warning(f"재무비율 데이터를 찾을 수 없습니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="재무비율")

        if len(quarters) < 4:
            logger.warning(f"4분기 데이터가 부족합니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="재무비율(4분기)")

        # 벡터화된 계산
        debt_ratios = [
            float((self._to_decimal(q.total_dept) / self._to_decimal(q.equity)) * 100)
            if self._to_decimal(q.equity) != 0
            else 0.0
            for q in quarters
        ]

        # 병렬 처리: 평균 계산과 산업 평균 조회를 동시에
        average_debt_ratio = self._round_and_clean(sum(debt_ratios) / len(debt_ratios))
        industry_avg = self._round_and_clean(self.get_financial_industry_avg(country=country, ticker=ticker, db=db))

        financial_ratio_response = FinancialRatioResponse(
            code=ticker, ratio=average_debt_ratio, industry_avg=industry_avg
        )

        return BaseResponse[FinancialRatioResponse](
            status_code=200,
            message="부채비율(4분기 평균) 데이터를 성공적으로 조회했습니다.",
            data=financial_ratio_response,
        )

    def get_debt_ratio_data(self, country: FinancialCountry, ticker: str, db: Session) -> BaseResponse[DebtRatioResponse]:
        """
        부채비율 데이터 조회
        """
        table_name = self.finpos_tables.get(country)
        if not table_name:
            logger.warning(f"잘못된 국가 코드: {country}")
            raise InvalidCountryException()

        quarters = self.financial_crud.get_debt_ratio_quarters(table_name, ticker, db)

        if not quarters:
            if ticker.endswith("-US"):
                ticker = ticker[:-3]
            target_en_name = self.db._select(table="stock_information", columns=["en_name"], ticker=ticker)
            same_company_tickers = self.db._select(
                table="stock_information", columns=["ticker", "en_name"], en_name=target_en_name[0][0]
            )
            ticker_list = [ticker[0] for ticker in same_company_tickers]
            ticker_list = [f"{t}-US" if country == FinancialCountry.USA else t for t in ticker_list if t != ticker]
            if len(ticker_list) == 1:
                quarters = self.financial_crud.get_debt_ratio_quarters(table_name, ticker_list[0], db)
            elif not ticker_list and len(ticker_list) > 1:
                logger.warning(f"No debt ratio data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="부채비율")
            else:
                logger.warning(f"부채비율 데이터를 찾을 수 없습니다: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="부채비율")

        if len(quarters) < 4:
            logger.warning(f"4분기 데이터가 부족합니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="부채비율(4분기)")

        try:
            # 벡터화된 계산
            debt_ratios = [
                float((self._to_decimal(q.total_dept) / self._to_decimal(q.total_asset)) * 100)
                if self._to_decimal(q.total_asset) != 0
                else 0.0
                for q in quarters
            ]

            # 병렬 처리: 평균 계산과 산업 평균 조회를 동시에
            average_debt_ratio = self._round_and_clean(sum(debt_ratios) / len(debt_ratios))
        except TypeError:
            average_debt_ratio = 0.0  # TODO:: FE에서 None 받을 수 있는지 확인 후 'None' 반환 필요

        industry_avg = self._round_and_clean(self.get_debt_ratio_industry_avg(country=country, ticker=ticker, db=db))

        debt_ratio_response = DebtRatioResponse(code=ticker, ratio=average_debt_ratio, industry_avg=industry_avg)

        return BaseResponse[DebtRatioResponse](
            status_code=200,
            message="부채비율(4분기 평균) 데이터를 성공적으로 조회했습니다.",
            data=debt_ratio_response,
        )

    # 유동비율 계산
    def get_liquidity_ratio_data(
        self, country: FinancialCountry, ticker: str, db: AsyncSession
    ) -> BaseResponse[LiquidityRatioResponse]:
        """
        유동비율 데이터 조회 (최근 4분기 평균)
        유동비율 = (유동자산 / 유동부채) * 100
        """
        table_name = self.finpos_tables.get(country)
        if not table_name:
            logger.warning(f"잘못된 국가 코드: {country}")
            raise InvalidCountryException()

        quarters = self.financial_crud.get_liquidity_ratio_quarters(table_name, ticker, db)

        if not quarters:
            if ticker.endswith("-US"):
                ticker = ticker[:-3]
            target_en_name = self.db._select(table="stock_information", columns=["en_name"], ticker=ticker)
            same_company_tickers = self.db._select(
                table="stock_information", columns=["ticker", "en_name"], en_name=target_en_name[0][0]
            )
            ticker_list = [ticker[0] for ticker in same_company_tickers]
            ticker_list = [f"{t}-US" if country == FinancialCountry.USA else t for t in ticker_list if t != ticker]
            if len(ticker_list) == 1:
                quarters = self.financial_crud.get_liquidity_ratio_quarters(table_name, ticker_list[0], db)
            elif not ticker_list and len(ticker_list) > 1:
                logger.warning(f"No liquidity ratio data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="유동비율")
            else:
                logger.warning(f"유동비율 데이터를 찾을 수 없습니다: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="유동비율")

        if len(quarters) < 4:
            logger.warning(f"4분기 데이터가 부족합니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="유동비율(4분기)")

        try:
            # 벡터화된 계산
            liquidity_ratios = [
                float((self._to_decimal(q.current_asset) / self._to_decimal(q.current_dept)) * 100)
                if self._to_decimal(q.current_dept) != 0
                else 0.0
                for q in quarters
            ]

            # 병렬 처리: 평균 계산과 산업 평균 조회를 동시에
            average_liquidity_ratio = self._round_and_clean(sum(liquidity_ratios) / len(liquidity_ratios))
        except TypeError:
            average_liquidity_ratio = 0.0  # TODO:: FE에서 None 받을 수 있는지 확인 후 'None' 반환 필요

        industry_avg = self._round_and_clean(self.get_liquidity_industry_avg(country=country, ticker=ticker, db=db))

        return BaseResponse[LiquidityRatioResponse](
            status_code=200,
            message="유동비율(4분기 평균) 데이터를 성공적으로 조회했습니다.",
            data=LiquidityRatioResponse(
                code=ticker, name=quarters[0].Name, ratio=average_liquidity_ratio, industry_avg=industry_avg
            ),
        )

    # 이자보상배율 계산
    def get_interest_coverage_ratio_data(
        self, country: FinancialCountry, ticker: str, db: Session
    ) -> BaseResponse[InterestCoverageRatioResponse]:
        """
        이자보상배율 데이터 조회 (최근 4분기 평균)
        이자보상배율 = 영업이익 / 금융비용
        """
        table_name = self.income_tables.get(country)
        if not table_name:
            logger.warning(f"잘못된 국가 코드: {country}")
            raise InvalidCountryException()

        quarters = self.financial_crud.get_interest_coverage_ratio_quarters(table_name, ticker, db)

        if not quarters:
            if ticker.endswith("-US"):
                ticker = ticker[:-3]
            target_en_name = self.db._select(table="stock_information", columns=["en_name"], ticker=ticker)
            same_company_tickers = self.db._select(
                table="stock_information", columns=["ticker", "en_name"], en_name=target_en_name[0][0]
            )
            ticker_list = [ticker[0] for ticker in same_company_tickers]
            ticker_list = [f"{t}-US" if country == FinancialCountry.USA else t for t in ticker_list if t != ticker]
            if len(ticker_list) == 1:
                quarters = self.financial_crud.get_interest_coverage_ratio_quarters(table_name, ticker_list[0], db)
            elif not ticker_list and len(ticker_list) > 1:
                logger.warning(f"No interest coverage ratio data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="이자보상배율")
            else:
                logger.warning(f"이자보상배율 데이터를 찾을 수 없습니다: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="이자보상배율")

        if len(quarters) < 4:
            logger.warning(f"4분기 데이터가 부족합니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="이자보상배율(4분기)")

        try:
            interest_coverage_ratios = [
                float(self._to_decimal(q.operating_income) / self._to_decimal(q.fin_cost))
                if self._to_decimal(q.fin_cost) != 0
                else 0.0
                for q in quarters
            ]

            average_ratio = self._round_and_clean(sum(interest_coverage_ratios) / len(interest_coverage_ratios))
        except TypeError:
            average_ratio = 0.0  # TODO:: FE에서 None 받을 수 있는지 확인 후 'None' 반환 필요

        industry_avg = self._round_and_clean(
            self.get_interest_coverage_industry_avg(country=country, ticker=ticker, db=db)
        )

        return BaseResponse[InterestCoverageRatioResponse](
            status_code=200,
            message="이자보상배율(4분기 평균) 데이터를 성공적으로 조회했습니다.",
            data=InterestCoverageRatioResponse(
                code=ticker, name=quarters[0].Name, ratio=average_ratio, industry_avg=industry_avg
            ),
        )

    # 재무비율 업종 평균 조회
    def get_financial_industry_avg(self, country: FinancialCountry, ticker: str, db: Session) -> float:
        """업종 평균 부채비율 조회"""
        table_name = self.finpos_tables.get(country)
        if not table_name:
            return 0.0

        return self._round_and_clean(
            self.financial_crud.get_financial_industry_avg_data(
                table_name=table_name,
                base_ticker=ticker.replace("-US", "") if country == FinancialCountry.USA else ticker,
                is_usa=country == FinancialCountry.USA,
                ratio_type="debt",
                db=db,
            )
        )

    # 부채비율 업종 평균 조회
    def get_debt_ratio_industry_avg(self, country: FinancialCountry, ticker: str, db: AsyncSession) -> float:
        """업종 평균 부채비율 조회"""
        table_name = self.finpos_tables.get(country)
        if not table_name:
            return 0.0
        return self._round_and_clean(
            self.financial_crud.get_financial_industry_avg_data(
                table_name=table_name,
                base_ticker=ticker.replace("-US", "") if country == FinancialCountry.USA else ticker,
                is_usa=country == FinancialCountry.USA,
                ratio_type="debt",
                db=db,
            )
        )

    def get_liquidity_industry_avg(self, country: FinancialCountry, ticker: str, db: Session) -> float:
        """업종 평균 유동비율 조회"""
        table_name = self.finpos_tables.get(country)
        if not table_name:
            return 0.0

        return self._round_and_clean(
            self.financial_crud.get_financial_industry_avg_data(
                table_name=table_name,
                base_ticker=ticker.replace("-US", "") if country == FinancialCountry.USA else ticker,
                is_usa=country == FinancialCountry.USA,
                ratio_type="liquidity",
                db=db,
            )
        )

    def get_interest_coverage_industry_avg(self, country: FinancialCountry, ticker: str, db: Session) -> float:
        """업종 평균 이자보상배율 조회"""
        table_name = self.income_tables.get(country)
        if not table_name:
            return 0.0

        return self._round_and_clean(
            self.financial_crud.get_financial_industry_avg_data(
                table_name=table_name,
                base_ticker=ticker.replace("-US", "") if country == FinancialCountry.USA else ticker,
                is_usa=country == FinancialCountry.USA,
                ratio_type="interest",
                db=db,
            )
        )

    ########################################## ttm 메서드 #########################################
    # 손익계산서 ttm
    def _process_income_ttm_result(self, result) -> IncomeStatementDetail:
        """
        손익계산서 ttm 결과 처리 - 모든 재무 항목에 대해 최근 12개월 합산
        """
        if not result:
            return IncomeStatementDetail()

        # 기간으로 정렬 (최신 데이터가 앞으로 오도록)
        sorted_result = sorted(result, key=lambda x: x.period_q, reverse=True)

        # 최근 12개월 데이터 선택
        recent_12_months = sorted_result[:4]

        # 첫 번째 row에서 컬럼 추출
        exclude_columns = ["Code", "Name", "StmtDt"]
        first_row = recent_12_months[0]

        # TTM 계산을 위한 딕셔너리 초기화
        ttm_dict = {}
        for col, val in zip(first_row._fields, first_row):
            if col not in exclude_columns and col != "period_q":
                # 모든 값이 None인 경우 None을 유지
                values = [self._to_decimal(getattr(row, col)) for row in recent_12_months]
                if all(v is None for v in values):
                    ttm_dict[col] = None
                else:
                    # None이 아닌 값들만 합산
                    ttm_dict[col] = sum((v or Decimal("0.00")) for v in values)

        ttm_dict["period_q"] = "TTM"

        return self._create_income_statement_detail(ttm_dict)

    # 현금흐름표 ttm
    def _process_cashflow_ttm_result(self, result) -> CashFlowDetail:
        """
        현금흐름표 ttm 결과 처리 - 모든 재무 항목에 대해 최근 12개월 합산
        """
        if not result:
            return CashFlowDetail()

        # 기간으로 정렬 (최신 데이터가 앞으로 오도록)
        sorted_result = sorted(result, key=lambda x: x.period_q, reverse=True)

        # 최근 12개월 데이터 선택
        recent_12_months = sorted_result[:4]

        # 첫 번째 row에서 컬럼 추출
        exclude_columns = ["Code", "Name", "StmtDt"]
        first_row = recent_12_months[0]

        # TTM 계산을 위한 딕셔너리 초기화
        ttm_dict = {}
        for col, val in zip(first_row._fields, first_row):
            if col not in exclude_columns and col != "period_q":
                # 모든 값이 None인 경우 None을 유지
                values = [self._to_decimal(getattr(row, col)) for row in recent_12_months]
                if all(v is None for v in values):
                    ttm_dict[col] = None
                else:
                    # None이 아닌 값들만 합산
                    ttm_dict[col] = sum((v or Decimal("0.00")) for v in values)

        # TTM 값에는 'TTM'이라고 표시
        ttm_dict["period_q"] = "TTM"

        return self._create_cashflow_detail(ttm_dict)

    # 재무상태표 ttm
    def _process_finpos_ttm_result(self, result) -> FinPosDetail:
        """
        재무상태표 ttm 결과 처리 - 각 컬럼별로 최근 12개월 합산
        """
        if not result:
            return FinPosDetail()

        # 기간으로 정렬 (최신 데이터가 앞으로 오도록)
        sorted_result = sorted(result, key=lambda x: x.period_q, reverse=True)

        # 최근 분기 데이터
        latest_quarter = sorted_result[0]

        # 첫 번째 row에서 컬럼 추출
        exclude_columns = ["Code", "Name", "StmtDt"]
        latest_quarter_dict = {
            col: self._to_decimal(getattr(latest_quarter, col, None)) or Decimal("0.00")
            for col in latest_quarter._fields
            if col not in exclude_columns and col != "period_q"
        }

        latest_quarter_dict["period_q"] = "TTM"

        return self._create_finpos_detail(latest_quarter_dict)

    def _process_income_total_result(self, result, user: Optional[AlphafinderUser] = None) -> List[IncomeStatementDetail]:
        """
        년도별 손익계산서 조회 - 각 년도의 분기 데이터를 합산하여 연간 총계 계산
        """
        if not result:
            return []

        # 년도별 데이터 집계
        yearly_data = defaultdict(lambda: defaultdict(list))

        for row in result:
            year = str(row.period_q)[:4]  # YYYYMM 형식에서 YYYY 추출

            # 제외할 컬럼
            exclude_columns = ["Code", "Name", "StmtDt", "period_q"]

            # 각 필드별로 년도별 데이터 수집
            for field_name, value in zip(row._fields, row):
                if field_name not in exclude_columns:
                    yearly_data[year][field_name].append(self._to_decimal(value))

        # 연도별 합산 데이터를 IncomeStatementDetail 객체로 변환
        yearly_statements = []
        for year in sorted(yearly_data.keys(), reverse=True):
            year_dict = {}

            # 각 필드별로 처리
            for field_name, values in yearly_data[year].items():
                # 모든 값이 None인 경우 None을 유지
                if all(v is None for v in values):
                    year_dict[field_name] = None
                else:
                    # None이 아닌 값들만 합산
                    year_dict[field_name] = sum((v or Decimal("0.00")) for v in values)

            # period_q를 연도로 설정
            year_dict["period_q"] = year

            # IncomeStatementDetail 객체 생성
            yearly_statement = self._create_income_statement_detail(year_dict)
            yearly_statements.append(yearly_statement)

        # 최근 10년치 데이터만 사용
        yearly_statements = yearly_statements[:10]

        # 사용자 구독 레벨에 따른 접근 제어 적용
        if user:
            yearly_statements = self._apply_subscription_based_access(yearly_statements, user, is_yearly=True)

        return yearly_statements

    def _process_cashflow_total_result(self, result, user: Optional[AlphafinderUser] = None) -> List[CashFlowDetail]:
        """
        년도별 현금흐름표 조회 - 각 년도의 분기 데이터를 합산하여 연간 총계 계산
        """
        if not result:
            return []

        # 년도별 데이터 집계
        yearly_data = defaultdict(lambda: defaultdict(list))

        for row in result:
            year = str(row.period_q)[:4]  # YYYYMM 형식에서 YYYY 추출

            # 제외할 컬럼
            exclude_columns = ["Code", "Name", "StmtDt", "period_q"]

            # 각 필드별로 년도별 데이터 수집
            for field_name, value in zip(row._fields, row):
                if field_name not in exclude_columns:
                    yearly_data[year][field_name].append(self._to_decimal(value))

        # 연도별 합산 데이터를 CashFlowDetail 객체로 변환
        yearly_statements = []
        for year in sorted(yearly_data.keys(), reverse=True):
            year_dict = {}

            # 각 필드별로 처리
            for field_name, values in yearly_data[year].items():
                # 모든 값이 None인 경우 None을 유지
                if all(v is None for v in values):
                    year_dict[field_name] = None
                else:
                    # None이 아닌 값들만 합산
                    year_dict[field_name] = sum((v or Decimal("0.00")) for v in values)

            # period_q를 연도로 설정
            year_dict["period_q"] = year

            # CashFlowDetail 객체 생성
            yearly_statement = self._create_cashflow_detail(year_dict)
            yearly_statements.append(yearly_statement)

        # 최근 10년치 데이터만 사용
        yearly_statements = yearly_statements[:10]

        # 사용자 구독 레벨에 따른 접근 제어 적용
        if user:
            yearly_statements = self._apply_subscription_based_access(yearly_statements, user, is_yearly=True)

        return yearly_statements

    def _process_finpos_total_result(self, result, user: Optional[AlphafinderUser] = None) -> List[FinPosDetail]:
        """
        년도별 재무상태표 조회 - 각 년도의 분기 데이터를 합산하여 연간 총계 계산
        재무상태표는 특성상 마지막 분기의 데이터를 사용
        """
        if not result:
            return []

        # 년도별 마지막 분기 데이터 저장
        yearly_data = {}

        for row in result:
            year = str(row.period_q)[:4]  # YYYYMM 형식에서 YYYY 추출
            quarter = str(row.period_q)[4:]  # 분기 정보

            # 해당 연도의 데이터가 없거나, 현재 분기가 더 큰 경우에만 업데이트
            if year not in yearly_data or quarter > str(yearly_data[year]["period_q"])[4:]:
                yearly_data[year] = {
                    field_name: self._to_decimal(value)
                    for field_name, value in zip(row._fields, row)
                    if field_name not in ["Code", "Name", "StmtDt"]
                }

        # 연도별 데이터를 FinPosDetail 객체로 변환
        yearly_statements = []
        for year in sorted(yearly_data.keys(), reverse=True):
            # period_q를 연도로 설정
            yearly_data[year]["period_q"] = year

            # FinPosDetail 객체 생성
            yearly_statement = self._create_finpos_detail(yearly_data[year])
            yearly_statements.append(yearly_statement)

        # 최근 10년치 데이터만 사용
        yearly_statements = yearly_statements[:10]

        # 사용자 구독 레벨에 따른 접근 제어 적용
        if user:
            yearly_statements = self._apply_subscription_based_access(yearly_statements, user, is_yearly=True)

        return yearly_statements

    ########################################## 결과 처리 메서드 #########################################
    # 분기 실적
    async def _process_income_performance_quarterly_result(
        self, result, ticker, ctry, sector_tickers, all_shares, user: Optional[AlphafinderUser] = None
    ) -> List[QuarterlyIncome]:
        if not result:
            return []

        tickers = sector_tickers
        company_shares = all_shares.get(ticker, 0)

        # 회사 데이터와 섹터 데이터 분리
        company_data = defaultdict(dict)
        sector_data = defaultdict(lambda: defaultdict(list))

        for row in result:
            row_ticker = row[0]
            # 국가별 티커 처리
            if ctry == "USA":
                clean_row_ticker = row_ticker.replace("-US", "")
                shares_ticker = row_ticker
                multiplier = 1000
            else:
                clean_row_ticker = row_ticker
                shares_ticker = row_ticker
                multiplier = 100000000

            period = row[2]

            if row_ticker == ticker:
                company_data[period] = {
                    "rev": float(row[4]) if row[4] is not None else 0.0,
                    "operating_income": float(row[9]) if row[9] is not None else 0.0,
                    "net_income_total": float(row[18]) if row[18] is not None else 0.0,
                }

            if clean_row_ticker in tickers:
                shares = all_shares.get(shares_ticker, 0)
                net_income = float(row[18]) if row[18] is not None else 0.0
                # 국가별 단위에 맞춰 EPS 계산
                eps = (net_income * multiplier) / shares if shares > 0 else 0.0

                sector_data[period]["rev"].append(float(row[4]) if row[4] is not None else 0.0)
                sector_data[period]["operating_income"].append(float(row[9]) if row[9] is not None else 0.0)
                sector_data[period]["net_income_total"].append(net_income)
                sector_data[period]["eps"].append(eps)

        # 섹터 평균 계산
        sector_averages = {}
        for period, values in sector_data.items():
            if values["rev"] or values["operating_income"] or values["net_income_total"]:
                sector_averages[period] = {
                    "rev": statistics.mean(values["rev"]) if values["rev"] else 0.0,
                    "operating_income": statistics.mean(values["operating_income"])
                    if values["operating_income"]
                    else 0.0,
                    "net_income_total": statistics.mean(values["net_income_total"])
                    if values["net_income_total"]
                    else 0.0,
                    "eps": statistics.mean(values["eps"])  # 각 회사 EPS의 평균 계산
                    if values["eps"]
                    else 0.0,
                }

        quarterly_results = []
        for period in sorted(company_data.keys(), reverse=True):
            company_values = company_data[period]

            # 회사의 EPS 계산도 국가별 단위에 맞춤
            multiplier = 100000000 if ctry != "USA" else 1000
            eps_company = (
                (company_values["net_income_total"] * multiplier) / company_shares if company_shares > 0 else 0.0
            )

            quarterly_income = QuarterlyIncome(
                period_q=period,
                rev=IncomeMetric(
                    company=Decimal(str(company_values["rev"])),
                    industry_avg=Decimal(str(sector_averages.get(period, {}).get("rev", 0.0))),
                ),
                operating_income=IncomeMetric(
                    company=Decimal(str(company_values["operating_income"])),
                    industry_avg=Decimal(str(sector_averages.get(period, {}).get("operating_income", 0.0))),
                ),
                net_income=IncomeMetric(
                    company=Decimal(str(company_values["net_income_total"])),
                    industry_avg=Decimal(str(sector_averages.get(period, {}).get("net_income_total", 0.0))),
                ),
                eps=IncomeMetric(
                    company=Decimal(str(eps_company)),
                    industry_avg=Decimal(str(sector_averages.get(period, {}).get("eps", 0.0))),
                ),
            )
            quarterly_results.append(quarterly_income)

        # 최근 10분기 데이터만 사용
        quarterly_results = quarterly_results[:10]

        # 사용자 구독 레벨에 따른 접근 제어 적용
        if user:
            quarterly_results = self._apply_subscription_based_access(quarterly_results, user, is_yearly=False)

        return quarterly_results

    # 연간 실적
    async def _process_income_performance_yearly_result(
        self, result, ticker, ctry, sector_tickers, all_shares, user: Optional[AlphafinderUser] = None
    ) -> List[QuarterlyIncome]:
        if not result:
            return []

        # count 키 추가
        company_data = defaultdict(
            lambda: {
                "rev": 0.0,
                "operating_income": 0.0,
                "net_income_total": 0.0,
                "count": 0,
            }
        )

        for row in result:
            year = row[2][:4]
            if row[0] == ticker:
                company_data[year]["rev"] += float(row[4]) if row[4] is not None else 0.0
                company_data[year]["operating_income"] += float(row[9]) if row[9] is not None else 0.0
                company_data[year]["net_income_total"] += float(row[18]) if row[18] is not None else 0.0
                company_data[year]["count"] += 1

        # shares 조회 결과 확인
        tickers = sector_tickers
        company_shares = all_shares.get(ticker, 0)

        # 회사 데이터와 섹터 데이터 분리
        sector_data = defaultdict(lambda: defaultdict(list))

        for row in result:
            row_ticker = row[0]
            # 국가별 티커 처리
            if ctry == "USA":
                clean_row_ticker = row_ticker.replace("-US", "")
                shares_ticker = row_ticker
                multiplier = 1000  # 백만 달러 -> 천 달러
            else:
                clean_row_ticker = row_ticker
                shares_ticker = row_ticker
                multiplier = 100000000  # 억원 -> 원

            year = row[2][:4]  # 연도만 추출

            # 섹터 데이터 수집 (중복 제거)
            if clean_row_ticker in tickers:
                shares = all_shares.get(shares_ticker, 0)
                net_income = float(row[18]) if row[18] is not None else 0.0
                eps = (net_income * multiplier) / shares if shares > 0 else 0.0

                sector_data[year]["rev"].append(float(row[4]) if row[4] is not None else 0.0)
                sector_data[year]["operating_income"].append(float(row[9]) if row[9] is not None else 0.0)
                sector_data[year]["net_income_total"].append(net_income)
                sector_data[year]["eps"].append(eps)

        # 섹터 평균 계산
        sector_averages = {}
        for year, values in sector_data.items():
            if values["rev"] or values["operating_income"] or values["net_income_total"]:
                sector_averages[year] = {
                    "rev": statistics.mean(values["rev"]) if values["rev"] else 0.0,
                    "operating_income": statistics.mean(values["operating_income"])
                    if values["operating_income"]
                    else 0.0,
                    "net_income_total": statistics.mean(values["net_income_total"])
                    if values["net_income_total"]
                    else 0.0,
                    "eps": statistics.mean(values["eps"]) if values["eps"] else 0.0,
                }

        # 회사 데이터 연간 평균 계산
        for year, data in company_data.items():
            if data["count"] > 0:
                company_data[year]["rev"] /= data["count"]
                company_data[year]["operating_income"] /= data["count"]
                company_data[year]["net_income_total"] /= data["count"]

        yearly_results = []
        for year in sorted(company_data.keys(), reverse=True):
            company_values = company_data[year]

            # 국가별 단위에 맞춰 EPS 계산
            multiplier = 100000000 if ctry != "USA" else 1000
            eps_company = (
                (company_values["net_income_total"] * multiplier) / company_shares if company_shares > 0 else 0.0
            )

            yearly_income = QuarterlyIncome(
                period_q=year,
                rev=IncomeMetric(
                    company=Decimal(str(company_values["rev"])),
                    industry_avg=Decimal(str(sector_averages.get(year, {}).get("rev", 0.0))),
                ),
                operating_income=IncomeMetric(
                    company=Decimal(str(company_values["operating_income"])),
                    industry_avg=Decimal(str(sector_averages.get(year, {}).get("operating_income", 0.0))),
                ),
                net_income=IncomeMetric(
                    company=Decimal(str(company_values["net_income_total"])),
                    industry_avg=Decimal(str(sector_averages.get(year, {}).get("net_income_total", 0.0))),
                ),
                eps=IncomeMetric(
                    company=Decimal(str(eps_company)),
                    industry_avg=Decimal(str(sector_averages.get(year, {}).get("eps", 0.0))),
                ),
            )
            yearly_results.append(yearly_income)

        # 최근 10년치 데이터만 사용
        yearly_results = yearly_results[:10]

        # 사용자 구독 레벨에 따른 접근 제어 적용
        if user:
            yearly_results = self._apply_subscription_based_access(yearly_results, user, is_yearly=True)

        return yearly_results

    async def get_all_shares_by_tickers(self, tickers: List[str], country: str) -> Dict[str, float]:
        """한 번에 모든 티커의 shares 조회"""
        try:
            if country == "USA":
                tickers = [ticker + "-US" for ticker in tickers]

            country_enum = FinancialCountry(country)
            table_name = f"{country_enum.value}_stock_factors"

            # 기존 _select 메서드 사용, in 조건으로 한 번에 조회
            result = self.db._select(
                table=table_name,
                ticker__in=tickers,
                limit=len(tickers),  # 각 티커당 최신 데이터 1개씩
            )

            SHARED_OUTSTANDING_INDEX = 2
            shares_dict = {}

            for row in result:
                ticker = row[0]  # ticker는 첫 번째 컬럼
                shares_value = row[SHARED_OUTSTANDING_INDEX]
                shares_dict[ticker] = float(shares_value) if shares_value is not None else 0.0

            return shares_dict

        except Exception as e:
            logger.error(f"Error getting shares for multiple tickers: {e}")
            print(f"에러 발생: {str(e)}")
            return {}

    # 손익계산서
    def _process_income_statement_result(
        self, result, exclude_columns=["Code", "Name", "StmtDt"], user: Optional[AlphafinderUser] = None
    ) -> List[IncomeStatementDetail]:
        """
        손익계산 결과 처리
        """
        if not result:
            return []

        statements = []
        for row in result:
            # 제외할 컬럼들의 인덱스를 제외한 데이터만 사용
            row_dict = {col: val for col, val in zip(row._fields, row) if col not in exclude_columns}
            statements.append(self._create_income_statement_detail(row_dict))

        # 가장 최근 40분기(10년) 데이터만 사용
        statements = statements[:40]

        # 사용자 구독 레벨에 따른 접근 제어 적용
        if user:
            statements = self._apply_subscription_based_access(statements, user, is_yearly=False)

        return statements

    # 현금흐름표
    def _process_cashflow_result(
        self, result, exclude_columns=["Code", "Name", "StmtDt"], user: Optional[AlphafinderUser] = None
    ) -> List[CashFlowDetail]:
        """
        현금흐름 결과 처리
        """
        if not result:
            return []

        statements = []
        for row in result:
            # 제외할 컬럼들의 인덱스를 제외한 데이터만 사용
            row_dict = {col: val for col, val in zip(row._fields, row) if col not in exclude_columns}
            statements.append(self._create_cashflow_detail(row_dict))

        # 가장 최근 40분기(10년) 데이터만 사용
        statements = statements[:40]

        # 사용자 구독 레벨에 따른 접근 제어 적용
        if user:
            statements = self._apply_subscription_based_access(statements, user, is_yearly=False)

        return statements

    # 재무상태표
    def _process_finpos_result(
        self, result, exclude_columns=["Code", "Name", "StmtDt"], user: Optional[AlphafinderUser] = None
    ) -> List[FinPosDetail]:
        """
        재무상태표 결과 처리
        """
        if not result:
            return []

        statements = []
        for row in result:
            # 제외할 컬럼들의 인덱스를 제외한 데이터만 사용
            row_dict = {col: val for col, val in zip(row._fields, row) if col not in exclude_columns}
            statements.append(self._create_finpos_detail(row_dict))

        # 가장 최근 40분기(10년) 데이터만 사용
        statements = statements[:40]

        # 사용자 구독 레벨에 따른 접근 제어 적용
        if user:
            statements = self._apply_subscription_based_access(statements, user, is_yearly=False)

        return statements

    ########################################## 데이터 생성 메서드 #########################################
    # 실적
    def _create_comprehensive_income_statement(self, row_dict: Dict) -> QuarterlyIncome:
        """
        모든 실적 정보를 포함하는 통합 Statement 생성
        """
        values = {}
        for field_name, value in row_dict.items():
            if field_name == "period_q":
                values[field_name] = str(value)
            else:
                try:
                    values[field_name] = self._to_decimal(value)
                except Exception as e:
                    logger.warning(f"Error converting {field_name}: {str(e)}")
                    values[field_name] = Decimal("0")

        return QuarterlyIncome(**values)

    # 손익계산서
    def _create_income_statement_detail(self, row_dict: Dict) -> IncomeStatementDetail:
        """
        손익계산서 상세 정보 생성
        """
        # 컬럼명 매핑
        field_mapping = {"equity method gain": "equity_method_gain"}

        values = {}
        for field_name, value in row_dict.items():
            # period_q는 Decimal에서 str로 변환
            if field_name == "period_q":
                values[field_name] = str(value)

            elif field_name in field_mapping:
                values[field_mapping[field_name]] = self._to_decimal(value)
            else:
                values[field_name] = self._to_decimal(value)

        return IncomeStatementDetail(**values)

    # 현금흐름표
    def _create_cashflow_detail(self, row_dict: Dict) -> CashFlowDetail:
        """
        현금흐름 상세 정보 생성
        """
        values = {}
        for field_name, value in row_dict.items():
            if field_name == "period_q":
                values[field_name] = str(value)
            else:
                values[field_name] = self._to_decimal(value)

        return CashFlowDetail(**values)

    # 재무상태표
    def _create_finpos_detail(self, row_dict: Dict) -> FinPosDetail:
        """
        재무상태표 상세 정보 생성
        """
        values = {}
        for field_name, value in row_dict.items():
            if field_name == "period_q":
                values[field_name] = str(value)
            else:
                values[field_name] = self._to_decimal(value)

        return FinPosDetail(**values)

    def _round_and_clean(self, value: Union[float, Decimal]) -> Union[float, int]:
        """
        소수점 첫째자리에서 반올림하고, 소수점이 0이면 정수로 변환
        Decimal과 float 타입 모두 처리
        """
        # Decimal을 float로 변환
        if isinstance(value, Decimal):
            value = float(value)

        rounded = round(value, 1)
        # float의 is_integer() 메서드 사용
        return int(rounded) if float(rounded).is_integer() else rounded

    def _apply_subscription_based_access(self, data_list, user: AlphafinderUser, is_yearly: bool = False):
        """
        사용자 구독 레벨에 따라 데이터 접근 제어를 적용
        level 3: 10년 전체 데이터 제공
        level 1: 5년치 데이터만 제공 (나머지는 period_q만 제공하고 데이터는 마스킹)

        Args:
            data_list: 처리할 데이터 리스트
            user: 사용자 정보
            is_yearly: 연간 데이터 여부 (True: 연간, False: 분기)

        Returns:
            처리된 데이터 리스트
        """
        from app.modules.financial.schemas import (
            CashFlowDetail,
            FinPosDetail,
            IncomeMetric,
            IncomeStatementDetail,
            QuarterlyIncome,
        )

        if not data_list:
            return []

        # 구독 레벨 확인
        subscribed_level = getattr(user, "subscription_level", 1)  # 기본값은 1

        # 레벨 3 이상인 경우 전체 데이터 제공
        if subscribed_level >= 3:
            return data_list

        # 레벨 1인 경우 최근 5년치 데이터만 제공

        # 연간 데이터와 분기 데이터의 처리 방식 차이
        years_to_show = 5
        periods_to_show = years_to_show * (1 if is_yearly else 4)  # 연간은 5년, 분기는 20분기

        # 결과 리스트 초기화
        result = []

        # 모든 데이터의 period_q를 유지하되, 접근 가능한 기간 이외의 데이터는 마스킹
        for i, item in enumerate(data_list):
            if i < periods_to_show:
                # 접근 가능한 기간의 데이터는 그대로 추가
                result.append(item)
            else:
                # 접근 불가능한 기간의 데이터는 period_q만 유지하고 나머지는 마스킹
                if hasattr(item, "rev") and hasattr(item.rev, "company") and hasattr(item.rev, "industry_avg"):
                    # QuarterlyIncome 타입 (실적 데이터)

                    # 원본 period_q 유지
                    period_q = item.period_q

                    # 새 IncomeMetric 객체들 생성 (company는 None으로, industry_avg는 유지)
                    masked_rev = IncomeMetric(company=None, industry_avg=item.rev.industry_avg)
                    masked_operating_income = IncomeMetric(company=None, industry_avg=item.operating_income.industry_avg)
                    masked_net_income = IncomeMetric(company=None, industry_avg=item.net_income.industry_avg)
                    masked_eps = IncomeMetric(company=None, industry_avg=item.eps.industry_avg)

                    # 새로운 QuarterlyIncome 객체 생성
                    masked_item = QuarterlyIncome(
                        period_q=period_q,
                        rev=masked_rev,
                        operating_income=masked_operating_income,
                        net_income=masked_net_income,
                        eps=masked_eps,
                    )

                    result.append(masked_item)
                else:
                    # IncomeStatementDetail, CashFlowDetail, FinPosDetail 타입 (재무제표 데이터)
                    # 모델 타입에 따라 적절한 클래스 결정

                    if isinstance(item, IncomeStatementDetail):
                        model_class = IncomeStatementDetail
                    elif isinstance(item, CashFlowDetail):
                        model_class = CashFlowDetail
                    elif isinstance(item, FinPosDetail):
                        model_class = FinPosDetail
                    else:
                        # 일치하는 모델 클래스를 찾을 수 없으면 원본 반환
                        result.append(item)
                        continue

                    # period_q만 유지하고 나머지 필드는 None으로 설정
                    masked_data = {"period_q": item.period_q}

                    # 새 모델 인스턴스 생성
                    masked_item = model_class(**masked_data)
                    result.append(masked_item)

        return result


def get_financial_service(common_service: CommonService = Depends(get_common_service)) -> FinancialService:
    return FinancialService(common_service=common_service)
