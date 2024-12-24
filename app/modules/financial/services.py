from collections import defaultdict
import statistics
from requests import Session
from sqlalchemy import select
from app.core.logging.config import get_logger
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, Dict, List
from fastapi import HTTPException, Depends
import math
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.crud import database
from app.database.conn import db
from app.models.models_stock import StockInformation
from app.modules.common.enum import FinancialCountry
from app.modules.common.services import CommonService, get_common_service
from app.modules.financial.crud import FinancialCrud
from app.modules.financial.schemas import (
    CashFlowResponse,
    FinPosDetail,
    FinPosResponse,
    DebtRatioResponse,
    FinancialRatioResponse,
    IncomePerformanceResponse,
    IncomeStatementDetail,
    CashFlowDetail,
    IncomeStatementResponse,
    InterestCoverageRatioResponse,
    LiquidityRatioResponse,
    QuarterlyIncome,
    IncomeMetric,
)
from app.modules.common.schemas import BaseResponse
from app.core.exception.custom import DataNotFoundException, InvalidCountryException, AnalysisException
from app.modules.common.utils import contry_mapping

logger = get_logger(__name__)


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

    def _get_date_conditions(self, start_date: Optional[str], end_date: Optional[str]) -> Dict:
        """
        날짜 조건 생성
        start_date (Optional[str]): YYYYMM 형식의 시작일
        end_date (Optional[str]): YYYYMM 형식의 종료일
        기본값은 10분기/10년치 데이터를 조회
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

    def _to_decimal(self, value) -> Decimal:
        """
        값을 Decimal로 변환하고 JSON 직렬화 가능한 값으로 처리
        소수점 2자리까지 반올림
        """
        try:
            if value is None or (isinstance(value, str) and not value.strip()):
                return Decimal("0.00")
            if isinstance(value, (float, Decimal)):
                if isinstance(value, float) and math.isnan(value):
                    return Decimal("0.00")
                if isinstance(value, Decimal) and value.is_nan():
                    return Decimal("0.00")
                if isinstance(value, float) and math.isinf(value):
                    return Decimal("0.00")
                if isinstance(value, Decimal) and value.is_infinite():
                    return Decimal("0.00")

            # 값을 Decimal로 변환하고 소수점 2자리로 반올림
            return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        except (ValueError, TypeError, InvalidOperation):
            logger.warning(f"Failed to convert value to Decimal: {value}")
            return Decimal("0.00")

    ########################################## Router에서 호출하는 메서드 #########################################
    # 실적 데이터 조회
    def get_income_performance_data(
        self,
        ctry: str,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        db: Session = Depends(db.get_db),
    ) -> BaseResponse[IncomePerformanceResponse]:
        """
        실적 데이터 조회
        """
        if ticker:
            if ctry == "KOR":
                ticker = ticker

        try:
            if ctry == "USA":
                ticker = f"{ticker}-US"
            country = FinancialCountry(ctry)
            table_name = self.income_tables.get(country)
            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException()

            # 섹터 정보 조회
            sector = self.get_sector_by_ticker(ticker)
            tickers = self.get_ticker_by_sector(sector)

            tickers_with_suffix = [f"{t}-US" for t in tickers]  # 각 티커에 -US 접미사 추가

            conditions = {
                "Code__in": tickers_with_suffix,  # 수정된 티커 리스트 사용
                **self._get_date_conditions(start_date, end_date),
            }

            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                logger.warning(f"No income performance data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="실적")

            quarterly_statements = self._process_income_performance_quarterly_result(result, sector, ticker, ctry)
            yearly_statements = self._process_income_performance_yearly_result(result, sector, ticker, ctry)

            # DB 결과에서 직접 이름 추출
            company_name = self.get_kr_name_by_ticker(db=db, ticker=ticker)

            ctry = contry_mapping.get(ctry)

            performance_response = IncomePerformanceResponse(
                code=ticker,
                name=company_name,
                ctry=ctry,
                sector=sector,
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
    ) -> BaseResponse[IncomeStatementResponse]:
        """
        손익계산서 시계열 분석
        """
        logger.info(f"Starting income analysis for {ticker}")

        try:
            income_data = self.get_income_data(ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date)

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
    ) -> BaseResponse[CashFlowResponse]:
        """
        현금흐름 시계열 분석
        """
        try:
            cashflow_data = self.get_cashflow_data(ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date)
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
    ) -> BaseResponse[FinPosResponse]:
        """
        재무상태표 시계열 분석
        """
        try:
            finpos_data = self.get_finpos_data(ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date)

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
    ) -> BaseResponse[IncomeStatementResponse]:
        """
        손익계산서 데이터 조회
        """
        try:
            table_name = self.income_tables.get(ctry)
            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException(country=ctry)

            # USA 기업인 경우 티커에 -US 접미사 추가
            db_ticker = f"{ticker}-US" if ctry == FinancialCountry.USA else ticker
            conditions = {"Code": db_ticker, **self._get_date_conditions(start_date, end_date)}

            logger.debug(f"Querying income data for {ticker} with conditions: {conditions}")
            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                logger.warning(f"No income data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="손익계산")

            # DB 결과에서 직접 이름 추출
            name = result[0][1] if result else ""  # result[0][1]은 Name 컬럼의 값

            statements = self._process_income_statement_result(result)
            ttm = self._process_income_ttm_result(result)

            # 3자리 대문자를 2자리 소문자로 변환
            ctry_code = contry_mapping.get(ctry.value, "").lower()  # KOR -> kr

            # IncomeStatementResponse 객체 생성 시 2자리 소문자 국가 코드 사용
            income_statement_response = IncomeStatementResponse(
                code=ticker,
                name=name,
                ctry=ctry_code,  # 2자리 소문자 국가 코드
                ttm=ttm,
                details=statements,
            )

            # BaseResponse 생성
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
    ) -> BaseResponse[CashFlowResponse]:
        """
        현금흐름표 데이터 조회
        """
        try:
            table_name = self.cashflow_tables.get(ctry)
            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException(country=ctry)

            # USA 기업인 경우 티커에 -US 접미사 추가
            db_ticker = f"{ticker}-US" if ctry == FinancialCountry.USA else ticker
            conditions = {"Code": db_ticker, **self._get_date_conditions(start_date, end_date)}

            logger.debug(f"Querying cashflow data for {ticker} with conditions: {conditions}")
            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                logger.warning(f"No cashflow data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="현금흐름")

            statements = self._process_cashflow_result(result)
            ttm = self._process_cashflow_ttm_result(result)

            # DB 결과에서 직접 이름 추출
            name = result[0][1] if result else ""

            ctry_code = contry_mapping.get(ctry.value, "").lower()  # KOR -> kr
            cashflow_response = CashFlowResponse(code=ticker, name=name, ctry=ctry_code, ttm=ttm, details=statements)

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
    ) -> BaseResponse[FinPosResponse]:
        """
        재무상태표 데이터 조회
        """
        try:
            table_name = self.finpos_tables.get(ctry)
            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException(country=ctry)

            # USA 기업인 경우 티커에 -US 접미사 추가
            db_ticker = f"{ticker}-US" if ctry == FinancialCountry.USA else ticker
            conditions = {"Code": db_ticker, **self._get_date_conditions(start_date, end_date)}

            logger.debug(f"Querying finpos data for {ticker} with conditions: {conditions}")
            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                logger.warning(f"No finpos data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="재무상태")

            statements = self._process_finpos_result(result)
            ttm = self._process_finpos_ttm_result(result)

            # DB 결과에서 직접 이름 추출
            name = result[0][1] if result else ""

            ctry_code = contry_mapping.get(ctry.value, "").lower()  # KOR -> kr
            finpos_response = FinPosResponse(code=ticker, name=name, ctry=ctry_code, ttm=ttm, details=statements)

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

    @staticmethod
    def get_kr_name_by_ticker(db: Session, ticker: str) -> Optional[str]:
        """
        ticker로 StockInformation 테이블에서 한글로 된 기업이름 조회

        Args:
            db (AsyncSession): 데이터베이스 세션
            ticker (str): 종목 코드

        Returns:
        """
        if ticker.endswith("-US"):
            ticker = ticker[:-3]

        query = select(StockInformation.kr_name).where(StockInformation.ticker == ticker)
        result = db.execute(query)
        kr_name = result.scalar_one_or_none()

        return kr_name

    # 섹터 조회
    def get_sector_by_ticker(self, ticker: str) -> Optional[str]:
        """
        종목 섹터 조회
        """
        if ticker.endswith("-US"):
            ticker = ticker[:-3]

        query = select(StockInformation.sector_3).where(StockInformation.ticker == ticker)
        result = self.db._execute(query)
        sector = result.scalar_one_or_none()
        return sector

    # 섹터 내 종목 조회
    def get_ticker_by_sector(self, sector: str) -> List[str]:
        """
        섹터 내 종목 조회
        """
        query = select(StockInformation.ticker).where(StockInformation.sector_3 == sector)
        result = self.db._execute(query)
        tickers = result.scalars().all()
        return tickers

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
        average_debt_ratio = round(sum(debt_ratios) / len(debt_ratios), 2)
        industry_avg = self.get_financial_industry_avg(country=country, ticker=ticker, db=db)

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
            logger.warning(f"부채비율 데이터를 찾을 수 없습니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="부채비율")

        if len(quarters) < 4:
            logger.warning(f"4분기 데이터가 부족합니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="부채비율(4분기)")

        # 벡터화된 계산
        debt_ratios = [
            float((self._to_decimal(q.total_dept) / self._to_decimal(q.total_asset)) * 100)
            if self._to_decimal(q.total_asset) != 0
            else 0.0
            for q in quarters
        ]

        # 병렬 처리: 평균 계산과 산업 평균 조회를 동시에
        average_debt_ratio = round(sum(debt_ratios) / len(debt_ratios), 2)
        industry_avg = self.get_debt_ratio_industry_avg(country=country, ticker=ticker, db=db)

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
            logger.warning(f"유동비율 데이터를 찾을 수 없습니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="유동비율")

        if len(quarters) < 4:
            logger.warning(f"4분기 데이터가 부족합니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="유동비율(4분기)")

        # 벡터화된 계산
        liquidity_ratios = [
            float((self._to_decimal(q.current_asset) / self._to_decimal(q.current_dept)) * 100)
            if self._to_decimal(q.current_dept) != 0
            else 0.0
            for q in quarters
        ]

        # 병렬 처리: 평균 계산과 산업 평균 조회를 동시에
        average_liquidity_ratio = round(sum(liquidity_ratios) / len(liquidity_ratios), 2)
        industry_avg = self.get_liquidity_industry_avg(country=country, ticker=ticker, db=db)

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
            logger.warning(f"이자보상배율 데이터를 찾을 수 없습니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="이자보상배율")

        if len(quarters) < 4:
            logger.warning(f"4분기 데이터가 부족합니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="이자보상배율(4분기)")

        interest_coverage_ratios = [
            float(self._to_decimal(q.operating_income) / self._to_decimal(q.fin_cost))
            if self._to_decimal(q.fin_cost) != 0
            else 0.0
            for q in quarters
        ]

        average_ratio = round(sum(interest_coverage_ratios) / len(interest_coverage_ratios), 2)
        industry_avg = self.get_interest_coverage_industry_avg(country=country, ticker=ticker, db=db)

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

        return self.financial_crud.get_financial_industry_avg_data(
            table_name=table_name,
            base_ticker=ticker.replace("-US", "") if country == FinancialCountry.USA else ticker,
            is_usa=country == FinancialCountry.USA,
            ratio_type="debt",
            db=db,
        )

    # 부채비율 업종 평균 조회
    def get_debt_ratio_industry_avg(self, country: FinancialCountry, ticker: str, db: AsyncSession) -> float:
        """업종 평균 부채비율 조회"""
        table_name = self.finpos_tables.get(country)
        if not table_name:
            return 0.0
        return self.financial_crud.get_financial_industry_avg_data(
            table_name=table_name,
            base_ticker=ticker.replace("-US", "") if country == FinancialCountry.USA else ticker,
            is_usa=country == FinancialCountry.USA,
            ratio_type="debt",
            db=db,
        )

    def get_liquidity_industry_avg(self, country: FinancialCountry, ticker: str, db: Session) -> float:
        """업종 평균 유동비율 조회"""
        table_name = self.finpos_tables.get(country)
        if not table_name:
            return 0.0

        return self.financial_crud.get_financial_industry_avg_data(
            table_name=table_name,
            base_ticker=ticker.replace("-US", "") if country == FinancialCountry.USA else ticker,
            is_usa=country == FinancialCountry.USA,
            ratio_type="liquidity",
            db=db,
        )

    def get_interest_coverage_industry_avg(self, country: FinancialCountry, ticker: str, db: Session) -> float:
        """업종 평균 이자보상배율 조회"""
        table_name = self.income_tables.get(country)
        if not table_name:
            return 0.0

        return self.financial_crud.get_financial_industry_avg_data(
            table_name=table_name,
            base_ticker=ticker.replace("-US", "") if country == FinancialCountry.USA else ticker,
            is_usa=country == FinancialCountry.USA,
            ratio_type="interest",
            db=db,
        )

    ########################################## ttm 메서드 #########################################
    # 손익계산서 ttm
    def _process_income_ttm_result(self, result) -> IncomeStatementDetail:
        """
        손익계산서 ttm 결과 처리 - 모든 재무 항목에 대해 최근 12개월 합산
        """
        if not result:
            return IncomeStatementDetail()

        # 최근 12개월 데이터 선택
        recent_12_months = result[-12:]

        # 첫 번째 row에서 컬럼 추출 (exclude_columns 제외)
        exclude_columns = ["Code", "Name", "StmtDt"]  # period_q는 제외하지 않음
        first_row = recent_12_months[0]

        # TTM 계산을 위한 딕셔너리 초기화
        ttm_dict = {
            col: sum(self._to_decimal(getattr(row, col, 0)) for row in recent_12_months)
            for col, val in zip(first_row._fields, first_row)
            if col not in exclude_columns and col != "period_q"
        }

        # TTM 값에는 'TTM'이라고 표시
        ttm_dict["period_q"] = "TTM"

        return self._create_income_statement_detail(ttm_dict)

    # 현금흐름표 ttm
    def _process_cashflow_ttm_result(self, result) -> CashFlowDetail:
        """
        현금흐름표 ttm 결과 처리 - 모든 재무 항목에 대해 최근 12개월 합산
        """
        if not result:
            return CashFlowDetail()

        # 최근 12개월 데이터 선택
        recent_12_months = result[-12:]

        # 첫 번재 row에서 컬 추출
        exclude_columns = ["Code", "Name", "StmtDt"]
        first_row = recent_12_months[0]

        # TTM 계산을 위한 딕셔너리 초기화
        ttm_dict = {
            col: sum(self._to_decimal(getattr(row, col, 0)) for row in recent_12_months)
            for col, val in zip(first_row._fields, first_row)
            if col not in exclude_columns and col != "period_q"
        }

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

        # 최근 12개월 데이터 선택
        recent_12_months = result[-12:]

        # 첫 번재 row에서 컬럼 추출
        exclude_columns = ["Code", "Name", "StmtDt"]
        first_row = recent_12_months[0]

        # TTM 계산을 위한 딕셔너리 초기화
        ttm_dict = {
            col: sum(self._to_decimal(getattr(row, col, 0)) for row in recent_12_months)
            for col, val in zip(first_row._fields, first_row)
            if col not in exclude_columns and col != "period_q"
        }

        # TTM 값에는 'TTM'이라고 표시
        ttm_dict["period_q"] = "TTM"

        return self._create_finpos_detail(ttm_dict)

    ########################################## 결과 처리 메서드 #########################################
    # 분기 실적
    def _process_income_performance_quarterly_result(self, result, sector, ticker, ctry) -> List[QuarterlyIncome]:
        if not result:
            return []

        tickers = self.get_ticker_by_sector(sector)
        shares = self.get_shares_by_ticker(ticker, ctry)

        # 회사 데이터와 섹터 데이터 분리
        company_data = defaultdict(dict)
        sector_data = defaultdict(lambda: defaultdict(list))

        for row in result:
            row_ticker = row[0]
            clean_row_ticker = row_ticker.replace("-US", "")
            period = row[2]

            if row_ticker == ticker:
                company_data[period] = {
                    "rev": float(row[4]) if row[4] is not None else 0.0,
                    "operating_income": float(row[9]) if row[9] is not None else 0.0,
                    "net_income": float(row[19]) if row[19] is not None else 0.0,
                }

            if clean_row_ticker in tickers:
                sector_data[period]["rev"].append(float(row[4]) if row[4] is not None else 0.0)
                sector_data[period]["operating_income"].append(float(row[9]) if row[9] is not None else 0.0)
                sector_data[period]["net_income"].append(float(row[19]) if row[19] is not None else 0.0)

        # 섹터 평균 계산
        sector_averages = {}
        for period, values in sector_data.items():
            if values["rev"] or values["operating_income"] or values["net_income"]:
                sector_averages[period] = {
                    "rev": statistics.mean(values["rev"]) if values["rev"] else 0.0,
                    "operating_income": statistics.mean(values["operating_income"])
                    if values["operating_income"]
                    else 0.0,
                    "net_income": statistics.mean(values["net_income"]) if values["net_income"] else 0.0,
                }

        quarterly_results = []
        for period in sorted(company_data.keys(), reverse=True):
            company_values = company_data[period]

            # EPS 계산 시에만 1000을 곱해서 천 단위로 변환
            eps_company = (company_values["net_income"] * 1000) / shares if shares > 0 else 0.0
            eps_industry = (
                (sector_averages[period]["net_income"] * 1000) / shares
                if period in sector_averages and shares > 0
                else 0.0
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
                    company=Decimal(str(company_values["net_income"])),
                    industry_avg=Decimal(str(sector_averages.get(period, {}).get("net_income", 0.0))),
                ),
                eps=IncomeMetric(company=Decimal(str(eps_company)), industry_avg=Decimal(str(eps_industry))),
            )
            quarterly_results.append(quarterly_income)

        return quarterly_results[:10]

    # 연간 실적
    def _process_income_performance_yearly_result(self, result, sector, ticker, ctry) -> List[QuarterlyIncome]:
        if not result:
            return []

        tickers = self.get_ticker_by_sector(sector)
        shares = self.get_shares_by_ticker(ticker, ctry)

        # 회사 데이터와 섹터 데이터 분리
        company_data = defaultdict(dict)
        sector_data = defaultdict(lambda: defaultdict(list))

        for row in result:
            row_ticker = row[0]
            clean_row_ticker = row_ticker.replace("-US", "")
            year = row[2][:4]  # 연도만 추출

            if row_ticker == ticker:
                if year not in company_data:
                    company_data[year] = {"rev": 0.0, "operating_income": 0.0, "net_income": 0.0, "count": 0}
                company_data[year]["rev"] += float(row[4]) if row[4] is not None else 0.0
                company_data[year]["operating_income"] += float(row[9]) if row[9] is not None else 0.0
                company_data[year]["net_income"] += float(row[19]) if row[19] is not None else 0.0
                company_data[year]["count"] += 1

            if clean_row_ticker in tickers:
                sector_data[year]["rev"].append(float(row[4]) if row[4] is not None else 0.0)
                sector_data[year]["operating_income"].append(float(row[9]) if row[9] is not None else 0.0)
                sector_data[year]["net_income"].append(float(row[19]) if row[19] is not None else 0.0)

        # 섹터 평균 계산
        sector_averages = {}
        for year, values in sector_data.items():
            if values["rev"] or values["operating_income"] or values["net_income"]:
                sector_averages[year] = {
                    "rev": statistics.mean(values["rev"]) if values["rev"] else 0.0,
                    "operating_income": statistics.mean(values["operating_income"])
                    if values["operating_income"]
                    else 0.0,
                    "net_income": statistics.mean(values["net_income"]) if values["net_income"] else 0.0,
                }

        # 회사 데이터 연간 평균 계산
        for year, data in company_data.items():
            if data["count"] > 0:
                company_data[year]["rev"] /= data["count"]
                company_data[year]["operating_income"] /= data["count"]
                company_data[year]["net_income"] /= data["count"]

        yearly_results = []
        for year in sorted(company_data.keys(), reverse=True):
            company_values = company_data[year]

            # EPS 계산 시에만 1000을 곱해서 천 단위로 변환
            eps_company = (company_values["net_income"] * 1000) / shares if shares > 0 else 0.0
            eps_industry = (
                (sector_averages[year]["net_income"] * 1000) / shares if year in sector_averages and shares > 0 else 0.0
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
                    company=Decimal(str(company_values["net_income"])),
                    industry_avg=Decimal(str(sector_averages.get(year, {}).get("net_income", 0.0))),
                ),
                eps=IncomeMetric(company=Decimal(str(eps_company)), industry_avg=Decimal(str(eps_industry))),
            )
            yearly_results.append(yearly_income)

        return yearly_results[:10]

    # 손익계산서
    def _process_income_statement_result(
        self, result, exclude_columns=["Code", "Name", "StmtDt"]
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

        return statements

    # 현금흐름표
    def _process_cashflow_result(self, result, exclude_columns=["Code", "Name", "StmtDt"]) -> List[CashFlowDetail]:
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

        return statements

    # 재무상태표
    def _process_finpos_result(self, result, exclude_columns=["Code", "Name", "StmtDt"]) -> List[FinPosDetail]:
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
            # 필드명 매핑 ��용
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


def get_financial_service(common_service: CommonService = Depends(get_common_service)) -> FinancialService:
    return FinancialService(common_service=common_service)
