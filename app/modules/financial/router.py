import csv
import io
from datetime import datetime
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from requests import Session

from app.core.exception.handler import exception_handler
from app.core.logger import setup_logger
from app.database.conn import db
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import FinancialCountry, TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.common.utils import async_check_ticker_country_len_3, check_ticker_country_len_3, contry_mapping
from app.modules.financial.services import FinancialService, get_financial_service
from app.modules.user.schemas import DataDownloadHistory
from app.modules.user.service import UserService, get_user_service
from app.utils.oauth_utils import get_current_user

from .schemas import CashFlowResponse, FinPosResponse, IncomePerformanceResponse, IncomeStatementResponse, RatioResponse

logger = setup_logger(__name__)
router = APIRouter()


@router.get(
    "/income-performance",
    response_model=BaseResponse[IncomePerformanceResponse],
    summary="실적 부분 조회 api",
)
async def get_income_performance_data(
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    lang: Annotated[TranslateCountry, Query(description="언어, 예시: KO, EN")] = TranslateCountry.KO,
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
    db: Session = Depends(db.get_db),
) -> BaseResponse[IncomePerformanceResponse]:
    try:
        ctry = await async_check_ticker_country_len_3(ticker)
        ctry = ctry.upper()
        return await financial_service.get_income_performance_data(
            ctry=ctry, ticker=ticker, lang=lang, start_date=start_date, end_date=end_date, db=db
        )
    except HTTPException as http_error:
        logger.error(
            f"Income performance data 조회 실패: {http_error.status_code}: {http_error.detail}, ticker: {ticker}, country: {ctry}"
        )
        raise http_error
    except Exception as error:
        logger.error(f"Income performance data 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        raise HTTPException(status_code=500, detail="내부 서버 오류")


@router.get(
    "/income",
    response_model=BaseResponse[IncomeStatementResponse],
    summary="손익계산서",
)
def get_income_analysis(
    request: Request,
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
    user: AlphafinderUser = Depends(get_current_user),
) -> BaseResponse[IncomeStatementResponse]:
    try:
        country_code = check_ticker_country_len_3(ticker).upper()
        ctry = FinancialCountry(country_code)
        result = financial_service.get_income_analysis(
            ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date, user=user
        )
        return result

    except Exception as error:
        logger.error(f"Income analysis 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        return exception_handler(request, error)


@router.get(
    "/cashflow",
    response_model=BaseResponse[CashFlowResponse],
    summary="현금흐름",
)
def get_cashflow_analysis(
    request: Request,
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
    user: AlphafinderUser = Depends(get_current_user),
) -> BaseResponse[CashFlowResponse]:
    try:
        country_code = check_ticker_country_len_3(ticker).upper()
        ctry = FinancialCountry(country_code)
        result = financial_service.get_cashflow_analysis(
            ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date, user=user
        )
        return result

    except Exception as error:
        logger.error(f"Cashflow analysis 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        return exception_handler(request, error)


@router.get(
    "/finpos",
    response_model=BaseResponse[FinPosResponse],
    summary="재무상태표",
)
def get_finpos_analysis(
    request: Request,
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    start_date: Annotated[Optional[str], Query(description="시작일자 (YYYYMM)")] = None,
    end_date: Annotated[Optional[str], Query(description="종료일자 (YYYYMM)")] = None,
    financial_service: FinancialService = Depends(get_financial_service),
    user: AlphafinderUser = Depends(get_current_user),
) -> BaseResponse[FinPosResponse]:
    try:
        country_code = check_ticker_country_len_3(ticker).upper()
        ctry = FinancialCountry(country_code)
        result = financial_service.get_finpos_analysis(
            ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date, user=user
        )

        return result

    except Exception as error:
        logger.error(f"Financial position analysis 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        return exception_handler(request, error)


@router.get(
    "/financial-ratio",
    response_model=BaseResponse[RatioResponse],
    summary="재무 api",
)
async def get_financial_ratio(
    request: Request,
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    lang: Annotated[TranslateCountry, Query(description="언어, 예시: KO, EN")] = TranslateCountry.KO,
    financial_service: FinancialService = Depends(get_financial_service),
    db: Session = Depends(db.get_db),
) -> BaseResponse[RatioResponse]:
    try:
        ctry = check_ticker_country_len_3(ticker).upper()
        company_name = await financial_service.get_name_by_ticker(ticker=ticker, lang=lang)
        dept_ratio = await financial_service.get_debt_ratio(ctry=ctry, ticker=ticker, db=db)
        liquidity_ratio = await financial_service.get_liquidity_ratio(ctry=ctry, ticker=ticker, db=db)
        interest_coverage_ratio = await financial_service.get_interest_coverage_ratio(ctry=ctry, ticker=ticker, db=db)
        ctry_two = contry_mapping.get(ctry)

        return BaseResponse[RatioResponse](
            status_code=200,
            message="재무 데이터를 성공적으로 조회했습니다.",
            data=RatioResponse(
                code=ticker,
                name=company_name,
                ctry=ctry_two,
                debt_ratios=dept_ratio.data,
                liquidity_ratios=liquidity_ratio.data,
                interest_coverage_ratios=interest_coverage_ratio.data,
            ),
        )

    except Exception as error:
        logger.error(f"Financial ratio 조회 실패: {str(error)}, ticker: {ticker}, country: {ctry}")
        return exception_handler(request, error)


@router.get(
    "/income/download",
    response_class=Response,
    summary="손익계산서 다운로드 api",
)
async def get_income_download(
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    financial_service: FinancialService = Depends(get_financial_service),
    user: AlphafinderUser = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
) -> Response:
    # 사용자 인증 확인
    if user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    # 멤버십 레벨 확인
    if user.subscription_level < 3:
        raise HTTPException(
            status_code=403, detail="멤버십이 낮아 다운로드가 불가능합니다. Pro 이상으로 업그레이드해주세요."
        )

    try:
        # 데이터 가져오기 (country code 필요)
        country_code = check_ticker_country_len_3(ticker).upper()
        ctry = FinancialCountry(country_code)

        # 손익계산서 데이터 가져오기
        income_data = financial_service.get_income_analysis(ctry=ctry, ticker=ticker, user=user)

        # 데이터가 비어있는지 확인
        if not income_data.data.details and not income_data.data.total:
            logger.error(f"Income statement data is empty for ticker: {ticker}")
            raise HTTPException(status_code=404, detail="데이터를 찾을 수 없습니다.")

        # None 값을 '-'로 변환하는 함수
        def none_to_dash(value):
            return "-" if value is None else value

        # CSV 파일 생성을 위한 StringIO 객체 초기화
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)

        # 모든 데이터 병합 (연간 + 분기별)
        all_data = income_data.data.total + income_data.data.details

        # 기간 목록 추출
        periods = set(item.period_q for item in all_data if item.period_q)

        # TTM(Trailing Twelve Months) 추가
        ttm_exists = False
        if income_data.data.ttm:
            ttm_exists = True

        # 기간을 연도와 분기로 정렬하기 위한 함수
        def sort_periods(period):
            # TTM은 가장 앞에 위치
            if period == "TTM":
                return (0, 0, 0)

            # 4자리는 연간 데이터 (예: 2024)
            if len(period) == 4:
                year = int(period)
                return (1, -year, 0)  # 연도 내림차순

            # 6자리는 분기 데이터 (예: 202412)
            if len(period) == 6:
                year = int(period[:4])
                quarter = int(period[4:])
                return (1, -year, -quarter)  # 연도와 분기 내림차순

            return (3, 0, 0)  # 기타 형식은 마지막에 배치

        # 기간 정렬
        ordered_periods = sorted(periods, key=sort_periods)

        # TTM이 있으면 맨 앞에 추가
        if ttm_exists and "TTM" not in ordered_periods:
            ordered_periods = ["TTM"] + ordered_periods

        # 항목 필드 정의 (손익계산서 항목들)
        # 필드 이름을 보기 좋게 매핑할 사전 정의
        field_mapping = {
            "period_q": "기간",
            "rev": "매출액",
            "cost_of_sales": "매출원가",
            "gross_profit": "매출총이익",
            "sell_admin_cost": "판매비와관리비",
            "rnd_expense": "연구개발비",
            "operating_income": "영업이익",
            "other_rev_gains": "기타영업수익",
            "other_exp_losses": "기타영업비용",
            "equity_method_gain": "지분법이익",
            "fin_profit": "금융수익",
            "fin_cost": "금융비용",
            "pbt": "법인세차감전순이익(손실)",
            "corp_tax_cost": "법인세비용",
            "profit_continuing_ops": "계속사업이익",
            "net_income": "당기순이익",
        }

        # 손익계산서 필드 목록 (field_mapping에서 period_q 제외하고 Code, Name, StmtDt 등 시스템 필드도 제외)
        income_fields = [
            "rev",
            "cost_of_sales",
            "gross_profit",
            "sell_admin_cost",
            "rnd_expense",
            "operating_income",
            "other_rev_gains",
            "other_exp_losses",
            "equity_method_gain",
            "fin_profit",
            "fin_cost",
            "pbt",
            "corp_tax_cost",
            "profit_continuing_ops",
            "net_income",
        ]

        # 헤더 작성 - 첫번째 열은 항목명, 나머지 열은 기간들
        header_row = ["항목"] + ordered_periods
        writer.writerow(header_row)

        # 각 필드별로 데이터 행 작성
        for field in income_fields:
            # 필드가 매핑에 있는 경우에만 처리
            if field in field_mapping:
                row = [field_mapping[field]]  # 첫번째 열은 항목 이름

                # 각 기간별 값 추가
                for period in ordered_periods:
                    if period == "TTM" and ttm_exists:
                        # TTM 데이터 추출
                        value = getattr(income_data.data.ttm, field, None)
                    else:
                        # 해당 기간의 데이터 찾기
                        period_data = next((item for item in all_data if item.period_q == period), None)
                        value = getattr(period_data, field, None) if period_data else None

                    row.append(none_to_dash(value))

                # 행 데이터 작성
                writer.writerow(row)

        # 파일 이름 생성
        file_name = f"{ticker}_income_statement.csv"

        # 응답 헤더 설정
        headers = {"Content-Disposition": f"attachment; filename={file_name}", "Content-Type": "text/csv"}

        # 데이터 다운로드 기록 저장
        data_download_history = DataDownloadHistory(
            user_id=user.id,
            data_type="income",
            data_detail=ticker,
            download_datetime=datetime.now(),
        )
        user_service.save_data_download_history(data_download_history)

        # 일반 응답으로 반환
        return Response(content=csv_buffer.getvalue().encode("utf-8-sig"), headers=headers)
    except Exception as error:
        logger.error(f"Income statement download 실패: {str(error)}, ticker: {ticker}")
        raise HTTPException(status_code=500, detail="내부 서버 오류")


@router.get(
    "/cashflow/download",
    response_class=Response,
    summary="현금흐름 다운로드 api",
)
async def get_cashflow_download(
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    financial_service: FinancialService = Depends(get_financial_service),
    user: AlphafinderUser = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
) -> Response:
    # 사용자 인증 확인
    if user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    # 멤버십 레벨 확인
    if user.subscription_level < 3:
        raise HTTPException(
            status_code=403, detail="멤버십이 낮아 다운로드가 불가능합니다. Pro 이상으로 업그레이드해주세요."
        )

    try:
        # 데이터 가져오기 (country code 필요)
        country_code = check_ticker_country_len_3(ticker).upper()
        ctry = FinancialCountry(country_code)

        # 현금흐름표 데이터 가져오기
        cashflow_data = financial_service.get_cashflow_analysis(ctry=ctry, ticker=ticker, user=user)

        # 데이터가 비어있는지 확인
        if not cashflow_data.data.details and not cashflow_data.data.total:
            logger.error(f"Cashflow data is empty for ticker: {ticker}")
            raise HTTPException(status_code=404, detail="데이터를 찾을 수 없습니다.")

        # None 값을 '-'로 변환하는 함수
        def none_to_dash(value):
            return "-" if value is None else value

        # CSV 파일 생성을 위한 StringIO 객체 초기화
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)

        # 모든 데이터 병합 (연간 + 분기별)
        all_data = cashflow_data.data.total + cashflow_data.data.details

        # 기간 목록 추출
        periods = set(item.period_q for item in all_data if item.period_q)

        # TTM(Trailing Twelve Months) 추가
        ttm_exists = False
        if cashflow_data.data.ttm:
            ttm_exists = True

        # 기간을 연도와 분기로 정렬하기 위한 함수
        def sort_periods(period):
            # TTM은 가장 앞에 위치
            if period == "TTM":
                return (0, 0, 0)

            # 4자리는 연간 데이터 (예: 2024)
            if len(period) == 4:
                year = int(period)
                return (1, -year, 0)  # 연도 내림차순

            # 6자리는 분기 데이터 (예: 202412)
            if len(period) == 6:
                year = int(period[:4])
                quarter = int(period[4:])
                return (1, -year, -quarter)  # 연도와 분기 내림차순

            return (3, 0, 0)  # 기타 형식은 마지막에 배치

        # 기간 정렬
        ordered_periods = sorted(periods, key=sort_periods)

        # TTM이 있으면 맨 앞에 추가
        if ttm_exists and "TTM" not in ordered_periods:
            ordered_periods = ["TTM"] + ordered_periods

        # 필드 이름을 보기 좋게 매핑할 사전 정의
        field_mapping = {
            "period_q": "기간",
            "operating_cashflow": "영업활동 현금흐름",
            "non_controlling_changes": "비지배지분 변동",
            "working_capital_changes": "운전자본 변동",
            "finance_cashflow": "재무활동 현금흐름",
            "dividends": "배당금",
            "investing_cashflow": "투자활동 현금흐름",
            "depreciation": "감가상각비",
            "free_cash_flow1": "FCF (영업-투자)",
            "free_cash_flow2": "FCF (잉여현금흐름)",
            "cash_earnings": "현금수익",
            "capex": "설비투자",
            "other_cash_flows": "기타현금흐름",
            "cash_increment": "현금증감",
        }

        # 현금흐름표 필드 목록 (field_mapping에서 period_q 제외)
        cashflow_fields = [
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

        # 헤더 작성 - 첫번째 열은 항목명, 나머지 열은 기간들
        header_row = ["항목"] + ordered_periods
        writer.writerow(header_row)

        # 각 필드별로 데이터 행 작성
        for field in cashflow_fields:
            # 필드가 매핑에 있는 경우에만 처리
            if field in field_mapping:
                row = [field_mapping[field]]  # 첫번째 열은 항목 이름

                # 각 기간별 값 추가
                for period in ordered_periods:
                    if period == "TTM" and ttm_exists:
                        # TTM 데이터 추출
                        value = getattr(cashflow_data.data.ttm, field, None)
                    else:
                        # 해당 기간의 데이터 찾기
                        period_data = next((item for item in all_data if item.period_q == period), None)
                        value = getattr(period_data, field, None) if period_data else None

                    row.append(none_to_dash(value))

                # 행 데이터 작성
                writer.writerow(row)

        # 파일 이름 생성
        file_name = f"{ticker}_cashflow_statement.csv"

        # 응답 헤더 설정
        headers = {"Content-Disposition": f"attachment; filename={file_name}", "Content-Type": "text/csv"}

        # 데이터 다운로드 기록 저장
        data_download_history = DataDownloadHistory(
            user_id=user.id,
            data_type="cashflow",
            data_detail=ticker,
            download_datetime=datetime.now(),
        )
        user_service.save_data_download_history(data_download_history)

        # 일반 응답으로 반환
        return Response(content=csv_buffer.getvalue().encode("utf-8-sig"), headers=headers)

    except Exception as error:
        logger.error(f"Cashflow download 실패: {str(error)}, ticker: {ticker}")
        raise HTTPException(status_code=500, detail="내부 서버 오류")


@router.get(
    "/finpos/download",
    response_class=Response,
    summary="재무상태표 다운로드 api",
)
async def get_finpos_download(
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    financial_service: FinancialService = Depends(get_financial_service),
    user: AlphafinderUser = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
) -> Response:
    # 사용자 인증 확인
    if user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    # 멤버십 레벨 확인
    if user.subscription_level < 3:
        raise HTTPException(
            status_code=403, detail="멤버십이 낮아 다운로드가 불가능합니다. Pro 이상으로 업그레이드해주세요."
        )

    try:
        # 데이터 가져오기 (country code 필요)
        country_code = check_ticker_country_len_3(ticker).upper()
        ctry = FinancialCountry(country_code)

        # 재무상태표 데이터 가져오기
        finpos_data = financial_service.get_finpos_analysis(ctry=ctry, ticker=ticker, user=user)

        # 데이터가 비어있는지 확인
        if not finpos_data.data.details and not finpos_data.data.total:
            logger.error(f"Financial position data is empty for ticker: {ticker}")
            raise HTTPException(status_code=404, detail="데이터를 찾을 수 없습니다.")

        # None 값을 '-'로 변환하는 함수
        def none_to_dash(value):
            return "-" if value is None else value

        # CSV 파일 생성을 위한 StringIO 객체 초기화
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)

        # 모든 데이터 병합 (연간 + 분기별)
        all_data = finpos_data.data.total + finpos_data.data.details

        # 기간 목록 추출
        periods = set(item.period_q for item in all_data if item.period_q)

        # TTM(Trailing Twelve Months) 추가
        ttm_exists = False
        if finpos_data.data.ttm:
            ttm_exists = True

        # 기간을 연도와 분기로 정렬하기 위한 함수
        def sort_periods(period):
            # TTM은 가장 앞에 위치
            if period == "TTM":
                return (0, 0, 0)

            # 4자리는 연간 데이터 (예: 2024)
            if len(period) == 4:
                year = int(period)
                return (1, -year, 0)  # 연도 내림차순

            # 6자리는 분기 데이터 (예: 202412)
            if len(period) == 6:
                year = int(period[:4])
                quarter = int(period[4:])
                return (1, -year, -quarter)  # 연도와 분기 내림차순

            return (3, 0, 0)  # 기타 형식은 마지막에 배치

        # 기간 정렬
        ordered_periods = sorted(periods, key=sort_periods)

        # TTM이 있으면 맨 앞에 추가
        if ttm_exists and "TTM" not in ordered_periods:
            ordered_periods = ["TTM"] + ordered_periods

        # 필드 이름을 보기 좋게 매핑할 사전 정의
        field_mapping = {
            "period_q": "기간",
            "total_asset": "자산총계",
            "current_asset": "유동자산",
            "stock_asset": "재고자산",
            "trade_and_other_receivables": "매출채권 및 기타채권",
            "cash_asset": "현금성자산",
            "assets_held_for_sale": "매각예정자산",
            "non_current_asset": "비유동자산",
            "tangible_asset": "유형자산",
            "intangible_asset": "무형자산",
            "investment_asset": "투자자산",
            "non_current_trade_and_other_receivables": "비유동 매출채권 및 기타채권",
            "deferred_tax_asset": "이연법인세자산",
            "extra_intangible": "영업권 등",
            "total_dept": "부채총계",
            "current_dept": "유동부채",
            "trade_and_other_payables": "매입채무 및 기타채무",
            "liabilities_held_for_sale": "매각예정부채",
            "non_current_liability": "비유동부채",
            "debenture": "사채",
            "non_current_trade_and_other_payables": "비유동 매입채무 및 기타채무",
            "deferred_tax_liability": "이연법인세부채",
            "equity": "자본",
            "total_equity": "자본총계",
            "controlling_equity": "지배기업주주지분",
            "capital": "자본금",
            "capital_surplus": "자본잉여금",
            "other_capital": "기타자본",
            "comp_income": "기타포괄손익누계액",
            "retained_earnings": "이익잉여금",
            "non_ctrl_shrhld_eq": "비지배지분",
        }

        # 재무상태표 필드 목록 (field_mapping에서 period_q 제외)
        finpos_fields = [
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
            "capital_surplus",
            "other_capital",
            "comp_income",
            "retained_earnings",
            "non_ctrl_shrhld_eq",
        ]

        # 헤더 작성 - 첫번째 열은 항목명, 나머지 열은 기간들
        header_row = ["항목"] + ordered_periods
        writer.writerow(header_row)

        # 각 필드별로 데이터 행 작성
        for field in finpos_fields:
            # 필드가 매핑에 있는 경우에만 처리
            if field in field_mapping:
                row = [field_mapping[field]]  # 첫번째 열은 항목 이름

                # 각 기간별 값 추가
                for period in ordered_periods:
                    if period == "TTM" and ttm_exists:
                        # TTM 데이터 추출
                        value = getattr(finpos_data.data.ttm, field, None)
                    else:
                        # 해당 기간의 데이터 찾기
                        period_data = next((item for item in all_data if item.period_q == period), None)
                        value = getattr(period_data, field, None) if period_data else None

                    row.append(none_to_dash(value))

                # 행 데이터 작성
                writer.writerow(row)

        # 파일 이름 생성
        file_name = f"{ticker}_financial_position.csv"

        # 응답 헤더 설정
        headers = {"Content-Disposition": f"attachment; filename={file_name}", "Content-Type": "text/csv"}

        # 데이터 다운로드 기록 저장
        data_download_history = DataDownloadHistory(
            user_id=user.id,
            data_type="finpos",
            data_detail=ticker,
            download_datetime=datetime.now(),
        )
        user_service.save_data_download_history(data_download_history)

        # 일반 응답으로 반환
        return Response(content=csv_buffer.getvalue().encode("utf-8-sig"), headers=headers)

    except Exception as error:
        logger.error(f"Financial position download 실패: {str(error)}, ticker: {ticker}")
        raise HTTPException(status_code=500, detail="내부 서버 오류")
