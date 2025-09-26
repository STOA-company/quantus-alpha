from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from collections import defaultdict

from app.common.constants import UTC
from app.core.exception.custom import DividendNotFoundException
from app.database.crud import database
from app.modules.common.enum import Country, FinancialCountry
from app.modules.common.utils import contry_mapping
from app.modules.dividend.v2.schemas import DividendItem, DividendDetail, DividendYearResponse


class DividendService:
    def __init__(self):
        self.db = database

    async def get_dividend_renewal(self, ctry: FinancialCountry, ticker: str) -> DividendItem:
        """배당 정보 조회 - 최근 7년치 데이터만 조회하여 성능 최적화"""
        current_year = datetime.now(UTC).year
        min_year = current_year - 6  # 최근 7년치 데이터만 조회
        
        # 최근 7년치 데이터만 쿼리 레벨에서 필터링
        condition = {
            "ticker": ticker,
            "payment_date__gte": f"{min_year}-01-01",  # 쿼리 레벨에서 필터링
        }

        # 데이터베이스에서 직접 조회 (Pandas 없이)
        dividend_data = await self.db._select_async(
            table="dividend_information",
            columns=["ticker", "payment_date", "ex_date", "per_share", "yield_rate"],
            order="ex_date",
            ascending=False,
            **condition,
        )

        # ticker가 없는 경우 체크
        if not dividend_data:
            return DividendItem(
                ticker=ticker,
                name="",
                ctry=ctry,
                last_year_dividend_count=0,
                last_year_dividend_date=[],
                last_dividend_per_share=None,
                last_dividend_ratio=None,
                last_dividend_growth_rate=None,
                detail=[],
            )
            # raise DividendNotFoundException(ticker=ticker, data_type="dividend")

        # 순수 Python으로 데이터 처리
        processed_data = self._process_dividend_data(dividend_data, current_year)
        
        # 배당성향 및 성장률 계산
        last_dividend_ratio = await self._calculate_dividend_ratio(processed_data, ctry, ticker)
        last_dividend_growth_rate = self._calculate_growth_rate(processed_data, current_year)

        return DividendItem(
            ticker=ticker,
            name="",
            ctry=ctry,
            last_year_dividend_count=processed_data["last_year_count"],
            last_year_dividend_date=processed_data["last_year_months"],
            last_dividend_per_share=processed_data["latest_dividend_per_share"],
            last_dividend_ratio=round(last_dividend_ratio, 2) if last_dividend_ratio is not None else None,
            last_dividend_growth_rate=round(last_dividend_growth_rate, 2) if last_dividend_growth_rate is not None else None,
            detail=processed_data["yearly_details"],
        )

    def _process_dividend_data(self, dividend_data: List[tuple], current_year: int) -> Dict[str, Any]:
        """배당 데이터를 순수 Python으로 처리"""
        yearly_data = defaultdict(list)
        last_year = current_year - 1
        last_year_months = []
        last_year_count = 0
        latest_dividend_per_share = None

        for row in dividend_data:
            ticker, payment_date, ex_date, per_share, yield_rate = row
            
            # 날짜 파싱 및 문자열 변환
            try:
                # datetime.date 객체를 문자열로 변환
                if hasattr(payment_date, 'strftime'):
                    payment_date_str = payment_date.strftime("%Y-%m-%d")
                    payment_dt = payment_date
                else:
                    payment_date_str = str(payment_date)
                    payment_dt = datetime.strptime(payment_date_str, "%Y-%m-%d")
                
                if hasattr(ex_date, 'strftime'):
                    ex_date_str = ex_date.strftime("%Y-%m-%d")
                    ex_dt = ex_date
                else:
                    ex_date_str = str(ex_date)
                    ex_dt = datetime.strptime(ex_date_str, "%Y-%m-%d")
                
                year = payment_dt.year
                month = payment_dt.month
            except (ValueError, TypeError, AttributeError):
                continue

            # 데이터 타입 변환 및 검증
            try:
                per_share_val = float(per_share) if per_share is not None else None
                yield_rate_val = float(yield_rate) if yield_rate is not None else None
            except (ValueError, TypeError):
                continue

            # 최신 배당금 저장
            if latest_dividend_per_share is None:
                latest_dividend_per_share = per_share_val

            # 작년 데이터 처리
            if year == last_year:
                last_year_count += 1
                month_str = f"{month:02d}"  # 월을 2자리 문자열로 변환 (예: "03", "12")
                if month_str not in last_year_months:
                    last_year_months.append(month_str)

            # 연도별 데이터 그룹화
            yearly_data[year].append({
                "ex_date": ex_date_str,
                "payment_date": payment_date_str,
                "per_share": per_share_val,
                "yield_rate": yield_rate_val,
            })

        # 연도별 상세 정보 생성
        yearly_details = []
        for year in sorted(yearly_data.keys(), reverse=True):
            dividend_details = []
            for data in yearly_data[year]:
                detail = DividendDetail(
                    ex_dividend_date=data["ex_date"],
                    dividend_payment_date=data["payment_date"],
                    dividend_per_share=data["per_share"],
                    dividend_yield=data["yield_rate"],
                )
                dividend_details.append(detail)

            yearly_details.append(DividendYearResponse(
                year=year, 
                dividend_detail=dividend_details
            ))

        return {
            "yearly_details": yearly_details,
            "last_year_count": last_year_count,
            "last_year_months": sorted(last_year_months),
            "latest_dividend_per_share": latest_dividend_per_share,
            "yearly_data": yearly_data,
        }

    async def _calculate_dividend_ratio(self, processed_data: Dict[str, Any], ctry: FinancialCountry, ticker: str) -> Optional[float]:
        """배당성향(Dividend Payout Ratio) 계산 - 순수 Python으로 최적화"""
        try:
            reverse_mapping = {v: k for k, v in contry_mapping.items()}
            ctry_three = reverse_mapping.get(ctry)

            if not ctry_three:
                return None

            table_name = f"{ctry_three}_stock_factors"
            ticker_query = f"{ticker}-US" if ctry_three == "USA" else ticker

            # 주식 발행주식수 조회
            shares_data = await self.db._select_async(
                table=table_name, 
                columns=["shared_outstanding"], 
                limit=1, 
                **{"ticker": ticker_query}
            )

            # 순이익 조회
            income_data = await self.db._select_async(
                table=f"{ctry_three}_income",
                columns=["net_income"],
                limit=1,
                order="StmtDt",
                ascending=False,
                **{"Code": ticker_query},
            )

            if not shares_data or not income_data:
                return None

            latest_shares = shares_data[0][0]
            latest_net_income = income_data[0][0] * 1_000_000  # 백만 단위를 실제 금액으로 변환

            # 최신 배당금 가져오기
            latest_dividend_per_share = processed_data["latest_dividend_per_share"]
            if latest_dividend_per_share is None:
                return None

            if latest_shares == 0:
                return 0.0

            # 주당순이익(EPS) 계산
            eps = latest_net_income / latest_shares

            # 배당성향 = (1주당 배당금 / EPS) * 100
            if eps == 0:
                return None

            return (latest_dividend_per_share / eps) * 100

        except Exception as e:
            print(f"Error calculating dividend ratio: {e}")
            return None

    def _calculate_growth_rate(self, processed_data: Dict[str, Any], current_year: int) -> Optional[float]:
        """배당 성장률 계산 - Compound Annual Growth Rate (CAGR) - 순수 Python으로 최적화"""
        try:
            yearly_data = processed_data["yearly_data"]
            latest_year = current_year - 1
            years_back = 5  # 5년 전 데이터와 비교

            # 필요한 연도 데이터 확인
            if latest_year not in yearly_data or (latest_year - years_back) not in yearly_data:
                return None

            # 각 연도의 배당금 합계 계산
            current_year_div = sum(
                data["per_share"] for data in yearly_data[latest_year] 
                if data["per_share"] is not None
            )
            prev_year_div = sum(
                data["per_share"] for data in yearly_data[latest_year - years_back] 
                if data["per_share"] is not None
            )

            # 유효한 값인지 확인
            if prev_year_div <= 0 or current_year_div <= 0:
                return None

            # CAGR = (Final Value / Initial Value)^(1/n) - 1
            return ((current_year_div / prev_year_div) ** (1 / years_back)) - 1

        except Exception as e:
            print(f"Error calculating growth rate: {e}")
            return None


def get_dividend_service():
    return DividendService()
