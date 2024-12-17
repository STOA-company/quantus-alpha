from app.modules.common.enum import FinancialCountry
from app.modules.dividend.schemas import DividendItem, DividendDetail, DividendYearResponse
from app.database.crud import database


class DividendService:
    def __init__(self):
        self.db = database

    async def get_dividend(self, ctry: FinancialCountry, ticker: str) -> DividendItem:
        """배당 정보 조회"""
        # Mock 데이터 생성
        yearly_details = [
            DividendYearResponse(
                year=2024,
                dividend_detail=[
                    DividendDetail(
                        ex_dividend_date="2024-03-28",
                        dividend_payment_date="2024-04-15",
                        dividend_per_share=1500.0,
                        dividend_yield=1.8,
                    ),
                    DividendDetail(
                        ex_dividend_date="2024-06-28",
                        dividend_payment_date="2024-07-15",
                        dividend_per_share=2000.0,
                        dividend_yield=2.1,
                    ),
                    DividendDetail(
                        ex_dividend_date="2024-09-27",
                        dividend_payment_date="2024-10-15",
                        dividend_per_share=2500.0,
                        dividend_yield=2.5,
                    ),
                    DividendDetail(
                        ex_dividend_date="2024-12-27",
                        dividend_payment_date="2025-01-15",
                        dividend_per_share=3000.0,
                        dividend_yield=2.8,
                    ),
                ],
            ),
            DividendYearResponse(
                year=2023,
                dividend_detail=[
                    DividendDetail(
                        ex_dividend_date="2023-03-29",
                        dividend_payment_date="2023-04-14",
                        dividend_per_share=1300.0,
                        dividend_yield=1.6,
                    ),
                    DividendDetail(
                        ex_dividend_date="2023-06-29",
                        dividend_payment_date="2023-07-14",
                        dividend_per_share=1800.0,
                        dividend_yield=2.0,
                    ),
                    DividendDetail(
                        ex_dividend_date="2023-09-28",
                        dividend_payment_date="2023-10-16",
                        dividend_per_share=2300.0,
                        dividend_yield=2.3,
                    ),
                    DividendDetail(
                        ex_dividend_date="2023-12-28",
                        dividend_payment_date="2024-01-15",
                        dividend_per_share=2800.0,
                        dividend_yield=2.6,
                    ),
                ],
            ),
            DividendYearResponse(
                year=2022,
                dividend_detail=[
                    DividendDetail(
                        ex_dividend_date="2022-03-29",
                        dividend_payment_date="2022-04-15",
                        dividend_per_share=1000.0,
                        dividend_yield=1.4,
                    ),
                    DividendDetail(
                        ex_dividend_date="2022-06-29",
                        dividend_payment_date="2022-07-15",
                        dividend_per_share=1500.0,
                        dividend_yield=1.8,
                    ),
                    DividendDetail(
                        ex_dividend_date="2022-09-29",
                        dividend_payment_date="2022-10-17",
                        dividend_per_share=2000.0,
                        dividend_yield=2.1,
                    ),
                    DividendDetail(
                        ex_dividend_date="2022-12-29",
                        dividend_payment_date="2023-01-16",
                        dividend_per_share=2500.0,
                        dividend_yield=2.4,
                    ),
                ],
            ),
            DividendYearResponse(
                year=2021,
                dividend_detail=[
                    DividendDetail(
                        ex_dividend_date="2021-03-29",
                        dividend_payment_date="2021-04-15",
                        dividend_per_share=800.0,
                        dividend_yield=1.2,
                    ),
                    DividendDetail(
                        ex_dividend_date="2021-06-29",
                        dividend_payment_date="2021-07-15",
                        dividend_per_share=1200.0,
                        dividend_yield=1.5,
                    ),
                    DividendDetail(
                        ex_dividend_date="2021-09-29",
                        dividend_payment_date="2021-10-15",
                        dividend_per_share=1700.0,
                        dividend_yield=1.9,
                    ),
                    DividendDetail(
                        ex_dividend_date="2021-12-29",
                        dividend_payment_date="2022-01-17",
                        dividend_per_share=2200.0,
                        dividend_yield=2.2,
                    ),
                ],
            ),
        ]

        return DividendItem(
            last_year_dividend_count=4,  # 2023년 배당 건수
            last_dividend_per_share=2800.0,  # 2023년 마지막 배당금
            last_dividend_ratio=0.45,  # 배당성향
            last_dividend_growth_rate=0.12,  # 전년 대비 성장률
            detail=yearly_details,
        )


def get_dividend_service():
    return DividendService()
