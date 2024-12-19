from typing import List
import pandas as pd
from sqlalchemy import select
from app.database.crud import database
from app.core.exception.custom import DataNotFoundException
from app.models.models_stock import StockInformation
from app.modules.stock_info.schemas import Indicators, SimilarStock, StockInfo
from app.core.logging.config import get_logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, func
from fastapi import HTTPException

logger = get_logger(__name__)


class StockInfoService:
    def __init__(self):
        self.db = database
        self.file_path = "static"
        self.file_name = "stock_{}_info.csv"

    async def get_stock_info(self, ctry: str, ticker: str, db: AsyncSession) -> StockInfo:
        """
        주식 정보 조회
        """
        if ctry != "us":
            raise DataNotFoundException(ticker=ctry, data_type="stock_info")

        file_name = self.file_name.format(ctry)
        info_file_path = f"{self.file_path}/{file_name}"
        df = pd.read_csv(info_file_path)
        result = df.loc[df["ticker"] == ticker].to_dict(orient="records")[0]
        if result is None:
            raise DataNotFoundException(ticker=ticker, data_type="stock_info")

        intro_file_path = f"{self.file_path}/summary_{ctry}.parquet"
        intro_df = pd.read_parquet(intro_file_path)
        intro_result = intro_df.loc[intro_df["Code"] == ticker].to_dict(orient="records")[0]

        result = StockInfo(
            introduction=intro_result.get("translated_overview", ""),
            homepage_url=result["URL"],
            ceo_name=result["LastName"] + " " + result["FirstName"],
            establishment_date=result["IncInDt"],
            listing_date=result["oldest_date"],
        )

        return result

    async def get_indicators(self, ctry: str, ticker: str, db: AsyncSession) -> Indicators:
        """
        지표 조회
        """
        if ctry == "us":
            ticker = f"{ticker}-US"

        return Indicators(
            per=15.7,
            industry_per=22.4,
            pbr=2.8,
            industry_pbr=3.2,
            roe=12.5,
            industry_roe=9.8,
            financial_data="좋음",
            price_trend="보통",
            market_situation="나쁨",
            industry_situation="좋음",
        )

        # TODO: 임시 Mock 데이터
        # try:
        #     # 실제 쿼리
        #     stock_data = self.db._select(table="stock_kr_1d", order="Date", ascending=False, limit=1, Ticker=ticker)

        #     financial_data = self.db._select(table="KOR_finpos", order="period_q", ascending=False, limit=1, Code=ticker)

        #     # 기본값 설정
        #     result = Indicators(
        #         per=None,
        #         industry_per=None,
        #         pbr=None,
        #         industry_pbr=None,
        #         roe=None,
        #         industry_roe=None,
        #         financial_data=None,
        #         price_trend=None,
        #         market_situation=None,
        #         industry_situation=None,
        #     )

        #     if stock_data and financial_data:
        #         # Row 객체의 _mapping 속성 사용
        #         stock_row = stock_data[0]._mapping
        #         fin_row = financial_data[0]._mapping

        #         stock_price = stock_row["Close"]
        #         retained_earnings = fin_row["retained_earnings"]
        #         total_equity = fin_row["total_equity"]

        #         print("=== DEBUG VALUES ===")
        #         print(f"Stock price: {format(stock_price, ',.2f')}")
        #         print(f"Retained earnings: {format(retained_earnings, ',.8f')}")
        #         print(f"Total equity: {format(total_equity, ',.2f')}")

        #         # 지표 계산
        #         per = round(stock_price / retained_earnings, 2) if retained_earnings and retained_earnings != 0 else None
        #         pbr = round(stock_price / total_equity, 2) if total_equity and total_equity != 0 else None

        #         # ROE 계산 과정 출력
        #         if total_equity and total_equity != 0:
        #             roe_calc = (retained_earnings / total_equity) * 100
        #             print(
        #                 f"ROE calculation: ({format(retained_earnings, ',.8f')} / {format(total_equity, ',.2f')}) * 100 = {format(roe_calc, ',.2f')}"
        #             )
        #             roe = round(roe_calc, 2)
        #         else:
        #             roe = None

        #         result = Indicators(per=per, pbr=pbr, roe=roe)

        #     return result

        # except Exception as e:
        #     print(f"Error: {str(e)}")
        #     raise e

    async def get_similar_stocks(self, ctry: str, ticker: str, db: AsyncSession) -> List[SimilarStock]:
        """
        연관 종목 조회

        Args:
            ctry (str): 국가 코드
            ticker (str): 종목 코드

        Returns:
            List[SimilarStock]: 연관 종목 리스트
        """
        # ticker의 섹터 조회
        query = select(StockInformation).where(StockInformation.ticker == ticker)
        result = await db.execute(query)
        stock_info = result.scalars().first()

        if not stock_info:
            raise HTTPException(status_code=404, detail=f"Stock not found: {ticker}")

        sector = stock_info.sector_3

        # 같은 섹터의 다른 종목들을 랜덤하게 6개 조회
        query = (
            select(StockInformation)
            .where(and_(StockInformation.sector_3 == sector, StockInformation.ticker != ticker))
            .order_by(func.rand())
            .limit(6)
        )

        result = await db.execute(query)
        stocks = result.scalars().all()

        # 종목 SimilarStock 리스트 생성
        similar_stocks = [
            SimilarStock(
                ticker=stock.ticker,
                name=stock.kr_name,
                ctry=stock.ctry,
                current_price=None,
                current_price_rate=None,
            )
            for stock in stocks
        ]

        return similar_stocks


def get_stock_info_service() -> StockInfoService:
    return StockInfoService()
