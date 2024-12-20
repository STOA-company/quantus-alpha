from typing import List

from app.modules.news.services import get_news_service
from app.database.crud import JoinInfo, database
from app.database.conn import db
from app.modules.trending.schemas import TrendingStock, TrendingStockKr, TrendingStockUs


class TrendingService:
    def __init__(self):
        self.news_service = get_news_service()
        self.database = database
        self.db = db

    async def get_trending_stocks(self) -> TrendingStock:
        kr = await self._get_trending_stocks_kr()
        us = await self._get_trending_stocks_us()
        return TrendingStock(kr=kr, us=us)

    async def _get_trending_stocks_kr(self) -> List[TrendingStockKr]:
        table_name = "stock_kr_1d"

        # 최신 날짜 조회
        latest_date = self.database._select(table=table_name, columns=["Date"], order="Date", ascending=False, limit=1)[
            0
        ].Date

        # 최신 날짜의 데이터 중 volume 상위 10개 조회
        result = self.database._select(
            table=table_name,
            columns=["Ticker", "Name", "Volume"],
            order="Volume",
            ascending=False,
            limit=10,
            **{"Date": latest_date},
        )

        trending_stocks = []
        for idx, row in enumerate(result, start=1):
            current_price, current_price_rate = await self.get_current_price(ticker=row.Ticker, table_name=table_name)

            stock_dict = {
                "num": idx,
                "ticker": str(row.Ticker),
                "name": str(row.Name),
                "volume": float(row.Volume) if row.Volume is not None else 0.0,
                "current_price": current_price,
                "current_price_rate": current_price_rate,
            }
            trending_stocks.append(TrendingStockKr(**stock_dict))

        return trending_stocks

    async def _get_trending_stocks_us(self) -> List[TrendingStockUs]:
        table_name = "stock_us_1d"

        # 최신 날짜 조회
        latest_date = self.database._select(table=table_name, columns=["Date"], order="Date", ascending=False, limit=1)[
            0
        ].Date

        # 조인 정보 설정
        join_info = JoinInfo(
            primary_table=table_name,
            secondary_table="stock_us_tickers",
            primary_column="Ticker",
            secondary_column="ticker",
            columns=["korean_name"],
            is_outer=False,
        )

        # 최신 날짜의 데이터 중 volume 상위 10개 조회 (조인 포함)
        result = self.database._select(
            table=table_name,
            columns=["Ticker", "Volume"],
            order="Volume",
            ascending=False,
            limit=10,
            join_info=join_info,
            **{"Date": latest_date},
        )

        trending_stocks = []
        for idx, row in enumerate(result, start=1):
            # 각 종목별 현재가와 변동률 조회
            current_price, current_price_rate = await self.get_current_price(ticker=row.Ticker, table_name=table_name)

            stock_dict = {
                "num": idx,
                "ticker": str(row.Ticker),
                "name": str(row.korean_name),
                "volume": float(row.Volume) if row.Volume is not None else 0.0,
                "current_price": current_price,
                "current_price_rate": current_price_rate,
            }
            trending_stocks.append(TrendingStockUs(**stock_dict))

        return trending_stocks

    # 가져온 테이블에서의 각 티커별 가격 및 변동률
    async def get_current_price(self, ticker: str, table_name: str) -> tuple[float, float]:
        """
        현재가와 변동률 조회 (당일 종가와 시가 기준)
        Args:
            ticker: 종목코드
            table_name: 테이블명
        Returns:
            tuple[float, float]: (현재가, 변동률)
        """
        # 당일 시가와 종가 조회
        result = self.database._select(
            table=table_name,
            columns=["Date", "Open", "Close"],
            order="Date",
            ascending=False,
            limit=1,
            **{"Ticker": ticker},
        )

        if not result:
            return 0.0, 0.0

        current_price = float(result[0].Close)
        open_price = float(result[0].Open)

        # 변동률 계산: ((종가 - 시가) / 시가) * 100
        price_rate = round(((current_price - open_price) / open_price * 100), 2) if open_price != 0 else 0.0

        return current_price, price_rate


def get_trending_service():
    return TrendingService()
