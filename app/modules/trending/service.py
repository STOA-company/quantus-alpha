from typing import List
import random  # 파일 상단에 추가

from app.modules.news.services import get_news_service
from app.database.crud import JoinInfo, database
from app.database.conn import db
from app.modules.trending.schemas import TrendingStock, TrendingStockEn, TrendingStockKr


class TrendingService:
    def __init__(self):
        self.news_service = get_news_service()
        self.database = database
        self.db = db

    async def get_trending_stocks(self) -> TrendingStock:
        kr = await self._get_trending_stocks_kr()
        en = await self._get_trending_stocks_en()
        return TrendingStock(kr=kr, en=en)

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
            # 무작위 가격과 변동률 생성
            current_price = random.randint(10000, 100000)  # 1만원 ~ 10만원
            current_price_rate = round(random.uniform(-5.0, 5.0), 2)  # -5.0% ~ +5.0%

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

    async def _get_trending_stocks_en(self) -> List[TrendingStockEn]:
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
            # 무작위 가격과 변동률 생성
            current_price = round(random.uniform(10.0, 500.0), 2)  # $10 ~ $500
            current_price_rate = round(random.uniform(-5.0, 5.0), 2)  # -5.0% ~ +5.0%

            stock_dict = {
                "num": idx,
                "ticker": str(row.Ticker),
                "name": str(row.korean_name),
                "volume": float(row.Volume) if row.Volume is not None else 0.0,
                "current_price": current_price,
                "current_price_rate": current_price_rate,
            }
            trending_stocks.append(TrendingStockEn(**stock_dict))

        return trending_stocks


def get_trending_service():
    return TrendingService()
