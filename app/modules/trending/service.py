from datetime import date, timedelta
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

    def get_trending_stocks(self) -> TrendingStock:
        kr = self._get_trending_stocks_kr()
        us = self._get_trending_stocks_us()
        return TrendingStock(kr=kr, us=us)

    def _get_trending_stocks_kr(self) -> List[TrendingStockKr]:
        table_name = "stock_kr_1d"

        check_date = date.today() - timedelta(days=1)
        while True:
            date_str = check_date.strftime("%Y-%m-%d")
            query_result = self.database._select(
                table=table_name,
                columns=["Ticker", "Name", "Volume", "Open", "Close", "Date"],
                order="Volume",
                ascending=False,
                limit=10,
                Date=date_str,
            )
            if query_result:
                break
            else:
                check_date = check_date - timedelta(days=1)

        result = []
        for idx, row in enumerate(query_result, start=1):
            current_price = float(row.Close) if row.Close is not None else 0.0
            open_price = float(row.Open) if row.Open is not None else 0.0

            price_rate = round(((current_price - open_price) / open_price * 100), 2) if open_price != 0 else 0.0

            stock = TrendingStockKr(
                num=idx,
                ticker=str(row.Ticker),
                name=str(row.Name),
                volume=float(row.Volume) if row.Volume is not None else 0.0,
                current_price=current_price,
                current_price_rate=price_rate,
            )
            result.append(stock)

        return result

    def _get_trending_stocks_us(self) -> List[TrendingStockUs]:
        table_name = "stock_us_1d"

        # 조인 정보 설정
        join_info = JoinInfo(
            primary_table=table_name,
            secondary_table="stock_us_tickers",
            primary_column="Ticker",
            secondary_column="ticker",
            columns=["korean_name"],
            is_outer=False,
        )

        query_result = self.database._select(
            table=table_name,
            columns=["Ticker", "Volume", "Open", "Close", "korean_name"],
            order="Volume",
            ascending=False,
            limit=10,
            join_info=join_info,
            Date=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
        )

        result = []
        for idx, row in enumerate(query_result, start=1):
            current_price = float(row.Close) if row.Close is not None else 0.0
            open_price = float(row.Open) if row.Open is not None else 0.0

            price_rate = round(((current_price - open_price) / open_price * 100), 2) if open_price != 0 else 0.0

            stock = TrendingStockUs(
                num=idx,
                ticker=str(row.Ticker),
                name=str(row.korean_name),
                volume=float(row.Volume) if row.Volume is not None else 0.0,
                current_price=current_price,
                current_price_rate=price_rate,
            )
            result.append(stock)

        return result


def get_trending_service():
    return TrendingService()
