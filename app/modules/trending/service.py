from datetime import datetime, timedelta
from typing import List

import pytz
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
        kr = [
            TrendingStockKr(
                num=1, ticker="A005930", name="삼성전자", volume=12458930, current_price=73800, current_price_rate=1.23
            ),
            TrendingStockKr(
                num=2,
                ticker="A373220",
                name="LG에너지솔루션",
                volume=8234567,
                current_price=425500,
                current_price_rate=-0.82,
            ),
            TrendingStockKr(
                num=3, ticker="A000660", name="SK하이닉스", volume=7123456, current_price=156700, current_price_rate=2.15
            ),
            TrendingStockKr(
                num=4,
                ticker="A207940",
                name="삼성바이오로직스",
                volume=5234567,
                current_price=823000,
                current_price_rate=-1.32,
            ),
            TrendingStockKr(
                num=5, ticker="A035420", name="NAVER", volume=4567890, current_price=213500, current_price_rate=0.95
            ),
            TrendingStockKr(
                num=6, ticker="A051910", name="LG화학", volume=3987654, current_price=498000, current_price_rate=-0.60
            ),
            TrendingStockKr(
                num=7, ticker="A035720", name="카카오", volume=3654321, current_price=56700, current_price_rate=1.43
            ),
            TrendingStockKr(
                num=8, ticker="A006400", name="삼성SDI", volume=3234567, current_price=456700, current_price_rate=-0.48
            ),
            TrendingStockKr(
                num=9, ticker="A068270", name="셀트리온", volume=2987654, current_price=167800, current_price_rate=0.78
            ),
            TrendingStockKr(
                num=10, ticker="A005380", name="현대차", volume=2765432, current_price=187600, current_price_rate=1.19
            ),
        ]

        us = [
            TrendingStockUs(
                num=1, ticker="AAPL", name="애플", volume=98765432, current_price=169.25, current_price_rate=2.34
            ),
            TrendingStockUs(
                num=2,
                ticker="MSFT",
                name="마이크로소프트",
                volume=87654321,
                current_price=402.75,
                current_price_rate=1.56,
            ),
            TrendingStockUs(
                num=3, ticker="NVDA", name="엔비디아", volume=76543210, current_price=881.28, current_price_rate=3.21
            ),
            TrendingStockUs(
                num=4, ticker="GOOGL", name="알파벳", volume=65432109, current_price=152.50, current_price_rate=-0.45
            ),
            TrendingStockUs(
                num=5, ticker="META", name="메타플랫폼스", volume=54321098, current_price=509.58, current_price_rate=1.87
            ),
            TrendingStockUs(
                num=6, ticker="TSLA", name="테슬라", volume=43210987, current_price=172.63, current_price_rate=-2.15
            ),
            TrendingStockUs(
                num=7, ticker="AMZN", name="아마존", volume=32109876, current_price=178.15, current_price_rate=0.92
            ),
            TrendingStockUs(
                num=8,
                ticker="BRK.B",
                name="버크셔 해서웨이",
                volume=21098765,
                current_price=412.38,
                current_price_rate=0.34,
            ),
            TrendingStockUs(
                num=9, ticker="JPM", name="JP모건", volume=19876543, current_price=196.62, current_price_rate=-0.78
            ),
            TrendingStockUs(
                num=10, ticker="V", name="비자", volume=18765432, current_price=279.87, current_price_rate=0.65
            ),
        ]

        # kr = self._get_trending_stocks_kr()
        # us = self._get_trending_stocks_us()
        return TrendingStock(kr=kr, us=us)

    def _get_trending_stocks_kr(self) -> List[TrendingStockKr]:
        table_name = "stock_kr_1d"

        kst = pytz.timezone("Asia/Seoul")
        today = datetime.now(kst)
        weekday = today.weekday()

        if weekday == 0:
            check_date = today - timedelta(days=3)
        elif weekday == 6:
            check_date = today - timedelta(days=2)
        else:
            check_date = today - timedelta(days=1)

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

        kst = pytz.timezone("Asia/Seoul")
        today = datetime.now(kst)
        weekday = today.weekday()

        if weekday == 0:
            check_date = today - timedelta(days=3)
        elif weekday == 6:
            check_date = today - timedelta(days=2)
        else:
            check_date = today - timedelta(days=1)

        # 조인 정보 설정
        join_info = JoinInfo(
            primary_table=table_name,
            secondary_table="stock_us_tickers",
            primary_column="Ticker",
            secondary_column="ticker",
            columns=["korean_name"],
            is_outer=False,
        )

        while True:
            date_str = check_date.strftime("%Y-%m-%d")
            query_result = self.database._select(
                table=table_name,
                columns=["Ticker", "Volume", "Open", "Close", "korean_name"],
                order="Volume",
                ascending=False,
                limit=10,
                join_info=join_info,
                Date=date_str,
            )
            if query_result:
                break
            else:
                check_date = check_date - timedelta(days=1)
                if (today - check_date).days > 4:
                    break

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
