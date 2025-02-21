from app.database.crud import database
from typing import Literal
from app.kispy.sdk import CustomKisClientV2
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class StockUtils:
    def __init__(self, nation: Literal["kr", "us"]):
        self.db = database
        self.kispy = CustomKisClientV2(nation=nation.upper())
        self.nation = nation

    def get_top_change_stocks(self, period: Literal["rt", "1d", "1w", "1m", "6m", "1y"], limit: int = 10):
        stocks = self.db._select(
            table="stock_trend",
            columns=["ticker"],
            order=f"change_{period}",
            ascending=False,
            ctry=self.nation,
            is_activate=True,
            is_trading_stopped=False,
            is_delisted=False,
            limit=limit,
        )
        tickers = [stock[0] for stock in stocks]
        return tickers

    def activate_stock(self, ticker: str):
        self.db._update(
            table="stock_information",
            sets={"is_activate": True},
            ticker=ticker,
        )

        self.db._update(
            table="stock_trend",
            sets={"is_activate": True},
            ticker=ticker,
        )

    def deactivate_stock(self, ticker: str):
        self.db._update(
            table="stock_information",
            sets={"is_activate": False},
            ticker=ticker,
        )

        self.db._update(
            table="stock_trend",
            sets={"is_activate": False},
            ticker=ticker,
        )

    def update_stock_data(self, ticker: str):
        try:
            ticker_ = ticker[1:] if self.nation == "kr" else ticker
            self.deactivate_stock(ticker)

            df = self.kispy.fetch_stock_data(ticker_)
            if df is None:
                logger.warning(f"데이터를 가져올 수 없음: {ticker}")
                return False

            for _, row in df.iterrows():
                try:
                    existing = self.db._select(
                        table=f"stock_{self.nation}_1d",
                        columns=["Ticker"],
                        Ticker=ticker,
                        Date=pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                    )

                    data = {
                        "Date": pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                        "Open": float(row["Open"]),
                        "High": float(row["High"]),
                        "Low": float(row["Low"]),
                        "Close": float(row["Close"]),
                        "Volume": int(row["Volume"]),
                    }

                    if existing:
                        self.db._update(table=f"stock_{self.nation}_1d", sets=data, Ticker=ticker, Date=data["Date"])
                    else:
                        data["Ticker"] = ticker
                        self.db._insert(table=f"stock_{self.nation}_1d", data=data)

                except Exception as e:
                    logger.error(f"데이터 업데이트 실패 {ticker}, date: {row['Date']}: {str(e)}")
                    continue

            self.activate_stock(ticker)
            return True

        except Exception as e:
            logger.error(f"주가 데이터 업데이트 실패 {ticker}: {str(e)}")
            self.activate_stock(ticker)
            return False


if __name__ == "__main__":
    kr_stock_utils = StockUtils(nation="kr")
    us_stock_utils = StockUtils(nation="us")

    kr_tickers = kr_stock_utils.get_top_change_stocks(period="1d", limit=10)
    us_tickers = us_stock_utils.get_top_change_stocks(period="1d", limit=10)
    print(kr_tickers)
    print(us_tickers)
