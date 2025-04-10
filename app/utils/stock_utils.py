from app.database.crud import database
from typing import Literal
from app.kispy.sdk import CustomKisClientV2
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from app.core.logger import setup_logger


logger = setup_logger(__name__)


class StockUtils:
    def __init__(self, nation: Literal["kr", "us"]):
        self.db = database
        self.kispy = CustomKisClientV2(nation=nation.upper())
        self.nation = nation
        self.max_workers = 10
        self.chunk_size = 1000

    def get_top_gainers(self, period: Literal["rt", "1d", "1w", "1m", "6m", "1y"], limit: int = 10):
        stocks = self.db._select(
            table="stock_trend",
            columns=["ticker"],
            order=f"change_{period}",
            ascending=False,
            ctry=self.nation,
            limit=limit,
        )
        tickers = [stock[0] for stock in stocks]
        return tickers

    def get_top_losers(self, period: Literal["rt", "1d", "1w", "1m", "6m", "1y"], limit: int = 10):
        stocks = self.db._select(
            table="stock_trend",
            columns=["ticker"],
            order=f"change_{period}",
            ascending=True,
            ctry=self.nation,
            limit=limit,
        )
        tickers = [stock[0] for stock in stocks]
        return tickers

    def update_time_series_data(self, ticker: str) -> bool:
        try:
            ticker_ = ticker[1:] if self.nation == "kr" else ticker

            df = self.kispy.fetch_stock_data(ticker_)
            if df is None:
                logger.warning(f"데이터를 가져올 수 없음: {ticker}")
                return False

            bulk_data = []
            for _, row in df.iterrows():
                data = {
                    "Ticker": ticker,
                    "Date": pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                    "Open": float(row["Open"]),
                    "High": float(row["High"]),
                    "Low": float(row["Low"]),
                    "Close": float(row["Close"]),
                    "Volume": int(row["Volume"]),
                }
                bulk_data.append(data)

            self.db._delete(table=f"stock_{self.nation}_1d", Ticker=ticker)

            self.db._insert(table=f"stock_{self.nation}_1d", sets=bulk_data)
            return True

        except Exception as e:
            logger.error(f"주가 데이터 업데이트 실패 {ticker}: {str(e)}")
            return False

    def update_time_series_data_parallel(self, tickers: list[str], max_workers: int = 5):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.update_time_series_data, ticker): ticker for ticker in tickers}
            results = {}
            for future in concurrent.futures.as_completed(futures):
                ticker = futures[future]
                try:
                    success = future.result()
                    results[ticker] = success
                except Exception as e:
                    logger.error(f"Error processing {ticker}: {e}")
                    results[ticker] = False
        return results

    def update_prev_close(self, tickers: list[str]):
        stock_data = self.db._select(
            table=f"stock_{self.nation}_1d",
            columns=["Date", "Ticker", "Close"],
            Ticker__in=tickers,
            order="Date",
            ascending=False,
        )

        latest_date = stock_data[0][0] if stock_data else None
        if not latest_date:
            return

        ticker_to_close = {row[1]: row[2] for row in stock_data if row[0] == latest_date}

        current_prices = self.db._select(
            table="stock_trend",
            columns=["ticker", "current_price"],
            ticker__in=tickers,
        )

        # 티커별 현재 가격 딕셔너리
        ticker_to_current = {row[0]: row[1] for row in current_prices}

        prev_close_data = []
        for ticker in tickers:
            if ticker in ticker_to_close and ticker in ticker_to_current:
                prev_close = ticker_to_close[ticker]
                current_price = ticker_to_current[ticker]
                change_rt = (current_price - prev_close) / prev_close * 100 if prev_close else 0
                prev_close_data.append({"ticker": ticker, "change_rt": change_rt, "prev_close": prev_close})

        if prev_close_data:
            self.db._bulk_update(table="stock_trend", data=prev_close_data, key_column="ticker")

    def update_top_gainers(self):
        tickers = []
        for period in ["rt", "1d"]:
            tickers.extend(self.get_top_gainers(period))
        tickers = list(set(tickers))
        for ticker in tickers:
            self.update_time_series_data(ticker)
        self.update_prev_close(tickers)

    def update_top_losers(self):
        tickers = []
        for period in ["rt", "1d"]:
            tickers.extend(self.get_top_losers(period))
        tickers = list(set(tickers))
        for ticker in tickers:
            self.update_time_series_data(ticker)
        self.update_prev_close(tickers)


us_stock_utils = StockUtils(nation="us")
kr_stock_utils = StockUtils(nation="kr")


if __name__ == "__main__":
    us_stock_utils.update_top_gainers()
    us_stock_utils.update_top_losers()
    # kr_stock_utils.update_top_gainers()
    # kr_stock_utils.update_top_losers()
