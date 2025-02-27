from app.database.crud import database
from typing import Literal
from app.kispy.sdk import CustomKisClientV2
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from app.utils.date_utils import check_market_status

logger = logging.getLogger(__name__)


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
            is_activate=True,
            is_trading_stopped=False,
            is_delisted=False,
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
            is_activate=True,
            is_trading_stopped=False,
            is_delisted=False,
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

    def update_stock_trend(self, tickers: list[str]):
        if not check_market_status(self.nation.upper()):
            # 장 중에만 실행
            return

        try:
            all_data = self.db._select(
                table=f"stock_{self.nation}_1d",
                columns=["Ticker", "Date", "Close", "Volume", "Open", "High", "Low"],
                Ticker__in=tickers,
                order="Date",
                ascending=False,
            )

            df = pd.DataFrame(all_data, columns=["Ticker", "Date", "Close", "Volume", "Open", "High", "Low"])
            df["volume_change"] = (df["Open"] + df["High"] + df["Low"] + df["Close"]) / 4 * df["Volume"]

            update_data = []
            for ticker in tickers:
                ticker_data = df[df["Ticker"] == ticker].copy()
                if len(ticker_data) < 2:
                    continue

                one_day_ago = ticker_data.iloc[0]
                two_days_ago = ticker_data.iloc[1]

                update_dict = {
                    "ticker": ticker,
                    "last_updated": pd.to_datetime(one_day_ago["Date"]),
                    "current_price": float(one_day_ago["Close"]),
                    "prev_close": float(two_days_ago["Close"]),
                    "change_rt": float(((one_day_ago["Close"] - two_days_ago["Close"]) / two_days_ago["Close"] * 100)),
                    "change_1d": float(((one_day_ago["Close"] - two_days_ago["Close"]) / two_days_ago["Close"] * 100)),
                    "volume_rt": float(one_day_ago["Volume"]),
                    "volume_1d": float(two_days_ago["Volume"]),
                    "volume_change_rt": float(one_day_ago["volume_change"]),
                    "volume_change_1d": float(two_days_ago["volume_change"]),
                }

                periods = {
                    "1w": 5,
                    "1m": 20,
                    "6m": 120,
                    "1y": 240,
                }

                for period, days in periods.items():
                    period_data = ticker_data.head(min(days + 1, len(ticker_data)))
                    if len(period_data) > 1:
                        start_price = period_data.iloc[-1]["Close"]
                        update_dict[f"change_{period}"] = float(
                            ((one_day_ago["Close"] - start_price) / start_price * 100)
                        )
                        update_dict[f"volume_{period}"] = float(period_data["Volume"].sum())
                        update_dict[f"volume_change_{period}"] = float(period_data["volume_change"].sum())
                    else:
                        update_dict[f"change_{period}"] = None
                        update_dict[f"volume_{period}"] = None
                        update_dict[f"volume_change_{period}"] = None

                update_data.append(update_dict)

            if update_data:
                self.db._bulk_update(table="stock_trend", data=update_data, key_column="ticker")

        except Exception as e:
            logger.error(f"Error in update_stock_trend: {str(e)}")
            raise e

    def update_prev_close(self, tickers: list[str]):
        latest_date = self.db._select(
            table=f"stock_{self.nation}_1d",
            columns=["Date"],
            order="Date",
            ascending=False,
            limit=1,
        )[0][0]
        prev_close = self.db._select(
            table=f"stock_{self.nation}_1d",
            columns=["Ticker", "Close"],
            Ticker__in=tickers,
            Date=latest_date,
        )
        prev_close_data = [{"ticker": row[0], "prev_close": row[1]} for row in prev_close]
        self.db._bulk_update(table="stock_trend", data=prev_close_data, key_column="ticker")

    def update_multiple_tickers(self, tickers: list[str], max_workers: int = 5):
        results = self.update_time_series_data_parallel(tickers, max_workers)

        successful_tickers = [ticker for ticker, success in results.items() if success]
        if successful_tickers:
            self.update_stock_trend(successful_tickers)

        return results

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
