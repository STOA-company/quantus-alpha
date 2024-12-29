import logging
from sqlalchemy import text
import yfinance as yf
import requests
from typing import Tuple
import asyncio
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from app.database.crud import database
from app.modules.stock_indices.schemas import IndexSummary, IndicesData, IndicesResponse, TimeData


class StockIndicesService:
    def __init__(self):
        self.db = database
        self.symbols = {"kospi": "^KS11", "kosdaq": "^KQ11", "nasdaq": "^IXIC", "sp500": "^GSPC"}
        self._cache = {}
        self._cache_timeout = 300
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._lock = asyncio.Lock()
        self._background_task_running = False
        self.session = requests.Session()

    async def _update_cache_background(self):
        """백그라운드에서 캐시 업데이트"""
        if self._background_task_running:
            return

        try:
            self._background_task_running = True
            while True:
                tasks = [self._fetch_yf_data_concurrent(symbol, name) for name, symbol in self.symbols.items()]
                await asyncio.gather(*tasks)

                ratio_tasks = [self.get_market_ratios(name) for name in self.symbols.keys()]
                await asyncio.gather(*ratio_tasks)

                await asyncio.sleep(240)
        finally:
            self._background_task_running = False

    async def _fetch_yf_data_concurrent(self, symbol: str, name: str):
        """비동기로 yfinance 데이터 조회"""
        try:
            cache_key_daily = f"{name}_daily"
            cache_key_min5 = f"{name}_min5"
            now = datetime.now()

            if cache_key_daily in self._cache and cache_key_min5 in self._cache:
                cached_daily, timestamp_daily = self._cache[cache_key_daily]
                cached_min5, timestamp_min5 = self._cache[cache_key_min5]
                if now - timestamp_daily < timedelta(seconds=self._cache_timeout) and now - timestamp_min5 < timedelta(
                    seconds=self._cache_timeout
                ):
                    return

            async def fetch_history(period, interval=None):
                try:
                    loop = asyncio.get_event_loop()
                    ticker = yf.Ticker(symbol, session=self.session)

                    def fetch():
                        if interval:
                            return ticker.history(period=period, interval=interval)
                        return ticker.history(period=period)

                    df = await loop.run_in_executor(self._executor, fetch)
                    return df
                except Exception:
                    return pd.DataFrame()

            daily_df, min5_df = await asyncio.gather(fetch_history("1d"), fetch_history("1d", "5m"))

            if not daily_df.empty:
                daily_data = {
                    "open": round(float(daily_df["Open"].iloc[0]), 2),
                    "close": round(float(daily_df["Close"].iloc[0]), 2),
                }
                self._cache[cache_key_daily] = (daily_data, now)

            if not min5_df.empty:
                min5_data = {
                    index.strftime("%Y-%m-%d %H:%M:%S"): TimeData(
                        open=round(row["Open"], 2),
                        high=round(row["High"], 2),
                        low=round(row["Low"], 2),
                        close=round(row["Close"], 2),
                        volume=round(row["Volume"], 2),
                    )
                    for index, row in min5_df.iterrows()
                }
                self._cache[cache_key_min5] = (min5_data, now)

        except Exception as e:
            logging.error(f"Error fetching data for {name}: {e}")

    # 코스피200 ticker 조회
    def get_kospi200_ticker(self):
        try:
            df = self.db._select(table="stock_information", columns=["ticker"], is_kospi_200=True)
            return pd.DataFrame(df, columns=["ticker"])
        except Exception as e:
            print(f"Exception in get_kospi200_ticker: {str(e)}")
            logging.error(f"Error fetching kospi200 ticker: {e}")

    # 코스닥150 ticker 조회
    def get_kosdaq150_ticker(self):
        try:
            df = self.db._select(table="stock_information", columns=["ticker"], is_kosdaq_150=True)
            return pd.DataFrame(df, columns=["ticker"])
        except Exception as e:
            print(f"Exception in get_kosdaq150_ticker: {str(e)}")
            logging.error(f"Error fetching kosdaq150 ticker: {e}")

    # 나스닥100 ticker 조회
    def get_nasdaq100_ticker(self):
        try:
            df = self.db._select(table="stock_information", columns=["ticker"], is_nasdaq_100=True)
            return pd.DataFrame(df, columns=["ticker"])
        except Exception as e:
            print(f"Exception in get_nasdaq100_ticker: {str(e)}")
            logging.error(f"Error fetching nasdaq100 ticker: {e}")

    # S&P500 ticker 조회
    def get_sp500_ticker(self):
        try:
            df = self.db._select(table="stock_information", columns=["ticker"], is_snp_500=True)
            return pd.DataFrame(df, columns=["ticker"])  # 결과를 DataFrame으로 변환
        except Exception as e:
            print(f"Exception in get_sp500_ticker: {str(e)}")
            logging.error(f"Error fetching S&P500 ticker: {e}")
            return pd.DataFrame()

    async def get_market_ratios(self, market: str) -> Tuple[float, float, float]:
        """최적화된 시장 등락비율 조회"""
        try:
            cache_key = f"{market}_ratio"
            now = datetime.now()

            if cache_key in self._cache:
                data, timestamp = self._cache[cache_key]
                if now - timestamp < timedelta(seconds=self._cache_timeout):
                    return data

            async with self._lock:
                market_mapping = {"kospi": "KOSPI200", "kosdaq": "KOSDAQ150", "nasdaq": "NASDAQ100", "sp500": "S&P500"}
                market_filter = market_mapping.get(market.lower(), market)

                ticker_functions = {
                    "KOSPI200": self.get_kospi200_ticker,
                    "KOSDAQ150": self.get_kosdaq150_ticker,
                    "NASDAQ100": self.get_nasdaq100_ticker,
                    "S&P500": self.get_sp500_ticker,
                }

                ticker_func = ticker_functions.get(market_filter)
                if not ticker_func:
                    return 0.0, 0.0, 0.0

                df = ticker_func()
                if df.empty:
                    return 0.0, 0.0, 0.0

                tickers = tuple(df["ticker"].tolist())
                table = "stock_kr_1d" if market_filter in ["KOSPI200", "KOSDAQ150"] else "stock_us_1d"

                query = text(
                    """
                    WITH latest_date AS (
                        SELECT MAX(Date) as max_date
                        FROM """
                    + table
                    + """
                        WHERE ticker IN :tickers
                    )
                    SELECT
                        SUM(CASE WHEN (Close - Open) / Open * 100 > 1 THEN 1 ELSE 0 END) AS advance,
                        SUM(CASE WHEN (Close - Open) / Open * 100 < -1 THEN 1 ELSE 0 END) AS decline,
                        SUM(CASE WHEN ABS((Close - Open) / Open * 100) <= 1 THEN 1 ELSE 0 END) AS unchanged,
                        COUNT(*) AS total
                    FROM """
                    + table
                    + """, latest_date
                    WHERE ticker IN :tickers
                    AND Date = latest_date.max_date
                    AND Open != 0
                """
                )

                result = self.db._execute(query, {"tickers": tickers})
                rows = result.fetchall()

                if not rows:
                    return 0.0, 0.0, 0.0

                advance, decline, unchanged, total = rows[0]

                if total == 0:
                    return 0.0, 0.0, 0.0

                ratios = (
                    round(advance / total * 100, 2),
                    round(decline / total * 100, 2),
                    round(unchanged / total * 100, 2),
                )

                self._cache[cache_key] = (ratios, now)
                return ratios

        except Exception as e:
            logging.error(f"Error in get_market_ratios for {market}: {str(e)}")
            return 0.0, 0.0, 0.0

    async def get_indices_data(self) -> IndicesData:
        """지수 데이터 조회"""
        try:
            tasks = [self._fetch_yf_data_concurrent(symbol, name) for name, symbol in self.symbols.items()]
            await asyncio.gather(*tasks)

            ratio_tasks = [self.get_market_ratios(name) for name in self.symbols.keys()]
            ratio_results = await asyncio.gather(*ratio_tasks)

            indices_summary = {}
            indices_data = {}

            for name, ratios in zip(self.symbols.keys(), ratio_results):
                cache_key_daily = f"{name}_daily"
                cache_key_min5 = f"{name}_min5"

                if cache_key_daily in self._cache:
                    daily_data, _ = self._cache[cache_key_daily]
                    change = daily_data["close"] - daily_data["open"]
                    change_percent = round((change / daily_data["open"]) * 100, 2) if daily_data["open"] != 0 else 0.00

                    rise_ratio, fall_ratio, unchanged_ratio = ratios

                    indices_summary[name] = IndexSummary(
                        prev_close=daily_data["close"],
                        change=round(change, 2),
                        change_percent=change_percent,
                        rise_ratio=rise_ratio,
                        fall_ratio=fall_ratio,
                        unchanged_ratio=unchanged_ratio,
                    )
                else:
                    indices_summary[name] = IndexSummary(
                        prev_close=0.00,
                        change=0.00,
                        change_percent=0.00,
                        rise_ratio=0.00,
                        fall_ratio=0.00,
                        unchanged_ratio=0.00,
                    )

                indices_data[name] = self._cache.get(cache_key_min5, ({}, None))[0]

            return IndicesData(
                status_code=200,
                message="데이터를 성공적으로 조회했습니다.",
                kospi=indices_summary["kospi"],
                kosdaq=indices_summary["kosdaq"],
                nasdaq=indices_summary["nasdaq"],
                sp500=indices_summary["sp500"],
                data=IndicesResponse(
                    kospi=indices_data["kospi"],
                    kosdaq=indices_data["kosdaq"],
                    nasdaq=indices_data["nasdaq"],
                    sp500=indices_data["sp500"],
                ),
            )

        except Exception as e:
            empty_summary = IndexSummary(
                prev_close=0.00, change=0.00, change_percent=0.00, rise_ratio=0.00, fall_ratio=0.00, unchanged_ratio=0.00
            )
            return IndicesData(
                status_code=404,
                message=f"데이터 조회 중 오류가 발생했습니다: {e}",
                kospi=empty_summary,
                kosdaq=empty_summary,
                nasdaq=empty_summary,
                sp500=empty_summary,
                data=None,
            )

    # async def get_market_ratios(self, market: str) -> Tuple[float, float, float]:
    #     """최적화된 시장 등락비율 조회"""
    #     try:
    #         cache_key = f"{market}_ratio"
    #         now = datetime.now()

    #         # 캐시 확인
    #         if cache_key in self._cache:
    #             data, timestamp = self._cache[cache_key]
    #             if now - timestamp < timedelta(seconds=self._cache_timeout):
    #                 return data

    #         # 고정된 테스트 데이터 반환
    #         ratios = {
    #             "kospi": (45.32, 35.45, 19.23),
    #             "kosdaq": (42.15, 38.65, 19.20),
    #             "nasdaq": (48.25, 32.55, 19.20),
    #             "sp500": (46.78, 34.02, 19.20),
    #         }

    #         test_ratios = ratios.get(market.lower(), (33.33, 33.33, 33.34))

    #         # 캐시에 저장
    #         self._cache[cache_key] = (test_ratios, now)
    #         return test_ratios

    #     except Exception as e:
    #         logging.error(f"Error in get_market_ratios for {market}: {str(e)}")
    #         return 0.0, 0.0, 0.0
