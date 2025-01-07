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
from app.utils.date_utils import get_time_checker
from zoneinfo import ZoneInfo


class StockIndicesService:
    def __init__(self):
        self.db = database
        self.symbols = {"kospi": "^KS11", "kosdaq": "^KQ11", "nasdaq": "^IXIC", "sp500": "^GSPC"}
        self._cache = {}
        self._cache_timeout = 86400
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._lock = asyncio.Lock()
        self._background_task_running = False
        self.session = requests.Session()
        self._nasdaq_lock = asyncio.Lock()
        self._sp500_lock = asyncio.Lock()

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

            # 캐시가 있고 유효한 경우 그대로 반환
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
                        # 데이터가 있을 때까지 기간을 늘려가며 조회
                        for days in [1, 2, 3, 4, 5]:  # 최대 5일까지 확인
                            df = ticker.history(period=f"{days}d")
                            if not df.empty:
                                return df
                        return pd.DataFrame()

                    df = await loop.run_in_executor(self._executor, fetch)
                    return df
                except Exception:
                    return pd.DataFrame()

            daily_df, min5_df = await asyncio.gather(fetch_history("1d"), fetch_history("1d", "5m"))

            # 일별 데이터 처리
            if not daily_df.empty:
                valid_data = daily_df[daily_df["Open"] != 0]
                if not valid_data.empty:
                    latest_data = valid_data.iloc[-1]
                    daily_data = {
                        "open": round(float(latest_data["Open"]), 2),
                        "close": round(float(latest_data["Close"]), 2),
                    }
                    self._cache[cache_key_daily] = (daily_data, now)
                    logging.info(f"Cached daily data for {name}: {cache_key_daily}")

            # 5분 데이터 처리
            if not min5_df.empty:
                valid_data = min5_df[min5_df["Open"] != 0]
                if not valid_data.empty:
                    min5_data = {
                        index.strftime("%Y-%m-%d %H:%M:%S"): TimeData(
                            open=round(row["Open"], 2),
                            high=round(row["High"], 2),
                            low=round(row["Low"], 2),
                            close=round(row["Close"], 2),
                            volume=round(row["Volume"], 2),
                        )
                        for index, row in valid_data.iterrows()
                    }
                    self._cache[cache_key_min5] = (min5_data, now)
                    logging.info(f"Cached 5-minute data for {name}: {cache_key_min5}")

        except Exception as e:
            logging.error(f"Error fetching data for {name}: {e}")

    # 코스피200 ticker 조회
    def get_kospi200_ticker(self):
        try:
            df = self.db._select(table="stock_information", columns=["ticker"], is_kospi_200=True)
            return pd.DataFrame(df, columns=["ticker"])
        except Exception as e:
            logging.error(f"Error fetching kospi200 ticker: {e}")

    # 코스닥150 ticker 조회
    def get_kosdaq150_ticker(self):
        try:
            df = self.db._select(table="stock_information", columns=["ticker"], is_kosdaq_150=True)
            return pd.DataFrame(df, columns=["ticker"])
        except Exception as e:
            logging.error(f"Error fetching kosdaq150 ticker: {e}")

    # 나스닥100 ticker 조회
    def get_nasdaq100_ticker(self):
        try:
            df = self.db._select(table="stock_information", columns=["ticker"], is_nasdaq_100=True)
            return pd.DataFrame(df, columns=["ticker"])
        except Exception as e:
            logging.error(f"Error fetching nasdaq100 ticker: {e}")

    # S&P500 ticker 조회
    def get_sp500_ticker(self):
        try:
            df = self.db._select(table="stock_information", columns=["ticker"], is_snp_500=True)
            return pd.DataFrame(df, columns=["ticker"])  # 결과를 DataFrame으로 변환
        except Exception as e:
            logging.error(f"Error fetching S&P500 ticker: {e}")
            return pd.DataFrame()

    async def get_market_ratios(self, market: str) -> Tuple[float, float, float]:
        """최적화된 시장 등락비율 조회"""
        try:
            cache_key = f"{market}_ratio"
            now = datetime.now()

            # 캐시 체크 로직 유지
            if cache_key in self._cache:
                data, timestamp = self._cache[cache_key]
                if now - timestamp < timedelta(seconds=self._cache_timeout):
                    return data

            async with self._lock:
                market_mapping = {"kospi": "KOSPI200", "kosdaq": "KOSDAQ150", "nasdaq": "NASDAQ100", "sp500": "S&P500"}
                market_filter = market_mapping.get(market.lower(), market)

                # 티커 조회 함수 매핑
                ticker_functions = {
                    "KOSPI200": self.get_kospi200_ticker,
                    "KOSDAQ150": self.get_kosdaq150_ticker,
                    "NASDAQ100": self.get_nasdaq100_ticker,
                    "S&P500": self.get_sp500_ticker,
                }

                ticker_func = ticker_functions.get(market_filter)
                df = ticker_func()

                if df.empty:
                    return 0.0, 0.0, 0.0

                tickers = tuple(df["ticker"].tolist())
                table = "stock_kr_1d" if market_filter in ["KOSPI200", "KOSDAQ150"] else "stock_us_1d"

                # 쿼리 최적화: 한 번의 쿼리로 필요한 모든 정보 조회
                optimized_query = text(
                    """
                    WITH latest_date AS (
                        SELECT MAX(Date) as max_date
                        FROM """
                    + table
                    + """
                        WHERE ticker IN :tickers
                        AND Open != 0
                    )
                    SELECT
                        COUNT(CASE WHEN (Close - Open) / Open * 100 > 0.1 THEN 1 END) AS advance,
                        COUNT(CASE WHEN (Close - Open) / Open * 100 < -0.1 THEN 1 END) AS decline,
                        COUNT(CASE WHEN ABS((Close - Open) / Open * 100) <= 0.1 THEN 1 END) AS unchanged,
                        COUNT(*) AS total
                    FROM """
                    + table
                    + """, latest_date
                    WHERE ticker IN :tickers
                    AND Date = latest_date.max_date
                    AND Open != 0
                    """
                )

                result = self.db._execute(optimized_query, {"tickers": tickers})
                row = result.fetchone()

                if not row or row[3] == 0:  # total이 0인 경우
                    return 0.0, 0.0, 0.0

                advance, decline, unchanged, total = row

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
        try:
            tasks = [self._fetch_yf_data_concurrent(symbol, name) for name, symbol in self.symbols.items()]
            await asyncio.gather(*tasks)

            ratio_tasks = [self.get_market_ratios(name) for name in self.symbols.keys()]
            ratio_results = await asyncio.gather(*ratio_tasks)

            indices_summary = {}
            indices_data = {}
            response_time = None
            kr_last_time = None
            us_last_time = None

            for name, ratios in zip(self.symbols.keys(), ratio_results):
                cache_key_daily = f"{name}_daily"
                cache_key_min5 = f"{name}_min5"

                if cache_key_daily in self._cache:
                    daily_data, _ = self._cache[cache_key_daily]
                    min5_data, _ = self._cache.get(cache_key_min5, ({}, None))

                    change = daily_data["close"] - daily_data["open"]
                    change_percent = round((change / daily_data["open"]) * 100, 2) if daily_data["open"] != 0 else 0.00

                    rise_ratio, fall_ratio, unchanged_ratio = ratios

                    if name in ["kospi", "kosdaq"]:
                        is_open = get_time_checker("KR")
                        if is_open and min5_data:
                            last_timestamp = list(min5_data.keys())[-1] if min5_data else None
                            if last_timestamp:
                                kr_time = datetime.strptime(last_timestamp, "%Y-%m-%d %H:%M:%S")
                                kr_last_time = kr_time.strftime("%H:%M")
                                response_time = kr_last_time
                    else:  # nasdaq, sp500
                        is_open = get_time_checker("US")
                        if min5_data:
                            last_timestamp = list(min5_data.keys())[-1] if min5_data else None
                            if last_timestamp:
                                us_time = datetime.strptime(last_timestamp, "%Y-%m-%d %H:%M:%S")
                                us_time = us_time.replace(tzinfo=ZoneInfo("America/New_York"))
                                kr_time = us_time.astimezone(ZoneInfo("Asia/Seoul"))
                                us_last_time = kr_time.strftime("%H:%M")
                                if not get_time_checker("KR"):  # 한국 시장이 닫혀있을 때
                                    response_time = us_last_time

                    indices_summary[name] = IndexSummary(
                        prev_close=daily_data["close"],
                        change=round(change, 2),
                        change_percent=change_percent,
                        rise_ratio=rise_ratio,
                        fall_ratio=fall_ratio,
                        unchanged_ratio=unchanged_ratio,
                        is_open=is_open,
                    )

                else:
                    indices_summary[name] = IndexSummary(
                        prev_close=0.00,
                        change=0.00,
                        change_percent=0.00,
                        rise_ratio=0.00,
                        fall_ratio=0.00,
                        unchanged_ratio=0.00,
                        is_open=False,
                    )

                indices_data[name] = self._cache.get(cache_key_min5, ({}, None))[0]

            # 시장이 모두 닫혀있을 때 마지막 거래 시간 사용
            if response_time is None:
                current_kr_time = datetime.now(ZoneInfo("Asia/Seoul"))
                current_hour = current_kr_time.hour
                current_minute = current_kr_time.minute
                current_time = current_hour * 60 + current_minute

                # 한국장 종료(15:30) 후 미국장 시작(22:30) 전
                if (15 * 60 + 30) <= current_time < (22 * 60 + 30):
                    response_time = kr_last_time if kr_last_time else "15:30"
                # 미국장 종료(06:00) 후 한국장 시작(09:00) 전
                else:
                    response_time = us_last_time if us_last_time else "06:00"

            return IndicesData(
                status_code=200,
                message="데이터를 성공적으로 조회했습니다.",
                time=response_time,  # HH:MM 포맷의 시간
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
                time=datetime.now(ZoneInfo("Asia/Seoul")).strftime("%H:%M"),  # HH:MM 포맷으로 변경
                kospi=empty_summary,
                kosdaq=empty_summary,
                nasdaq=empty_summary,
                sp500=empty_summary,
                data=None,
            )

    def get_nasdaq_ticker(self):
        """stock_indices 테이블에서 나스닥 데이터 조회"""
        try:
            result = self.db._select(
                table="stock_indices",
                columns=["ticker", "rise_ratio", "rise_soft_ratio", "fall_ratio", "fall_soft_ratio", "unchanged_ratio"],
                ticker="nasdaq",
            )

            if not result:
                return {"ticker": "nasdaq", "상승": 0.0, "하락": 0.0, "보합": 0.0}

            # 첫 번째 행의 데이터 사용
            row = result[0]

            # 상승 = 급상승 + 약상승
            total_rise = float(row[1]) + float(row[2])  # rise_ratio + rise_soft_ratio

            # 하락 = 급하락 + 약하락
            total_fall = float(row[3]) + float(row[4])  # fall_ratio + fall_soft_ratio

            # 보합은 그대로
            unchanged = float(row[5])  # unchanged_ratio

            return {
                "ticker": row[0],
                "상승": round(total_rise, 2),
                "하락": round(total_fall, 2),
                "보합": round(unchanged, 2),
            }

        except Exception as e:
            logging.error(f"Error in get_nasdaq_ticker: {str(e)}")
            return {"ticker": "nasdaq", "상승": 0.0, "하락": 0.0, "보합": 0.0}

    def get_snp500_ticker(self):
        """stock_indices 테이블에서 S&P 500 데이터 조회"""
        try:
            result = self.db._select(
                table="stock_indices",
                columns=["ticker", "rise_ratio", "rise_soft_ratio", "fall_ratio", "fall_soft_ratio", "unchanged_ratio"],
                ticker="sp500",
            )

            if not result:
                return {"ticker": "sp500", "상승": 0.0, "하락": 0.0, "보합": 0.0}

            # 첫 번째 행의 데이터 사용
            row = result[0]

            # 상승 = 급상승 + 약상승
            total_rise = float(row[1]) + float(row[2])  # rise_ratio + rise_soft_ratio

            # 하락 = 급하락 + 약하락
            total_fall = float(row[3]) + float(row[4])  # fall_ratio + fall_soft_ratio

            # 보합은 그대로
            unchanged = float(row[5])  # unchanged_ratio

            return {
                "ticker": row[0],
                "상승": round(total_rise, 2),
                "하락": round(total_fall, 2),
                "보합": round(unchanged, 2),
            }

        except Exception as e:
            logging.error(f"Error in get_snp500_ticker: {str(e)}")
            return {"ticker": "sp500", "상승": 0.0, "하락": 0.0, "보합": 0.0}
