import logging
from sqlalchemy import text
import yfinance as yf
import requests
from typing import Tuple, Optional
import asyncio
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from app.database.crud import database
from app.modules.stock_indices.schemas import IndexSummary, IndicesData, IndicesResponse, TimeData
from app.utils.date_utils import get_time_checker
from app.core.config import korea_tz, utc_tz


class StockIndicesService:
    _instance = None
    _initialized = False
    _background_task = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.db = database
        self.symbols = {"kospi": "^KS11", "kosdaq": "^KQ11", "nasdaq": "^IXIC", "sp500": "^GSPC"}
        self._cache = {}
        self._ticker_cache = {}
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._lock = asyncio.Lock()
        self._background_task_running = False
        self.session = requests.Session()
        self._nasdaq_lock = asyncio.Lock()
        self._sp500_lock = asyncio.Lock()

        self._initialized = True

    async def initialize(self):
        """비동기 초기화"""
        if not self._background_task:
            self._background_task = asyncio.create_task(self._update_cache_background())

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

                await asyncio.sleep(120)
        finally:
            self._background_task_running = False

    async def _fetch_yf_data_concurrent(self, symbol: str, name: str):
        """비동기로 yfinance 데이터 조회"""
        try:
            cache_key_daily = f"{name}_daily"
            cache_key_min5 = f"{name}_min5"
            now = datetime.now(utc_tz).astimezone(korea_tz)

            # 시장별 캐시 체크
            is_market_open = get_time_checker("KR") if name in ["kospi", "kosdaq"] else get_time_checker("US")

            # 캐시가 있고 유효한 경우 그대로 반환
            if cache_key_daily in self._cache and cache_key_min5 in self._cache:
                cached_daily, timestamp_daily = self._cache[cache_key_daily]
                cached_min5, timestamp_min5 = self._cache[cache_key_min5]

                # 시장이 열려있을 때만 timeout 체크
                if is_market_open:
                    timeout = self._get_cache_timeout(name)
                    if now - timestamp_daily < timedelta(seconds=timeout) and now - timestamp_min5 < timedelta(
                        seconds=timeout
                    ):
                        logging.debug(f"Using cached data for {name}")
                        return
                else:
                    # 장 마감 시에는 캐시 유지
                    logging.debug(f"Market closed, using cached data for {name}")
                    return

            # 캐시 미스 또는 캐시 만료 시에만 데이터 조회
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

            # 데이터 처리 및 캐시 저장
            if not daily_df.empty:
                valid_data = daily_df[daily_df["Open"] != 0]
                if not valid_data.empty:
                    latest_data = valid_data.iloc[-1]
                    daily_data = {
                        "open": round(float(latest_data["Open"]), 2),
                        "close": round(float(latest_data["Close"]), 2),
                    }
                    self._cache[cache_key_daily] = (daily_data, now)
                    logging.info(f"Updated daily cache for {name}")

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

    def _get_cache_timeout(self, market: str) -> int:
        """
        시장별 지수 데이터 캐시 타임아웃 설정
        """
        if market.lower() in ["kospi", "kosdaq"]:
            now = datetime.now(korea_tz)
            current_time = now.hour * 60 + now.minute

            # 장 시작 직전 30분은 짧은 캐시 타임아웃 적용
            if 510 <= current_time < 540:  # 8:30 ~ 9:00
                return 10
            # 장 중
            elif 540 <= current_time <= 930:  # 9:00 ~ 15:30
                return 10
            # 그 외 시간
            return 3600

        else:  # nasdaq, sp500
            now = datetime.now(utc_tz)
            current_time = now.hour * 60 + now.minute

            # 장 시작 직전 30분은 짧은 캐시 타임아웃 적용
            if 780 <= current_time < 810:  # 13:00 ~ 13:30 UTC
                return 10
            # 장 중
            elif 810 <= current_time <= 1200:  # 13:30 ~ 20:00 UTC
                return 10

            return 3600

    def _get_ticker_cache_timeout(self, market: str) -> int:
        """시장별 티커 캐시 타임아웃 설정"""
        is_open = get_time_checker("KR") if market in ["KOSPI200", "KOSDAQ150"] else get_time_checker("US")
        return 300 if is_open else 3600  # 장중 5분, 장마감 1시간

    async def _get_cached_tickers(self, market: str) -> pd.DataFrame:
        """캐시된 티커 정보 조회"""
        try:
            cache_key = f"tickers_{market}"
            now = datetime.now(utc_tz).astimezone(korea_tz)

            # Cache hit
            if cache_key in self._ticker_cache:
                data, timestamp = self._ticker_cache[cache_key]
                timeout = self._get_ticker_cache_timeout(market)
                if now - timestamp < timedelta(seconds=timeout):
                    logging.debug(f"Cache hit for {market} tickers")
                    return data

            # Cache miss
            async with self._lock:
                # 락 획득 후 캐시 재확인
                if cache_key in self._ticker_cache:
                    data, timestamp = self._ticker_cache[cache_key]
                    timeout = self._get_ticker_cache_timeout(market)
                    if now - timestamp < timedelta(seconds=timeout):
                        logging.debug(f"Cache hit after lock for {market} tickers")
                        return data

                # 티커 조회
                query_map = {
                    "KOSPI200": ("is_kospi_200", True),
                    "KOSDAQ150": ("is_kosdaq_150", True),
                    "NASDAQ100": ("is_nasdaq_100", True),
                    "S&P500": ("is_snp_500", True),
                }

                if market not in query_map:
                    logging.error(f"Invalid market: {market}")
                    return pd.DataFrame()

                column, value = query_map[market]
                result = self.db._select(table="stock_information", columns=["ticker"], **{column: value})

                df = pd.DataFrame(result, columns=["ticker"])
                if not df.empty:
                    self._ticker_cache[cache_key] = (df, now)
                    logging.info(f"Updated ticker cache for {market} with {len(df)} entries")
                else:
                    logging.warning(f"No tickers found for {market}")

                return df

        except Exception as e:
            logging.error(f"Error in _get_cached_tickers for {market}: {e}")
            return pd.DataFrame()

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
            return pd.DataFrame(df, columns=["ticker"])
        except Exception as e:
            logging.error(f"Error fetching S&P500 ticker: {e}")
            return pd.DataFrame()

    async def get_market_ratios(self, market: str) -> Tuple[float, float, float]:
        """시장 등락비율 조회"""
        try:
            cache_key = f"{market}_ratio"
            now = datetime.now(utc_tz).astimezone(korea_tz)

            if cache_key in self._cache:
                data, timestamp = self._cache[cache_key]
                if now - timestamp < timedelta(seconds=self._get_cache_timeout(market)):
                    return data

            market_mapping = {"kospi": "KOSPI200", "kosdaq": "KOSDAQ150", "nasdaq": "NASDAQ100", "sp500": "S&P500"}
            market_filter = market_mapping.get(market.lower())

            df = await self._get_cached_tickers(market_filter)
            if df.empty:
                return 0.0, 0.0, 0.0

            tickers = tuple(df["ticker"].tolist())
            table = "stock_kr_1d" if market_filter in ["KOSPI200", "KOSDAQ150"] else "stock_us_1d"

            async with self._lock:
                query = f"""
                    WITH latest_date AS (
                        SELECT MAX(Date) as max_date
                        FROM {table}
                        WHERE ticker IN :tickers
                        AND Open != 0
                    )
                    SELECT
                        COUNT(CASE WHEN (Close - Open) / Open * 100 > 0.1 THEN 1 END) AS advance,
                        COUNT(CASE WHEN (Close - Open) / Open * 100 < -0.1 THEN 1 END) AS decline,
                        COUNT(CASE WHEN ABS((Close - Open) / Open * 100) <= 0.1 THEN 1 END) AS unchanged,
                        COUNT(*) AS total
                    FROM {table}, latest_date
                    WHERE ticker IN :tickers
                    AND Date = latest_date.max_date
                    AND Open != 0
                """

                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    self._executor, lambda: self.db._execute(text(query), {"tickers": tickers})
                )
                row = result.fetchone()

                if not row or row[3] == 0:
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

    async def _fetch_all_data(self):
        """모든 데이터 수집"""
        tasks = [self._fetch_yf_data_concurrent(symbol, name) for name, symbol in self.symbols.items()]
        await asyncio.gather(*tasks)

        ratio_tasks = [self.get_market_ratios(name) for name in self.symbols.keys()]
        await asyncio.gather(*ratio_tasks)

    def _build_response_from_cache(self) -> IndicesData:
        """캐시된 데이터로 응답 구성"""
        indices_summary = {}
        indices_data = {}
        kr_last_time = None
        us_last_time = None

        for name in self.symbols.keys():
            cache_key_daily = f"{name}_daily"
            cache_key_min5 = f"{name}_min5"
            cache_key_ratio = f"{name}_ratio"

            if cache_key_daily in self._cache:
                daily_data, _ = self._cache[cache_key_daily]
                min5_data, _ = self._cache.get(cache_key_min5, ({}, None))
                ratios, _ = self._cache.get(cache_key_ratio, ((0.0, 0.0, 0.0), None))

                # 마지막 거래 시간 저장
                if min5_data:
                    last_timestamp = list(min5_data.keys())[-1] if min5_data else None
                    if last_timestamp:
                        last_time = datetime.strptime(last_timestamp, "%Y-%m-%d %H:%M:%S")
                        if name in ["kospi", "kosdaq"]:
                            kr_last_time = last_time.strftime("%H:%M")
                        else:
                            us_last_time = last_time.strftime("%H:%M")

                change = daily_data["close"] - daily_data["open"]
                change_percent = round((change / daily_data["open"]) * 100, 2) if daily_data["open"] != 0 else 0.00

                rise_ratio, fall_ratio, unchanged_ratio = ratios
                is_open = get_time_checker("KR") if name in ["kospi", "kosdaq"] else get_time_checker("US")

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

        response_time = self._determine_response_time(kr_last_time, us_last_time)

        return IndicesData(
            status_code=200,
            message="데이터를 성공적으로 조회했습니다.",
            time=response_time,
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

    def _determine_response_time(self, kr_last_time: Optional[str], us_last_time: Optional[str]) -> str:
        """응답 시간 결정"""
        current_kr_time = datetime.now(utc_tz).astimezone(korea_tz)
        current_minutes = current_kr_time.hour * 60 + current_kr_time.minute

        kr_is_open = get_time_checker("KR")
        us_is_open = get_time_checker("US")

        if kr_is_open and kr_last_time:
            return kr_last_time
        elif us_is_open and us_last_time:
            return us_last_time
        else:
            return "15:30" if 15 * 60 + 30 <= current_minutes < 23 * 60 + 30 else "06:00"

    async def get_indices_data(self) -> IndicesData:
        """지수 데이터 조회"""
        try:
            now = datetime.now(utc_tz).astimezone(korea_tz)
            need_update = False

            # 캐시 유효성 검사
            for name in self.symbols.keys():
                cache_keys = [f"{name}_daily", f"{name}_min5", f"{name}_ratio"]
                for key in cache_keys:
                    if key not in self._cache:
                        need_update = True
                        break
                    _, timestamp = self._cache[key]
                    if now - timestamp >= timedelta(seconds=self._get_cache_timeout(name)):
                        need_update = True
                        break

            if need_update:
                await self._fetch_all_data()

            return self._build_response_from_cache()

        except Exception as e:
            empty_summary = IndexSummary(
                prev_close=0.00,
                change=0.00,
                change_percent=0.00,
                rise_ratio=0.00,
                fall_ratio=0.00,
                unchanged_ratio=0.00,
                is_open=False,
            )
            return IndicesData(
                status_code=404,
                message=f"데이터 조회 중 오류가 발생했습니다: {e}",
                time=datetime.now(korea_tz).strftime("%H:%M"),
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
