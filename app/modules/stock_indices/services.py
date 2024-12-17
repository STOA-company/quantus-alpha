import yfinance as yf
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
        self._market_cache = {}

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
            cache_key = f"{name}_data"
            now = datetime.now()

            if cache_key in self._cache:
                cached_data, timestamp = self._cache[cache_key]
                if now - timestamp < timedelta(seconds=self._cache_timeout):
                    return name, cached_data["daily"], cached_data["min5"]

            async def fetch_history(period, interval=None):
                try:
                    loop = asyncio.get_event_loop()
                    ticker = yf.Ticker(symbol)

                    def fetch():
                        try:
                            if interval:
                                return ticker.history(period=period, interval=interval)
                            return ticker.history(period=period)
                        except Exception:
                            return pd.DataFrame()

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

                min5_data = {}
                if not min5_df.empty:
                    for index, row in min5_df.iterrows():
                        min5_data[index.strftime("%Y-%m-%d %H:%M:%S")] = TimeData(
                            open=round(float(row["Open"]), 2),
                            high=round(float(row["High"]), 2),
                            low=round(float(row["Low"]), 2),
                            close=round(float(row["Close"]), 2),
                            volume=round(float(row["Volume"]), 2),
                        )

                self._cache[cache_key] = ({"daily": daily_data, "min5": min5_data}, now)
                return name, daily_data, min5_data

            return name, None, {}

        except Exception:
            return name, None, {}

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
                market_mapping = {"kospi": "KOSPI", "kosdaq": "KOSDAQ", "nasdaq": "NASDAQ", "sp500": "S&P500"}
                market_filter = market_mapping.get(market.lower(), market)
                table = "stock_kr_1d" if market_filter in ["KOSPI", "KOSDAQ"] else "stock_us_1d"

                # 최신 날짜 데이터만 조회
                result = self.db._select(
                    table=table,
                    columns=["Date", "Open", "Close"],
                    Market=market_filter,
                    order="Date",
                    ascending=False,
                    limit=100,  # 최근 100개 종목만 조회하여 성능 최적화
                )

                if result:
                    latest_date = result[0].Date

                    # 최신 날짜의 데이터만 필터링
                    latest_data = [row for row in result if row.Date == latest_date and row.Open != 0]

                    if latest_data:
                        # 변화율 계산 및 카운트
                        changes = [((row.Close - row.Open) / row.Open * 100) for row in latest_data]

                        total = len(changes)
                        advance = sum(1 for x in changes if x > 1)
                        decline = sum(1 for x in changes if x < -1)
                        unchanged = sum(1 for x in changes if -1 <= x <= 1)

                        # 비율 계산
                        ratios = (
                            round(advance / total * 100, 2),
                            round(decline / total * 100, 2),
                            round(unchanged / total * 100, 2),
                        )

                        self._cache[cache_key] = (ratios, now)
                        return ratios

                return 0.0, 0.0, 0.0

        except Exception as e:
            print(f"Error in get_market_ratios for {market}: {str(e)}")
            return 0.0, 0.0, 0.0

    async def get_indices_data(self) -> IndicesData:
        """지수 데이터 조회"""
        try:
            tasks = [self._fetch_yf_data_concurrent(symbol, name) for name, symbol in self.symbols.items()]
            results = await asyncio.gather(*tasks)

            ratio_tasks = [self.get_market_ratios(name) for name in self.symbols.keys()]
            ratio_results = await asyncio.gather(*ratio_tasks)

            indices_summary = {}
            indices_data = {}

            for (name, daily_data, min5_data), ratios in zip(results, ratio_results):
                if daily_data:
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

                indices_data[name] = min5_data

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
                message=f"데이터 조회 중 오류가 발생했습니다: {str(e)}",
                kospi=empty_summary,
                kosdaq=empty_summary,
                nasdaq=empty_summary,
                sp500=empty_summary,
                data=None,
            )
