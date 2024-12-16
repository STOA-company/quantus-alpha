from functools import lru_cache
import asyncio
from datetime import datetime
import pytz
import yfinance as yf
from pykrx import stock
import json
from pathlib import Path


class StockIndicesService:
    def __init__(self):
        self.korea_tz = pytz.timezone("Asia/Seoul")
        self._cache = {}
        self._cache_duration = 300
        self._data_cache = {}
        self._data_cache_duration = 30
        self._market_status_cache = {}  # 시장 상태 캐시 추가
        self._market_status_cache_duration = 60  # 1분
        self._yf_tickers = {}
        self.indices = self._load_indices()
        self._update_components()

    def _load_indices(self):
        try:
            config_path = Path(__file__).parent.parent.parent / "config" / "indices.json"
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)

            indices = {}
            for name, info in config["indices"].items():
                indices[name] = {
                    "index": info["index"],
                    "components": [],
                    "market_code": info["market_code"],
                    "suffix": info["suffix"],
                }
            return indices
        except Exception as e:
            print(f"Error loading indices config: {str(e)}")
            # 기본값 반환
            return {
                "KOSPI": {"index": "^KS11", "components": [], "market_code": "KOSPI", "suffix": ".KS"},
                "KOSDAQ": {"index": "^KQ11", "components": [], "market_code": "KOSDAQ", "suffix": ".KQ"},
            }

    def _get_components_cache(self, key):
        if key in self._cache:
            data, timestamp = self._cache[key]
            if datetime.now().timestamp() - timestamp < self._cache_duration:
                return data
        return None

    def _set_components_cache(self, key, data):
        self._cache[key] = (data, datetime.now().timestamp())

    def _update_components(self):
        cache_key = "components"
        cached_data = self._get_components_cache(cache_key)
        if cached_data:
            self.indices = cached_data
            return

        today = datetime.now().strftime("%Y%m%d")
        try:
            for market_name, market_info in self.indices.items():
                tickers = stock.get_market_ticker_list(today, market=market_info["market_code"])
                market_info["components"] = [f"{ticker}{market_info['suffix']}" for ticker in tickers]

            self._set_components_cache(cache_key, self.indices)
        except Exception as e:
            print(f"Error updating components: {str(e)}")

    def _get_yf_ticker(self, ticker):
        if ticker not in self._yf_tickers:
            self._yf_tickers[ticker] = yf.Ticker(ticker)
        return self._yf_tickers[ticker]

    async def get_indices_data(self):
        now = datetime.now(self.korea_tz)
        cache_key = f"all_indices_{now.strftime('%Y%m%d_%H%M')}"

        # 전체 데이터 캐시 확인
        if cache_key in self._data_cache:
            data, timestamp = self._data_cache[cache_key]
            if now.timestamp() - timestamp < self._data_cache_duration:
                return data

        # 모든 지수 데이터를 동시에 가져오기
        time_series_tasks = []
        market_status_tasks = []

        for market_name, market_info in self.indices.items():
            time_series_tasks.append(self._get_index_data(market_info["index"]))
            market_status_tasks.append(self._get_market_status(market_info["components"]))

        # 병렬로 실행
        time_series_results, market_status_results = await asyncio.gather(
            asyncio.gather(*time_series_tasks), asyncio.gather(*market_status_tasks)
        )

        # 결과 조합
        result = {}
        for i, (market_name, _) in enumerate(self.indices.items()):
            if time_series_results[i] and market_status_results[i]:
                result[market_name] = {"time_series": time_series_results[i], "market_status": market_status_results[i]}

        # 결과 캐싱
        self._data_cache[cache_key] = (result, now.timestamp())
        return result

    async def _get_market_status(self, tickers):
        now = datetime.now(self.korea_tz)
        market = "KOSPI" if tickers[0].endswith(".KS") else "KOSDAQ"
        cache_key = f"market_status_{market}_{now.strftime('%Y%m%d_%H%M')}"

        # 시장 상태 캐시 확인
        if cache_key in self._market_status_cache:
            data, timestamp = self._market_status_cache[cache_key]
            if now.timestamp() - timestamp < self._market_status_cache_duration:
                return data

        try:
            today = now.strftime("%Y%m%d")
            df = stock.get_market_ohlcv_by_ticker(today, market=market)

            if len(df) == 0:
                raise Exception("No data available")

            up = (df["등락률"] > 0).sum()
            down = (df["등락률"] < 0).sum()
            unchanged = (df["등락률"] == 0).sum()
            total = len(df)

            result = {
                "상승": f"{round((up / total * 100), 1)}%",
                "하락": f"{round((down / total * 100), 1)}%",
                "보합": f"{round((unchanged / total * 100), 1)}%",
                "총합": "100.0%",
            }

            # 결과 캐싱
            self._market_status_cache[cache_key] = (result, now.timestamp())
            return result

        except Exception as e:
            print(f"Error getting market status: {str(e)}")
            return {
                "상승": "0.0%",
                "하락": "0.0%",
                "보합": "0.0%",
                "총합": "0.0%",
            }

    @lru_cache(maxsize=32)
    async def _get_index_data(self, ticker):
        cache_key = f"index_data_{ticker}"
        now = datetime.now(self.korea_tz)

        # 캐시된 데이터 확인
        if cache_key in self._data_cache:
            data, timestamp = self._data_cache[cache_key]
            if now.timestamp() - timestamp < self._data_cache_duration:
                return data

        today_start = now.replace(hour=9, minute=0, second=0, microsecond=0)
        today_end = now

        try:
            index = self._get_yf_ticker(ticker)
            df = index.history(start=today_start, end=today_end, interval="5m")

            if len(df) == 0:
                return None

            time_series = []
            first_price = None

            for i in range(len(df)):
                kr_time = df.index[i].tz_convert("Asia/Seoul")
                if not (9 <= kr_time.hour <= 15) or (kr_time.hour == 15 and kr_time.minute > 30):
                    continue

                current_value = round(float(df["Close"].iloc[i]), 2)
                if first_price is None:
                    first_price = current_value

                current_change = round(((current_value - first_price) / first_price * 100), 2)
                change_str = f"+{current_change}%" if current_change > 0 else f"{current_change}%"

                time_series.append({"time": kr_time.strftime("%H:%M"), "value": current_value, "change": change_str})

            # 결과 캐싱
            self._data_cache[cache_key] = (time_series, now.timestamp())
            return time_series

        except Exception as e:
            print(f"Error getting index data for {ticker}: {str(e)}")
            return None

    async def get_indices_data_fifteen(self):
        result = {}

        async def fetch_market_data(market_name, market_info):
            try:
                time_series = await self._get_index_data_fifteen(market_info["index"])
                if time_series:
                    return market_name, time_series
            except Exception as e:
                print(f"Error fetching {market_name}: {str(e)}")
            return None

        tasks = [fetch_market_data(market_name, market_info) for market_name, market_info in self.indices.items()]

        results = await asyncio.gather(*tasks)
        result = {name: data for item in results if item and (name := item[0]) and (data := item[1])}

        return result

    async def _get_index_data_fifteen(self, ticker):
        now = datetime.now(self.korea_tz)
        today_start = now.replace(hour=9, minute=0, second=0, microsecond=0)
        today_end = now.replace(hour=15, minute=30, second=0, microsecond=0)

        try:
            index = yf.Ticker(ticker)
            df = index.history(start=today_start, end=today_end, interval="15m")

            if len(df) == 0:
                return None

            time_series = []
            first_price = None

            for i in range(len(df)):
                kr_time = df.index[i].tz_convert("Asia/Seoul")

                if not (9 <= kr_time.hour <= 15) or (kr_time.hour == 15 and kr_time.minute > 30):
                    continue

                current_value = round(float(df["Close"].iloc[i]), 2)

                if first_price is None:
                    first_price = current_value

                current_change = round(((current_value - first_price) / first_price * 100), 2)
                time_series.append({"time": kr_time.strftime("%H:%M"), "value": current_value, "change": current_change})

            return time_series

        except Exception as e:
            print(f"Error getting index data for {ticker}: {str(e)}")
            return None
