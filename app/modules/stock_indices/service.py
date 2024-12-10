from functools import lru_cache
import asyncio
from datetime import datetime
import pytz
import yfinance as yf
from pykrx import stock


class StockIndicesService:
    def __init__(self):
        self.indices = {"KOSPI": {"index": "^KS11", "components": []}, "KOSDAQ": {"index": "^KQ11", "components": []}}
        self.korea_tz = pytz.timezone("Asia/Seoul")
        self._cache = {}
        self._cache_duration = 300  # 컴포넌트 리스트만 5분 캐시
        self._update_components()
        self._data_cache = {}
        self._data_cache_duration = 30  # 30초 캐시

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
            kospi_tickers = stock.get_market_ticker_list(today, market="KOSPI")
            kosdaq_tickers = stock.get_market_ticker_list(today, market="KOSDAQ")

            self.indices["KOSPI"]["components"] = [f"{ticker}.KS" for ticker in kospi_tickers]
            self.indices["KOSDAQ"]["components"] = [f"{ticker}.KQ" for ticker in kosdaq_tickers]

            self._set_components_cache(cache_key, self.indices)
        except Exception as e:
            print(f"Error updating components: {str(e)}")

    async def get_indices_data(self):
        result = {}

        async def fetch_market_data(market_name, market_info):
            try:
                time_series = await self._get_index_data(market_info["index"])
                if time_series:
                    market_status = await self._get_market_status(market_info["components"])
                    return market_name, {"time_series": time_series, "market_status": market_status}
            except Exception as e:
                print(f"Error fetching {market_name}: {str(e)}")
            return None

        tasks = [fetch_market_data(market_name, market_info) for market_name, market_info in self.indices.items()]

        results = await asyncio.gather(*tasks)
        result = {name: data for item in results if item and (name := item[0]) and (data := item[1])}

        return result

    async def _get_market_status(self, tickers):
        today = datetime.now(self.korea_tz).strftime("%Y%m%d")

        try:
            df = stock.get_market_ohlcv_by_ticker(today)

            up = (df["등락률"] > 0).sum()
            down = (df["등락률"] < 0).sum()
            unchanged = (df["등락률"] == 0).sum()
            total = len(df)

            return {
                "상승": f"{round((up / total * 100), 1)}%",
                "하락": f"{round((down / total * 100), 1)}%",
                "보합": f"{round((unchanged / total * 100), 1)}%",
                "총합": f"{round((total / total * 100), 1)}%",
            }

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
            index = yf.Ticker(ticker)
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
