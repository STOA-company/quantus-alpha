import logging
import yfinance as yf
import requests
from typing import Tuple
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from app.database.crud import database
from app.modules.stock_indices.schemas import IndexSummary, IndicesData, IndicesResponse, TimeData
from app.utils.date_utils import check_market_status
from app.core.config import korea_tz, utc_tz

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class StockIndicesService:
    _instance = None
    _initialized = False

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
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._lock = asyncio.Lock()
        self.session = requests.Session()
        self._initialized = True

    async def get_market_ratios(self, market: str) -> Tuple[float, float, float]:
        """시장 등락비율 조회"""
        try:
            cache_key = f"{market}_ratio"
            now = datetime.now(utc_tz).astimezone(korea_tz)
            is_market_open = check_market_status("KR") if market in ["kospi", "kosdaq"] else check_market_status("US")

            # 장 마감시에는 캐시 유지, 장 중에는 1분마다 갱신
            if cache_key in self._cache:
                data, timestamp = self._cache[cache_key]
                cache_age = (now - timestamp).total_seconds()
                if not is_market_open or cache_age < 60:
                    return data

            async with self._lock:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    self._executor,
                    lambda: self.db._select(
                        table="stock_indices",
                        columns=[
                            "ticker",
                            "rise_ratio",
                            "rise_soft_ratio",
                            "fall_ratio",
                            "fall_soft_ratio",
                            "unchanged_ratio",
                        ],
                        ticker=market,
                    ),
                )

                if not result:
                    return 0.0, 0.0, 0.0

                row = result[0]
                _, rise_ratio, rise_soft_ratio, fall_ratio, fall_soft_ratio, unchanged_ratio = row

                ratios = (
                    round(float(rise_ratio + rise_soft_ratio), 2),
                    round(float(fall_ratio + fall_soft_ratio), 2),
                    round(float(unchanged_ratio), 2),
                )

                self._cache[cache_key] = (ratios, now)
                return ratios

        except Exception as e:
            logger.error(f"Error in get_market_ratios for {market}: {str(e)}")
            return 0.0, 0.0, 0.0

    async def _fetch_yf_data_concurrent(self, symbol: str, name: str):
        """비동기로 yfinance 데이터 조회"""
        try:
            cache_key = f"{name}_data"
            now = datetime.now(utc_tz).astimezone(korea_tz)
            is_market_open = check_market_status("KR") if name in ["kospi", "kosdaq"] else check_market_status("US")

            # 장 마감시에는 캐시 유지, 장 중에는 1분마다 갱신
            if cache_key in self._cache:
                cached_data, timestamp = self._cache[cache_key]
                cache_age = (now - timestamp).total_seconds()

                if not is_market_open or cache_age < 60:
                    return cached_data

            async def fetch_data():
                loop = asyncio.get_event_loop()
                ticker = yf.Ticker(symbol, session=self.session)

                def fetch():
                    daily_df = ticker.history(period="5d")
                    min5_df = ticker.history("1d", interval="5m")
                    return daily_df, min5_df

                return await loop.run_in_executor(self._executor, fetch)

            daily_df, min5_df = await fetch_data()

            # 데이터 처리
            if not daily_df.empty:
                latest_data = daily_df.iloc[-1]
                prev_data = daily_df.iloc[-2] if len(daily_df) > 1 else latest_data
                daily_data = {
                    "open": round(float(latest_data["Open"]), 2),
                    "close": round(float(latest_data["Close"]), 2),
                    "prev_close": round(float(prev_data["Close"]), 2),
                }
            else:
                daily_data = {"open": 0.0, "close": 0.0, "prev_close": 0.0}

            min5_data = {}
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

            market_data = {"daily": daily_data, "min5": min5_data}
            self._cache[cache_key] = (market_data, now)
            return market_data

        except Exception as e:
            logger.error(f"Error fetching data for {name}: {e}")
            return None

    async def get_indices_data(self) -> IndicesData:
        """지수 데이터 조회"""
        try:
            data_tasks = [self._fetch_yf_data_concurrent(symbol, name) for name, symbol in self.symbols.items()]
            ratio_tasks = [self.get_market_ratios(name) for name in self.symbols.keys()]

            results = await asyncio.gather(*data_tasks)
            ratios = await asyncio.gather(*ratio_tasks)

            indices_summary = {}
            indices_data = {}

            for name, result in zip(self.symbols.keys(), results):
                if result:
                    daily_data = result["daily"]
                    min5_data = result["min5"]

                    is_open = check_market_status("KR") if name in ["kospi", "kosdaq"] else check_market_status("US")

                    change = daily_data["close"] - daily_data["prev_close"]
                    change_percent = (
                        round((change / daily_data["prev_close"]) * 100, 2) if daily_data["prev_close"] != 0 else 0.00
                    )

                    rise_ratio, fall_ratio, unchanged_ratio = ratios[list(self.symbols.keys()).index(name)]
                    indices_summary[name] = IndexSummary(
                        prev_close=daily_data["close"],
                        change=round(change, 2),
                        change_percent=change_percent,
                        rise_ratio=rise_ratio,
                        fall_ratio=fall_ratio,
                        unchanged_ratio=unchanged_ratio,
                        is_open=is_open,
                        timestamp=daily_data["timestamp"],
                    )
                    indices_data[name] = min5_data

            return IndicesData(
                status_code=200,
                message="데이터를 성공적으로 조회했습니다.",
                time=datetime.now(korea_tz).strftime("%H:%M"),
                kospi=indices_summary.get("kospi", IndexSummary()),
                kosdaq=indices_summary.get("kosdaq", IndexSummary()),
                nasdaq=indices_summary.get("nasdaq", IndexSummary()),
                sp500=indices_summary.get("sp500", IndexSummary()),
                data=IndicesResponse(
                    kospi=indices_data.get("kospi", {}),
                    kosdaq=indices_data.get("kosdaq", {}),
                    nasdaq=indices_data.get("nasdaq", {}),
                    sp500=indices_data.get("sp500", {}),
                ),
            )

        except Exception as e:
            logger.error(f"Error in get_indices_data: {e}")
            return IndicesData(
                status_code=404,
                message=f"데이터 조회 중 오류가 발생했습니다: {e}",
                time=datetime.now(korea_tz).strftime("%H:%M"),
                kospi=IndexSummary(),
                kosdaq=IndexSummary(),
                nasdaq=IndexSummary(),
                sp500=IndexSummary(),
                data=None,
            )
