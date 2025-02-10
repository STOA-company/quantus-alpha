import logging
from typing import Tuple
import asyncio
from datetime import datetime, timedelta
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
            cls._instance = super(StockIndicesService, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.db = database
        self.markets = ["kospi", "kosdaq", "nasdaq", "sp500"]
        self._cache = {}
        self._lock = asyncio.Lock()
        self._initialized = True

    def get_market_ratios(self, market: str) -> Tuple[float, float, float]:
        try:
            cache_key = f"{market}_ratio"
            now = datetime.now(utc_tz).astimezone(korea_tz)
            is_market_open = check_market_status("KR") if market in ["kospi", "kosdaq"] else check_market_status("US")

            if cache_key in self._cache:
                data, timestamp = self._cache[cache_key]
                cache_age = (now - timestamp).total_seconds()
                if not is_market_open or cache_age < 60:
                    return data

            result = self.db._select(
                table="stock_indices",
                columns=["ticker", "rise_ratio", "rise_soft_ratio", "fall_ratio", "fall_soft_ratio", "unchanged_ratio"],
                ticker=market,
            )

            if not result:
                return 0.0, 0.0, 0.0

            row = result[0]
            rise_ratio = float(row.rise_ratio + row.rise_soft_ratio)
            fall_ratio = float(row.fall_ratio + row.fall_soft_ratio)
            unchanged_ratio = float(row.unchanged_ratio)

            ratios = (
                round(rise_ratio, 2),
                round(fall_ratio, 2),
                round(unchanged_ratio, 2),
            )

            self._cache[cache_key] = (ratios, now)
            return ratios

        except Exception as e:
            logger.error(f"Error in get_market_ratios for {market}: {str(e)}")
            return 0.0, 0.0, 0.0

    def _fetch_market_data(self, market: str):
        try:
            cache_key = f"{market}_data"
            now = datetime.now(utc_tz).astimezone(korea_tz)
            is_market_open = check_market_status("KR") if market in ["kospi", "kosdaq"] else check_market_status("US")

            if cache_key in self._cache:
                cached_data, timestamp = self._cache[cache_key]
                cache_age = (now - timestamp).total_seconds()
                if not is_market_open or cache_age < 60:
                    return cached_data

            today = now.date()

            result = self.db._select(
                table="stock_indices_1m",
                columns=["date", "open", "high", "low", "close", "volume", "change", "change_rate"],
                order="date",
                ascending=True,
                ticker=market,
                date__gte=today.strftime("%Y-%m-%d 00:00:00"),
                date__lt=(today + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00"),
            )

            prev_close = float(result[-1].close) if result else 0.0
            min1_data = {}
            latest_close = 0.0
            latest_change = 0.0
            latest_change_rate = 0.0

            for row in result:
                timestamp = row.date.strftime("%Y-%m-%d %H:%M:%S")
                min1_data[timestamp] = TimeData(
                    open=round(float(row.open), 2),
                    high=round(float(row.high), 2),
                    low=round(float(row.low), 2),
                    close=round(float(row.close), 2),
                    volume=round(float(row.volume), 2),
                )
                latest_close = float(row.close)
                latest_change = float(row.change)
                latest_change_rate = float(row.change_rate)

            market_data = {
                "daily": {
                    "prev_close": round(prev_close, 2),
                    "close": round(latest_close, 2),
                    "change": round(latest_change, 2),
                    "change_percent": round(latest_change_rate, 2),
                },
                "min1": min1_data,
            }

            self._cache[cache_key] = (market_data, now)
            return market_data

        except Exception as e:
            logger.error(f"Error fetching data for {market}: {e}")
            return None

    async def get_indices_data(self) -> IndicesData:
        try:
            data_tasks = [asyncio.to_thread(self._fetch_market_data, market) for market in self.markets]
            ratio_tasks = [asyncio.to_thread(self.get_market_ratios, market) for market in self.markets]

            results, ratios = await asyncio.gather(asyncio.gather(*data_tasks), asyncio.gather(*ratio_tasks))

            indices_summary = {}
            indices_data = {}

            for market, result in zip(self.markets, results):
                if result:
                    daily_data = result["daily"]
                    min1_data = result["min1"]

                    is_open = check_market_status("KR") if market in ["kospi", "kosdaq"] else check_market_status("US")

                    rise_ratio, fall_ratio, unchanged_ratio = ratios[self.markets.index(market)]
                    indices_summary[market] = IndexSummary(
                        prev_close=daily_data["prev_close"],
                        change=daily_data["change"],
                        change_percent=daily_data["change_percent"],
                        rise_ratio=rise_ratio,
                        fall_ratio=fall_ratio,
                        unchanged_ratio=unchanged_ratio,
                        is_open=is_open,
                    )
                    indices_data[market] = min1_data

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
