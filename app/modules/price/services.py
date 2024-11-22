import asyncio
from datetime import date, datetime, timedelta
from functools import lru_cache
import logging
from typing import List, Optional, Tuple, Dict
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from app.modules.common.cache import MemoryCache
from app.modules.common.enum import Country, Frequency
from app.modules.common.schemas import BaseResponse
from app.modules.price.schemas import PriceDataItem, ResponsePriceDataItem
from app.database.crud import database

logger = logging.getLogger(__name__)


@dataclass
class PriceServiceConfig:
    """주가 데이터 서비스 설정"""

    FREQUENCY_MAPPING: Dict[Frequency, str] = field(
        default_factory=lambda: {
            Frequency.DAILY: "1d",
            Frequency.MINUTE: "1m",
        }
    )
    CHUNK_SIZE_DAYS: int = 1
    MAX_CONCURRENT_REQUESTS: int = 20

    # 캐시 TTL 설정
    CACHE_TTL: Dict[str, int] = field(
        default_factory=lambda: {
            "ONE_MONTH": 60 * 60 * 24 * 30,
            "ONE_WEEK": 60 * 60 * 24 * 7,
            "ONE_DAY": 60 * 60 * 24,
            "ONE_HOUR": 60 * 60,
        }
    )
    RECENT_DATA_DAYS: int = 4

    # 컬럼 설정
    BASE_COLUMNS: List[str] = field(default_factory=lambda: ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"])
    NUMERIC_COLUMNS: List[str] = field(default_factory=lambda: ["Open", "High", "Low", "Close", "Volume"])
    COUNTRY_SPECIFIC_COLUMNS: Dict[Country, List[str]] = field(
        default_factory=lambda: {
            Country.KR: ["Name"],
            Country.US: [],
        }
    )


class DataProcessor:
    """데이터 처리 클래스"""

    def __init__(self, config: PriceServiceConfig):
        self.config = config

    def preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 전처리"""
        if df.empty:
            return df
        df["Date"] = pd.to_datetime(df["Date"])
        df[self.config.NUMERIC_COLUMNS] = df[self.config.NUMERIC_COLUMNS].apply(pd.to_numeric, errors="coerce")
        return df

    def get_last_day_close(self, df: pd.DataFrame, end_date: date, frequency: Frequency) -> float:
        """전일 종가 계산"""
        try:
            if frequency == Frequency.DAILY:
                prev_day = end_date - timedelta(days=1)
                prev_day_data = df[df["Date"].dt.date == prev_day]
            else:
                prev_day_data = df[df["Date"].dt.date == (end_date - timedelta(days=1))]

            return float(prev_day_data["Close"].iloc[-1]) if not prev_day_data.empty else 0.0

        except Exception as e:
            logger.error(f"Error getting last day close: {str(e)}")
            return 0.0

    def process_price_data(
        self, df: pd.DataFrame, ctry: Country, frequency: Frequency, week52_data: Tuple[float, float], end_date: date
    ) -> ResponsePriceDataItem:
        """DataFrame을 PriceDataItem으로 변환"""
        if df.empty:
            return []

        try:
            week52_highest, week52_lowest = week52_data

            # 가격 변동률 계산
            df["daily_price_change_rate"] = np.round((df["Close"] - df["Open"]) / df["Open"] * 100, decimals=2).fillna(0)

            # 종목명 처리
            df["name"] = "" if ctry != Country.KR else df.get("Name", "").fillna("")

            # 전일 종가 계산
            df["last_day_close"] = self.get_last_day_close(df, end_date, frequency)

            return ResponsePriceDataItem(
                ticker=str(df["Ticker"].iloc[0]),
                name=str(df["name"].iloc[0]),
                week52_highest=week52_highest,
                week52_lowest=week52_lowest,
                last_day_close=float(df["last_day_close"].iloc[0]),
                price_data=self._create_price_data_items(df, frequency),
            )

        except Exception as e:
            logger.error(f"Error processing price data: {str(e)}")
            return None

    def _create_price_data_items(self, df: pd.DataFrame, frequency: Frequency) -> List[PriceDataItem]:
        """PriceDataItem 리스트 생성"""
        return [
            PriceDataItem(
                date=row["Date"],
                ticker=str(row["Ticker"]),
                name=str(row["name"]),
                open=float(row["Open"]),
                high=float(row["High"]),
                low=float(row["Low"]),
                close=float(row["Close"]),
                volume=int(row["Volume"]),
                daily_price_change_rate=float(row["daily_price_change_rate"]) if frequency == Frequency.DAILY else 0,
                last_day_close=float(row["last_day_close"]),
            )
            for _, row in df.iterrows()
            if not pd.isna(row["Open"]) and not pd.isna(row["Close"])
        ]


class DatabaseHandler:
    """데이터베이스 처리 클래스"""

    def __init__(self, config: PriceServiceConfig, database_instance):
        self.config = config
        self.database = database_instance

    def get_table_name(self, ctry: Country, frequency: Frequency) -> str:
        """테이블 이름 생성"""
        return f"stock_{ctry.value}_{self.config.FREQUENCY_MAPPING[frequency]}"

    @lru_cache(maxsize=100)
    def get_columns_for_country(self, ctry: Country) -> List[str]:
        """국가별 컬럼 리스트 반환"""
        return self.config.BASE_COLUMNS + self.config.COUNTRY_SPECIFIC_COLUMNS.get(ctry, [])

    async def fetch_data(
        self, ctry: Country, ticker: str, date_range: Tuple[date, date], frequency: Frequency
    ) -> pd.DataFrame:
        """데이터 조회"""
        start_date, end_date = date_range
        try:
            table_name = self.get_table_name(ctry, frequency)
            columns = self.get_columns_for_country(ctry)
            conditions = {
                "Ticker": ticker,
                "Date__gte": datetime.combine(start_date, datetime.min.time()),
                "Date__lte": datetime.combine(end_date, datetime.max.time()),
            }

            result = await asyncio.to_thread(
                self.database._select, table=table_name, columns=columns, order="Date", ascending=True, **conditions
            )

            return pd.DataFrame(result, columns=columns) if result else pd.DataFrame(columns=columns)

        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            return pd.DataFrame(columns=self.get_columns_for_country(ctry))


class PriceService:
    """주가 데이터 서비스"""

    def __init__(self):
        self.config = PriceServiceConfig()
        self._cache = MemoryCache()
        self.db_handler = DatabaseHandler(self.config, database)
        self.data_processor = DataProcessor(self.config)

    def _get_date_range(
        self, start_date: Optional[date], end_date: Optional[date], frequency: Frequency
    ) -> Tuple[date, date]:
        """날짜 범위 계산 및 검증"""
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            days = 1 if frequency == Frequency.MINUTE else 30
            start_date = end_date - timedelta(days=days)
        if start_date > end_date:
            raise ValueError("start_date cannot be later than end_date")
        return start_date, end_date

    async def get_52week_data(self, ctry: Country, ticker: str, end_date: date) -> Tuple[float, float]:
        """52주 최고/최저가 조회"""
        cache_key = f"52week_{ticker}_{end_date.strftime('%Y%m%d')}"

        cached_data = self._cache.get(cache_key)
        if cached_data is not None:
            logger.info("Using cached 52-week data")
            return cached_data["highest"], cached_data["lowest"]

        logger.info("Calculating 52-week high/low...")
        start_date = end_date - timedelta(days=365)
        df = await self.db_handler.fetch_data(ctry, ticker, (start_date, end_date), Frequency.DAILY)

        if df.empty:
            return 0.0, 0.0

        highest = float(df["High"].max())
        lowest = float(df["Low"].min())

        # 딕셔너리 형태로 캐시 저장
        cache_data = {"highest": highest, "lowest": lowest}

        try:
            self._cache.set(cache_key, cache_data, self.config.CACHE_TTL["ONE_DAY"])
        except Exception as e:
            logger.error(f"Error caching 52-week data: {str(e)}")

        return highest, lowest

    async def read_price_data(
        self,
        ctry: Country,
        ticker: str,
        frequency: Frequency,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> BaseResponse[ResponsePriceDataItem]:
        """가격 데이터 조회"""
        try:
            query_start_date, query_end_date = self._get_date_range(start_date, end_date, frequency)
            cache_key = f"{ctry.value}_{frequency.value}_{ticker}"

            # await 추가
            df = await self._get_cached_or_fetch_data(
                cache_key, ctry, ticker, (query_start_date, query_end_date), frequency
            )

            if df is None or df.empty:
                return BaseResponse(status="error", message=f"No price data found for {ticker}", data=None)

            # 52주 데이터 조회
            week52_data = await self.get_52week_data(ctry, ticker, query_end_date)

            # 데이터 처리
            price_data = self.data_processor.process_price_data(df, ctry, frequency, week52_data, query_end_date)

            if not price_data:
                return BaseResponse(status="error", message="No valid data found after conversion", data=None)

            return BaseResponse(status="success", message="Data retrieved successfully", data=price_data)

        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            logger.exception("Full traceback:")
            return BaseResponse(status="error", message=f"Internal server error: {str(e)}", data=None)

    async def _get_cached_or_fetch_data(
        self, cache_key: str, ctry: Country, ticker: str, date_range: Tuple[date, date], frequency: Frequency
    ) -> Optional[pd.DataFrame]:
        """캐시된 데이터 확인 또는 새로운 데이터 조회"""
        start_date, end_date = date_range

        # 캐시 확인
        cached_df = self._cache.get(cache_key)
        if cached_df is not None:
            logger.info("Cache hit!")
            try:
                # DataFrame으로 변환
                if isinstance(cached_df, dict):
                    cached_df = pd.DataFrame(cached_df)

                mask = (cached_df["Date"] >= pd.Timestamp(start_date)) & (cached_df["Date"] <= pd.Timestamp(end_date))
                df = cached_df[mask].copy()

                if not (
                    df.empty or df["Date"].min() > pd.Timestamp(start_date) or df["Date"].max() < pd.Timestamp(end_date)
                ):
                    logger.info(f"Using cached data with {len(df)} records")
                    return df
            except Exception as e:
                logger.error(f"Error processing cached data: {str(e)}")

        # 새로운 데이터 조회
        logger.info("Fetching data from database...")
        df = await self.db_handler.fetch_data(ctry, ticker, date_range, frequency)
        if not df.empty:
            df = self.data_processor.preprocess_dataframe(df)

            try:
                # DataFrame을 딕셔너리로 변환하여 캐시 저장
                cache_data = df.to_dict("records")
                self._cache.set(cache_key, cache_data, self.config.CACHE_TTL["ONE_HOUR"])
                logger.info(f"Cached {len(df)} records")
            except Exception as e:
                logger.error(f"Error caching data: {str(e)}")

            logger.info(f"Fetched {len(df)} records from database")

        return df


def get_price_service() -> PriceService:
    """PriceService 인스턴스 생성"""
    return PriceService()
