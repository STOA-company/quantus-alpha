import asyncio
from datetime import date, datetime, timedelta
from functools import lru_cache
import logging
from typing import List, Optional, Tuple
import numpy as np
import pandas as pd
from app.modules.common.cache import CacheStrategy, MemoryCache
from app.modules.common.enum import Country, Frequency
from app.modules.common.schemas import BaseResponse
from app.modules.price.schemas import PriceDataItem
from app.database.crud import database

logger = logging.getLogger(__name__)


class PriceService:
    """주가 데이터 서비스"""

    FREQUENCY_MAPPING = {
        Frequency.DAILY: "1d",
        Frequency.MINUTE: "1m",
    }

    # 청크 설정
    CHUNK_SIZE_DAYS = 1
    MAX_CONCURRENT_REQUESTS = 20

    # 캐시 설정
    PERMANENT_TTL = 60 * 60 * 24 * 30  # 30일
    TEMPORARY_TTL = 60 * 60  # 1시간
    RECENT_DATA_DAYS = 7  # 최근 7일은 temporary cache

    # 컬럼 정의
    BASE_COLUMNS = [
        "Date",
        "Ticker",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
    ]
    NUMERIC_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]
    COUNTRY_SPECIFIC_COLUMNS = {
        Country.KR: ["Name"],
        Country.US: [],
    }

    def __init__(self):
        self.database = database
        self._cache = MemoryCache()

    def _get_cache_strategy(self, start_date: date, end_date: date, frequency: Frequency) -> CacheStrategy:
        """데이터 특성에 따른 캐시 전략 결정"""
        today = date.today()

        # 분봉 데이터는 최근 데이터만 캐시
        if frequency == Frequency.MINUTE:
            if (today - end_date).days <= self.RECENT_DATA_DAYS:
                return CacheStrategy.TEMPORARY
            return CacheStrategy.NO_CACHE

        # 실시간 데이터는 캐시하지 않음
        if end_date >= today:
            return CacheStrategy.NO_CACHE

        # 최근 N일 데이터는 짧은 TTL
        if (today - end_date).days <= self.RECENT_DATA_DAYS:
            return CacheStrategy.TEMPORARY

        # 과거 데이터는 긴 TTL
        return CacheStrategy.PERMANENT

    def _get_cache_ttl(self, strategy: CacheStrategy) -> Optional[int]:
        """캐시 전략에 따른 TTL 반환"""
        if strategy == CacheStrategy.PERMANENT:
            return self.PERMANENT_TTL
        elif strategy == CacheStrategy.TEMPORARY:
            return self.TEMPORARY_TTL
        return None

    def _get_cache_key(self, ctry: Country, frequency: Frequency, ticker: str) -> str:
        """캐시 키 생성"""
        return f"{ctry.value}_{frequency.value}_{ticker}"

    @lru_cache(maxsize=100)
    def _get_columns_for_country(self, ctry: Country) -> List[str]:
        """국가별 컬럼 리스트 반환"""
        return self.BASE_COLUMNS + self.COUNTRY_SPECIFIC_COLUMNS.get(ctry, [])

    def _get_date_range(
        self, start_date: Optional[date], end_date: Optional[date], frequency: Frequency
    ) -> Tuple[date, date]:
        """날짜 범위 계산 및 검증"""
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            if frequency == Frequency.DAILY:
                start_date = end_date - timedelta(days=30)
            else:
                start_date = end_date - timedelta(days=1)
        if start_date > end_date:
            raise ValueError("start_date cannot be later than end_date")
        return start_date, end_date

    def _get_table_name(self, ctry: Country, frequency: Frequency) -> str:
        """테이블 이름 생성"""
        return f"stock_{ctry.value}_{self.FREQUENCY_MAPPING[frequency]}"

    def _split_date_ranges(self, start_date: date, end_date: date) -> List[Tuple[date, date]]:
        """날짜 범위를 청크로 분할"""
        date_ranges = []
        current_date = start_date

        while current_date <= end_date:
            chunk_end = min(current_date + timedelta(days=self.CHUNK_SIZE_DAYS - 1), end_date)
            date_ranges.append((current_date, chunk_end))
            current_date = chunk_end + timedelta(days=1)

        return date_ranges

    async def _fetch_chunk(
        self, ctry: Country, ticker: str, date_range: Tuple[date, date], frequency: Frequency
    ) -> pd.DataFrame:
        """단일 청크 데이터 조회"""
        start_date, end_date = date_range
        try:
            table_name = self._get_table_name(ctry, frequency)
            columns = self._get_columns_for_country(ctry)
            conditions = {
                "Ticker": ticker,
                "Date__gte": datetime.combine(start_date, datetime.min.time()),
                "Date__lte": datetime.combine(end_date, datetime.max.time()),
            }

            # 데이터베이스 조회를 비동기 세션으로 래핑
            result = await asyncio.to_thread(
                self.database._select, table=table_name, columns=columns, order="Date", ascending=True, **conditions
            )

            if not result:
                return pd.DataFrame(columns=columns)

            df = pd.DataFrame(result, columns=columns)
            return self._preprocess_dataframe(df)

        except Exception as e:
            logger.error(f"Error fetching chunk {date_range}: {str(e)}")
            return pd.DataFrame(columns=self._get_columns_for_country(ctry))

    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 전처리"""
        if df.empty:
            return df
        df["Date"] = pd.to_datetime(df["Date"])
        df[self.NUMERIC_COLUMNS] = df[self.NUMERIC_COLUMNS].apply(pd.to_numeric, errors="coerce")
        return df

    def _process_price_data(self, df: pd.DataFrame, ctry: Country) -> List[PriceDataItem]:
        """DataFrame을 PriceDataItem으로 변환"""
        if df.empty:
            return []

        try:
            # 가격 변동률 계산
            df["daily_price_change_rate"] = np.round((df["Close"] - df["Open"]) / df["Open"] * 100, decimals=2).fillna(0)

            # 종목명 처리
            df["name"] = "" if ctry != Country.KR else df.get("Name", "").fillna("")

            # 유효한 데이터만 변환
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
                    daily_price_change_rate=float(row["daily_price_change_rate"]),
                )
                for _, row in df.iterrows()
                if not pd.isna(row["Open"]) and not pd.isna(row["Close"])
            ]

        except Exception as e:
            logger.error(f"Error processing price data: {str(e)}")
            raise

    async def _fetch_data_parallel(
        self, ctry: Country, ticker: str, start_date: date, end_date: date, frequency: Frequency
    ) -> pd.DataFrame:
        """청크 단위 병렬 데이터 조회"""
        date_ranges = self._split_date_ranges(start_date, end_date)
        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)

        async def fetch_with_semaphore(date_range):
            async with semaphore:
                return await self._fetch_chunk(ctry, ticker, date_range, frequency)

        chunks = await asyncio.gather(*(fetch_with_semaphore(date_range) for date_range in date_ranges))

        if not chunks:
            return pd.DataFrame(columns=self._get_columns_for_country(ctry))

        combined_df = pd.concat(chunks, ignore_index=True)

        if not combined_df.empty:
            combined_df = combined_df.drop_duplicates(subset=["Date", "Ticker"]).sort_values("Date")

        return combined_df

    async def read_price_data(
        self,
        ctry: Country,
        ticker: str,
        frequency: Frequency,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> BaseResponse[List[PriceDataItem]]:
        """가격 데이터 조회"""
        try:
            # 1. 날짜 범위 설정
            start_date, end_date = self._get_date_range(start_date, end_date, frequency)
            logger.info(f"Date range: {start_date} to {end_date}")

            # 2. 캐시 전략 결정
            strategy = self._get_cache_strategy(start_date, end_date, frequency)
            logger.info(f"Cache strategy: {strategy}")

            # 3. 캐시 키 생성
            cache_key = self._get_cache_key(ctry, frequency, ticker)
            logger.info(f"Cache key: {cache_key}")

            # 4. 캐시 확인
            df = None
            if strategy != CacheStrategy.NO_CACHE:
                logger.info("Checking cache...")
                cached_df = self._cache.get(cache_key)
                if cached_df is not None:
                    logger.info("Cache hit!")
                    mask = (cached_df["Date"] >= pd.Timestamp(start_date)) & (cached_df["Date"] <= pd.Timestamp(end_date))
                    df = cached_df[mask].copy()

                    if (
                        df.empty
                        or df["Date"].min() > pd.Timestamp(start_date)
                        or df["Date"].max() < pd.Timestamp(end_date)
                    ):
                        logger.info("Cache data doesn't cover the full range, fetching from DB")
                        df = None
                    else:
                        logger.info(f"Using cached data with {len(df)} records")

            # 5. DB에서 데이터 조회
            if df is None:
                logger.info("Fetching data from database in parallel chunks...")
                df = await self._fetch_data_parallel(ctry, ticker, start_date, end_date, frequency)
                logger.info(f"Fetched {len(df) if not df.empty else 0} records from DB")

                # 6. 새로운 데이터 캐싱
                if not df.empty and strategy != CacheStrategy.NO_CACHE:
                    ttl = self._get_cache_ttl(strategy)
                    logger.info(f"Caching new data with TTL: {ttl}")
                    self._cache.set(cache_key, df, ttl)

            # 7. 결과 확인
            if df.empty:
                logger.warning(f"No data found for {ticker}")
                return BaseResponse(status="error", message=f"No price data found for {ticker}", data=None)

            # 8. 데이터 처리
            logger.info("Processing price data...")
            price_data = self._process_price_data(df, ctry)
            if not price_data:
                logger.warning("No valid data after processing")
                return BaseResponse(status="error", message="No valid data found after conversion", data=None)

            logger.info(f"Successfully processed {len(price_data)} records")
            return BaseResponse(status="success", message="Data retrieved successfully", data=price_data)

        except ValueError as e:
            logger.error(f"ValueError: {str(e)}")
            return BaseResponse(status="error", message=str(e), data=None)
        except Exception as e:
            logger.error(f"Unexpected error in read_price_data: {str(e)}")
            logger.exception("Full traceback:")  # 상세 에러 로그
            return BaseResponse(status="error", message=f"Internal server error: {str(e)}", data=None)


def get_price_service() -> PriceService:
    """PriceService 인스턴스 생성"""
    return PriceService()
