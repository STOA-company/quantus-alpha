import asyncio
from datetime import date, datetime, timedelta
from functools import lru_cache
import json
from typing import List, Optional, Tuple, Dict
from fastapi import Request
import numpy as np
import pandas as pd
from dataclasses import dataclass, field

from sse_starlette import EventSourceResponse
from app.modules.common.cache import MemoryCache
from app.modules.common.enum import Country, Frequency
from app.modules.common.schemas import BaseResponse
from app.modules.common.utils import check_ticker_country_len_2
from app.modules.price.schemas import PriceDataItem, RealTimePriceDataItem, ResponsePriceDataItem
from app.database.crud import database
from app.core.logging.config import get_logger
from app.core.exception.custom import DataNotFoundException


logger = get_logger(__name__)


@dataclass
class ChunkResult:
    """청크 결과를 담는 클래스"""

    df: pd.DataFrame
    start_date: date
    end_date: date
    success: bool
    error: Optional[str] = None


@dataclass
class PriceServiceConfig:
    """주가 데이터 서비스 설정"""

    FREQUENCY_MAPPING: Dict[Frequency, str] = field(
        default_factory=lambda: {
            Frequency.DAILY: "1d",
            Frequency.MINUTE: "1m",
        }
    )

    MINUTE_CHUNK_SIZE_DAYS: int = 1
    DAILY_CHUNK_SIZE_DAYS: int = 30
    MAX_CONCURRENT_REQUESTS: int = 10
    MAX_MINUTE_DAYS: int = 14
    # 캐시 TTL 설정
    CACHE_TTL: Dict[str, int] = field(
        default_factory=lambda: {
            "ONE_MONTH": 60 * 60 * 24 * 30,
            "ONE_WEEK": 60 * 60 * 24 * 7,
            "ONE_DAY": 60 * 60 * 24,
            "ONE_HOUR": 60 * 60,
        }
    )

    # 컬럼 설정
    BASE_COLUMNS: List[str] = field(
        default_factory=lambda: ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume", "Market"]
    )
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

    def get_last_day_close(self, df: pd.DataFrame, frequency: Frequency) -> float:
        """전일 종가 계산"""
        try:
            if df.empty:
                return 0.0

            # 실제 데이터의 마지막 날짜 확인
            last_available_date = df["Date"].max().date()

            if frequency == Frequency.DAILY:
                # 실제 데이터의 마지막 날짜 기준으로 전일 데이터 찾기
                prev_day_data = df[df["Date"].dt.date == (last_available_date - timedelta(days=1))]
            else:
                prev_day_data = df[df["Date"].dt.date == (last_available_date - timedelta(days=1))]

            return float(prev_day_data["Close"].iloc[-1]) if not prev_day_data.empty else 0.0

        except Exception as e:
            logger.error(f"Error getting last day close: {str(e)}")
            return 0.0

    def process_price_data(
        self,
        df: pd.DataFrame,
        ctry: Country,
        frequency: Frequency,
        week52_data: Tuple[float, float],
        end_date: date,
        usa_name: Optional[str] = None,
    ) -> ResponsePriceDataItem:
        """DataFrame을 PriceDataItem으로 변환"""
        if df.empty:
            return []

        try:
            week52_highest, week52_lowest = week52_data

            # 가격 변동률 계산
            df["daily_price_change_rate"] = np.round((df["Close"] - df["Open"]) / df["Open"] * 100, decimals=2).fillna(0)

            df["name"] = usa_name if ctry == Country.US else df.get("Name", "").fillna("")

            # 전일 종가 계산
            df["last_day_close"] = self.get_last_day_close(df, frequency)

            # TODO: 시가총액 Mock 데이터
            return ResponsePriceDataItem(
                ticker=str(df["Ticker"].iloc[0]),
                name=str(df["name"].iloc[0]),
                market=str(df["Market"].iloc[0]),
                market_cap=569.87,
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

    @lru_cache(maxsize=1000)
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

    async def fetch_data_in_chunks(
        self,
        ctry: Country,
        ticker: str,
        date_range: Tuple[date, date],
        frequency: Frequency,
        chunk_size_days: int,
        max_retries: int = 3,
    ) -> List[ChunkResult]:
        """청크 단위로 데이터 조회"""
        start_date, end_date = date_range
        chunks = self._create_date_chunks(start_date, end_date, chunk_size_days)

        # 세마포어를 사용하여 동시 요청 수 제한
        semaphore = asyncio.Semaphore(self.config.MAX_CONCURRENT_REQUESTS)

        async def fetch_chunk(chunk_start: date, chunk_end: date) -> ChunkResult:
            async with semaphore:
                for attempt in range(max_retries):
                    try:
                        df = await self.fetch_data(ctry, ticker, (chunk_start, chunk_end), frequency)
                        return ChunkResult(df, chunk_start, chunk_end, True)
                    except Exception as e:
                        if attempt == max_retries - 1:  # 마지막 시도였다면
                            logger.error(
                                f"Failed to fetch chunk {chunk_start}-{chunk_end} after {max_retries} attempts: {str(e)}"
                            )
                            return ChunkResult(pd.DataFrame(), chunk_start, chunk_end, False, str(e))
                        await asyncio.sleep(1 * (attempt + 1))  # 지수 백오프

        # 모든 청크에 대해 비동기로 데이터 조회
        tasks = [fetch_chunk(chunk_start, chunk_end) for chunk_start, chunk_end in chunks]
        return await asyncio.gather(*tasks)

    def _create_date_chunks(self, start_date: date, end_date: date, chunk_size_days: int) -> List[Tuple[date, date]]:
        """날짜 범위를 청크로 분할"""
        chunks = []
        current_start = start_date

        while current_start < end_date:
            chunk_end = min(current_start + timedelta(days=chunk_size_days - 1), end_date)
            chunks.append((current_start, chunk_end))
            current_start = chunk_end + timedelta(days=1)

        return chunks

    async def get_us_ticker_name(self, ticker: str) -> Optional[str]:
        """US 티커의 종목명 조회"""
        try:
            conditions = {"ticker": ticker}
            result = await asyncio.to_thread(
                self.database._select, table="stock_us_tickers", columns=["english_name"], **conditions
            )
            return result[0].english_name if result else None
        except Exception as e:
            logger.error(f"Error fetching US ticker name: {str(e)}")
            return None


class PriceService:
    """주가 데이터 서비스"""

    def __init__(self):
        self.config = PriceServiceConfig()
        self._cache = MemoryCache()
        self.db_handler = DatabaseHandler(self.config, database)
        self.data_processor = DataProcessor(self.config)
        self.database = database

    def _get_chunk_size(self, frequency: Frequency) -> int:
        """주기에 따른 청크 크기 반환"""
        return self.config.MINUTE_CHUNK_SIZE_DAYS if frequency == Frequency.MINUTE else self.config.DAILY_CHUNK_SIZE_DAYS

    def _get_date_range(
        self, start_date: Optional[date], end_date: Optional[date], frequency: Frequency
    ) -> Tuple[date, date]:
        """날짜 범위 계산 및 검증

        Args:
            start_date: 시작일자
            end_date: 종료일자
            frequency: 데이터 주기(분/일)

        Returns:
            Tuple[date, date]: (시작일자, 종료일자)
        """
        # 기본 날짜 범위 설정
        DEFAULT_DAYS = 1 if frequency == Frequency.MINUTE else 30

        # end_date 기본값 설정
        if end_date is None:
            end_date = date.today()

        # start_date 기본값 설정
        if start_date is None:
            start_date = end_date - timedelta(days=DEFAULT_DAYS)

        # 시작일이 종료일보다 늦으면 에러
        if start_date > end_date:
            raise ValueError("start_date cannot be later than end_date")

        # 시작일과 종료일이 같으면 종료일 +1
        if start_date == end_date:
            end_date = start_date + timedelta(days=1)

        # 분 단위 데이터는 최대 10일로 제한
        if frequency == Frequency.MINUTE:
            date_diff = (end_date - start_date).days
            if date_diff > self.config.MAX_MINUTE_DAYS:
                end_date = start_date + timedelta(days=self.config.MAX_MINUTE_DAYS)

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

        query_start_date, query_end_date = self._get_date_range(start_date, end_date, frequency)

        cache_key = f"{ctry.value}_{frequency.value}_{ticker}"
        df = await self._get_cached_or_fetch_data(cache_key, ctry, ticker, (query_start_date, query_end_date), frequency)

        if df is None or df.empty:
            raise DataNotFoundException(ticker, "price")

        # 52주 데이터 조회
        week52_data = await self.get_52week_data(ctry, ticker, query_end_date)

        # US 티커의 경우 이름 조회
        usa_name = None
        if ctry == Country.US:
            usa_name = await self.db_handler.get_us_ticker_name(ticker)

        # 데이터 처리
        price_data = self.data_processor.process_price_data(df, ctry, frequency, week52_data, query_end_date, usa_name)

        if not price_data:
            return BaseResponse(status_code=404, message="No valid data found after conversion", data=None)

        return BaseResponse(status_code=200, message="Data retrieved successfully", data=price_data)

    async def _get_cached_or_fetch_data(
        self, cache_key: str, ctry: Country, ticker: str, date_range: Tuple[date, date], frequency: Frequency
    ) -> Optional[pd.DataFrame]:
        """캐시된 데이터 확인 또는 청크 단위로 새로운 데이터 조회"""
        start_date, end_date = date_range

        # 캐시 확인 (기존 코드와 동일)

        cached_df = self._cache.get(cache_key)
        if cached_df is not None:
            logger.info("Cache hit!")
            try:
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

        # 청크 단위로 새로운 데이터 조회
        logger.info("Fetching data from database in chunks...")
        chunk_size = self._get_chunk_size(frequency)
        chunk_results = await self.db_handler.fetch_data_in_chunks(ctry, ticker, date_range, frequency, chunk_size)

        # 실패한 청크가 있는지 확인
        failed_chunks = [result for result in chunk_results if not result.success]
        if failed_chunks:
            chunk_errors = [f"{result.start_date}-{result.end_date}: {result.error}" for result in failed_chunks]
            logger.error(f"Failed to fetch some chunks: {chunk_errors}")
            return None

        # 모든 청크 데이터 합치기
        dfs = [result.df for result in chunk_results if not result.df.empty]
        if not dfs:
            return pd.DataFrame()

        df = pd.concat(dfs, ignore_index=True)
        df = df.sort_values("Date").reset_index(drop=True)
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

    async def get_real_time_price_data(self, ticker: str, request: Request) -> BaseResponse[RealTimePriceDataItem]:
        """일회성 실시간 가격 데이터 조회"""
        try:
            ctry = check_ticker_country_len_2(ticker)
            table_name = f"stock_{ctry}_1d"
            columns = ["Date", "Open", "Close"]
            conditions = {
                "Ticker": ticker,
                "Date": (date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
            }

            data = pd.DataFrame(self.database._select(table=table_name, columns=columns, **conditions))

            if data.empty:
                return BaseResponse(status_code=404, message="No data found", data=None)

            price_change = round(data["Close"] - data["Open"], 2)
            price_change_rate = round(price_change / data["Open"], 2)

            result = RealTimePriceDataItem(
                ctry=ctry,
                price=float(data["Close"]),
                price_change=float(price_change),
                price_change_rate=float(price_change_rate),
            )

            return BaseResponse(status_code=200, message="Data retrieved successfully", data=result)

        except Exception as e:
            logger.error(f"Error in get_real_time_price_data: {str(e)}")
            return BaseResponse(status_code=500, message=f"Error: {str(e)}", data=None)

    async def stream_real_time_price_data(self, ticker: str, request: Request) -> EventSourceResponse:
        """실시간 가격 데이터 스트림"""

        async def event_generator():
            while True:
                if await request.is_disconnected():
                    break

                try:
                    ctry = check_ticker_country_len_2(ticker)
                    table_name = f"stock_{ctry}_1d"
                    columns = ["Date", "Open", "Close"]
                    conditions = {
                        "Ticker": ticker,
                        "Date": (date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
                    }

                    data = pd.DataFrame(self.database._select(table=table_name, columns=columns, **conditions))

                    if data.empty:
                        yield {
                            "event": "error",
                            "data": json.dumps({"status_code": 404, "message": "No data found", "data": None}),
                        }
                        break

                    price_change = round(data["Close"] - data["Open"], 2)
                    price_change_rate = round(price_change / data["Open"], 2)

                    result = RealTimePriceDataItem(
                        ctry=ctry,
                        price=float(data["Close"]),
                        price_change=float(price_change),
                        price_change_rate=float(price_change_rate),
                    )

                    response_data = BaseResponse(status_code=200, message="Data retrieved successfully", data=result)

                    yield {"event": "update", "data": response_data.model_dump_json()}

                except Exception as e:
                    logger.error(f"Error in stream_real_time_price_data: {str(e)}")
                    yield {
                        "event": "error",
                        "data": json.dumps({"status_code": 500, "message": f"Error: {str(e)}", "data": None}),
                    }
                    break

                await asyncio.sleep(5)

        return EventSourceResponse(
            event_generator(),
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )


def get_price_service() -> PriceService:
    """PriceService 인스턴스 생성"""
    return PriceService()
