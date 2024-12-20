import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, text
from app.core.exception.custom import DataNotFoundException
from app.core.logging.config import get_logger
from app.models.models_stock import StockInformation
from app.modules.common.enum import Country
from app.modules.common.cache import MemoryCache
from app.modules.common.utils import check_ticker_country_len_2
from app.modules.price.schemas import PriceDailyItem, PriceSummaryItem
from app.database.conn import db
from app.database.crud import database
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)


@dataclass
class ChunkResult:
    """청크 결과를 담는 클래스"""

    df: pd.DataFrame
    start_date: date
    end_date: date
    success: bool
    error: Optional[str] = None


class PriceService:
    def __init__(self):
        self._cache = MemoryCache()
        self._db = database
        self._async_db = db
        self.cache_ttl_day = 60 * 60 * 24
        self.cache_ttl_week = 60 * 60 * 24 * 7
        self.cache_ttl_month = 60 * 60 * 24 * 30
        self.max_concurrent_requests = 10
        self.base_columns = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume", "Market"]
        self.country_specific_columns = {Country.KR: self.base_columns + ["Name"], Country.US: self.base_columns}
        self.price_columns = ["Date", "Open", "High", "Low", "Close", "Volume"]

    def _fetch_52week_data(self, ctry: str, ticker: str) -> pd.DataFrame:
        """
        52주 데이터 조회
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=365)

        table_name = f"stock_{ctry}_1d"
        columns = self.country_specific_columns.get(ctry, self.base_columns)

        result = self._db._select(
            table=table_name,
            columns=columns,
            Ticker=ticker,
            Date__gte=datetime.combine(start_date, datetime.min.time()),
            Date__lte=datetime.combine(end_date, datetime.max.time()),
            order="Date",
            ascending=True,
        )

        return pd.DataFrame(result, columns=columns) if result else pd.DataFrame(columns=columns)

    def _get_last_day_close(self, df: pd.DataFrame) -> float:
        """직전 거래일의 종가 반환"""
        if df.empty or len(df) < 2:  # 데이터가 없거나 하나밖에 없으면 0 반환
            return 0.0

        # Date로 정렬
        sorted_df = df.sort_values("Date", ascending=False)

        # 가장 최근 날짜를 제외한 첫 번째 데이터의 종가를 반환
        return float(sorted_df.iloc[1]["Close"])

    def _process_price_data(self, df: pd.DataFrame) -> Tuple[float, float, float]:
        """
        52주 최고가, 52주 최저가, 최근 종가 반환
        """
        week_52_high = df["High"].max()
        week_52_low = df["Low"].min()
        last_day_close = self._get_last_day_close(df)

        return week_52_high, week_52_low, last_day_close

    def _get_us_ticker_name(self, ticker: str) -> str:  # TODO: RDS DB에 추가하여 조회 로직 없애기
        """
        US 종목 이름 조회
        """
        result = self._db._select(table="stock_us_tickers", columns=["english_name"], ticker=ticker)
        return result[0].english_name if result else None

    def _validate_date_range(self, start_date: Optional[date], end_date: Optional[date]) -> Tuple[date, date]:
        """
        날짜 범위 계산 및 검증
        """
        # end_date 기본값 설정
        if end_date is None:
            end_date = date.today()

        # start_date 기본값 설정 (기본 30일)
        if start_date is None:
            start_date = end_date - timedelta(days=30)

        # 시작일이 종료일보다 늦으면 에러
        if start_date > end_date:
            raise ValueError("시작일이 종료일보다 늦을 수 없습니다")

        # 시작일과 종료일이 같으면 종료일 +1
        if start_date == end_date:
            end_date = start_date + timedelta(days=1)

        return start_date, end_date

    def _get_monthly_periods(self, start_date: date, end_date: date) -> List[tuple[date, date]]:
        """월별 기간 분할"""
        periods = []
        current_date = start_date.replace(day=1)

        while current_date <= end_date:
            # 다음 달 1일 계산
            if current_date.month == 12:
                next_month = current_date.replace(year=current_date.year + 1, month=1)
            else:
                next_month = current_date.replace(month=current_date.month + 1)

            # 월말 계산 (다음 달 1일 - 1일)
            month_end = min((next_month - timedelta(days=1)), end_date)

            # 시작일이 월말보다 작은 경우만 포함
            if month_end >= start_date:
                period_start = max(current_date, start_date)
                periods.append((period_start, month_end))

            current_date = next_month

        return periods

    async def _fetch_daily_data(self, ctry: Country, ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
        """일별 데이터 조회"""
        table_name = f"stock_{ctry.value.lower()}_1d"
        columns = self.country_specific_columns[ctry]

        query = text(f"""
            SELECT {', '.join(columns)}
            FROM {table_name}
            WHERE Ticker = :ticker
              AND Date >= :start_date
              AND Date <= :end_date
            ORDER BY Date ASC
        """)

        result = await self._async_db.execute_async_query(
            query,
            {
                "ticker": ticker,
                "start_date": datetime.combine(start_date, datetime.min.time()),
                "end_date": datetime.combine(end_date, datetime.max.time()),
            },
        )

        return pd.DataFrame(result.fetchall(), columns=columns) if result else pd.DataFrame(columns=columns)

    async def _fetch_chunk_with_retry(
        self, ctry: Country, ticker: str, chunk_dates: Tuple[date, date], semaphore: asyncio.Semaphore
    ) -> ChunkResult:
        """청크 데이터 조회 (재시도 로직 포함)"""
        chunk_start, chunk_end = chunk_dates

        async with semaphore:
            for attempt in range(self.max_concurrent_requests):
                try:
                    df = await self._fetch_daily_data(ctry, ticker, chunk_start, chunk_end)
                    return ChunkResult(df, chunk_start, chunk_end, True)
                except Exception as e:
                    if attempt == self.max_concurrent_requests - 1:
                        logger.error(
                            f"Failed to fetch chunk {chunk_start}-{chunk_end} after {self.max_concurrent_requests} attempts: {str(e)}"
                        )
                        return ChunkResult(pd.DataFrame(), chunk_start, chunk_end, False, str(e))
                    await asyncio.sleep(1 * (attempt + 1))  # 지수 백오프

    def _price_change_rate_data(self, df: pd.DataFrame) -> List[PriceDailyItem]:
        """
        일봉 데이터 조회
        """
        df = df.dropna(subset=["Open", "Close"])

        df["price_change_rate"] = ((df["Close"] - df["Open"]) / df["Open"] * 100).round(2)

        result_df = df.assign(
            date=df["Date"].dt.date,
            open=df["Open"].astype(float),
            high=df["High"].astype(float),
            low=df["Low"].astype(float),
            close=df["Close"].astype(float),
            volume=df["Volume"].astype(int),
            price_change_rate=df["price_change_rate"].astype(float),
        )[["date", "open", "high", "low", "close", "volume", "price_change_rate"]]

        return result_df.to_dict("records")

    async def _fetch_parallel_data(
        self, ctry: Country, ticker: str, start_date: date, end_date: date
    ) -> List[PriceDailyItem]:
        """장기 데이터 병렬 처리"""
        periods = self._get_monthly_periods(start_date, end_date)
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        chunk_results = await asyncio.gather(
            *[self._fetch_chunk_with_retry(ctry, ticker, period, semaphore) for period in periods]
        )

        # 실패한 청크 확인
        failed_chunks = [result for result in chunk_results if not result.success]
        if failed_chunks:
            chunk_errors = [f"{result.start_date}-{result.end_date}: {result.error}" for result in failed_chunks]
            logger.error(f"Failed chunks: {chunk_errors}")

        # 성공한 청크 데이터 병합
        all_data = []
        for result in chunk_results:
            if result.success and not result.df.empty:
                processed_data = self._price_change_rate_data(result.df)
                all_data.extend(processed_data)

        if not all_data:
            raise DataNotFoundException(ticker, "daily")

        return [PriceDailyItem(**item) for item in all_data]

    async def _fetch_short_term_data(
        self, ctry: Country, ticker: str, start_date: date, end_date: date
    ) -> List[PriceDailyItem]:
        """단기 데이터 조회"""
        df = await self._fetch_daily_data(ctry, ticker, start_date, end_date)
        if df.empty:
            raise DataNotFoundException(ticker, "daily")

        processed_data = self._price_change_rate_data(df)
        if not processed_data:
            raise DataNotFoundException(ticker, "daily")

        return [PriceDailyItem(**item) for item in processed_data]

    async def _fetch_monthly_data(
        self, ctry: Country, ticker: str, period: tuple[date, date], semaphore: asyncio.Semaphore
    ) -> List[Dict[str, Any]]:
        """월별 데이터 조회 (세마포어 적용)"""
        start_date, end_date = period
        cache_key = f"daily_{ctry.value}_{ticker}_{start_date}_{end_date}"

        # 캐시 확인
        cached_data = self._cache.get(cache_key)
        if cached_data:
            logger.info(f"Cache hit for {cache_key}")
            return cached_data

        # 세마포어로 동시 요청 제한
        async with semaphore:
            df = await self._fetch_daily_data(ctry, ticker, start_date, end_date)
            if df.empty:
                return []

            response_data = self._process_price_data(df)
            if response_data:
                self._cache.set(cache_key, response_data, self.cache_ttl_month)
            return response_data

    async def get_price_data_daily(
        self, ctry: Country, ticker: str, start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> List[PriceDailyItem]:
        """일봉 데이터 조회"""
        start_date, end_date = self._validate_date_range(start_date, end_date)
        ctry = check_ticker_country_len_2(ticker)
        cache_key = f"daily_{ctry.value}_{ticker}_{start_date}_{end_date}"
        cached_data = self._cache.get(cache_key)
        if cached_data:
            logger.info(f"Cache hit for {cache_key}")
            return [PriceDailyItem(**item) for item in cached_data]

        # 60일 이상 데이터는 병렬 처리, 그 외는 단일 요청
        data_diff = (end_date - start_date).days
        data = (
            await self._fetch_parallel_data(ctry, ticker, start_date, end_date)
            if data_diff > 60
            else await self._fetch_short_term_data(ctry, ticker, start_date, end_date)
        )

        # 캐시 저장
        try:
            cache_data = [item.dict() for item in data]
            self._cache.set(cache_key, cache_data, self.cache_ttl_day)
        except Exception as e:
            logger.error(f"Failed to set cache for {cache_key}: {e}")

        return data

    async def get_price_data_summary(self, ctry: str, ticker: str, db: AsyncSession) -> PriceSummaryItem:
        """
        종목 요약 데이터 조회
        """
        cache_key = f"summary_{ctry}_{ticker}"

        cached_data = self._cache.get(cache_key)
        if cached_data:
            logger.info(f"Cache hit for {cache_key}")
            return PriceSummaryItem(**cached_data)

        df = self._fetch_52week_data(ctry, ticker)
        if df.empty:
            raise DataNotFoundException(ticker, "52week")

        week_52_high, week_52_low, last_day_close = self._process_price_data(df)
        sector = await self._get_sector_by_ticker(ticker)

        name = self._get_us_ticker_name(ticker) if ctry == "us" else df["Name"].iloc[0]

        response_data = {
            "name": name,
            "ticker": ticker,
            "ctry": ctry,
            "logo_url": "https://kr.pinterest.com/eunju011014/%EA%B7%80%EC%97%AC%EC%9A%B4-%EC%A7%A4/",
            "market": df["Market"].iloc[0],
            "sector": sector,
            "market_cap": 123.45,
            "last_day_close": last_day_close,
            "week_52_low": week_52_low,
            "week_52_high": week_52_high,
            "is_market_close": True,
        }

        try:
            self._cache.set(cache_key, response_data, self.cache_ttl_day)
        except Exception as e:
            logger.error(f"Failed to set cache for {cache_key}: {e}")

        return PriceSummaryItem(**response_data)

    async def _get_sector_by_ticker(self, ticker: str) -> str:
        """
        종목 섹터 조회
        """
        db = self._async_db
        query = select(StockInformation.sector_3).where(StockInformation.ticker == ticker)
        result = await db.execute_async_query(query)
        return result.scalar() or None


def get_price_service() -> PriceService:
    return PriceService()
