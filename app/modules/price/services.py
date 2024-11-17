from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple
from sqlalchemy.exc import SQLAlchemyError
import logging
from app.modules.common.enum import Country
from app.database.crud import database
from app.modules.common.schemas import BaseResponse
from app.modules.price.schemas import PriceDataItem, StockKrFactorItem, VolumeDataItem

logger = logging.getLogger(__name__)


class PriceService:
    def __init__(self):
        self.database = database
        self.price_columns = [
            "Date",  # datetime
            "Ticker",  # varchar(7)
            "Name",  # varchar(100)
            "Open",  # float
            "High",  # float
            "Low",  # float
            "Close",  # float
        ]
        self.volume_columns = [
            "Date",  # datetime
            "Ticker",  # varchar(7)
            "Volume",  # int
            "Name",  # varchar(100)
        ]
        self.MAX_DAYS = 365

    def _get_date_range(self, start_date: Optional[date], end_date: Optional[date]) -> Tuple[date, date]:
        """날짜 범위를 계산하고 검증"""
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=30)

        date_diff = (end_date - start_date).days
        if date_diff > self.MAX_DAYS:
            start_date = end_date - timedelta(days=self.MAX_DAYS)

        return start_date, end_date

    def _get_query_conditions(self, ticker: str, start_date: date = None, end_date: date = None) -> dict:
        """쿼리 조건 생성"""
        return {
            "Ticker": ticker,
            "Date__gte": datetime.combine(start_date, datetime.min.time()),
            "Date__lte": datetime.combine(end_date, datetime.max.time()),
        }

    def _convert_to_price_item(self, row) -> PriceDataItem:
        """SQLAlchemy Row를 PriceDataItem으로 변환"""
        try:
            date_str = getattr(row, "Date").strftime("%Y-%m-%d") if getattr(row, "Date") else None
            return PriceDataItem(
                date=date_str,
                ticker=str(getattr(row, "Ticker", "") or ""),
                name=str(getattr(row, "Name", "") or ""),
                open=float(getattr(row, "Open", 0) or 0),
                high=float(getattr(row, "High", 0) or 0),
                low=float(getattr(row, "Low", 0) or 0),
                close=float(getattr(row, "Close", 0) or 0),
            )
        except Exception as e:
            logger.error(f"Error converting row to PriceDataItem: {e}")
            logger.debug(f"Row data: {row}")
            raise

    def _convert_to_volume_item(self, row) -> VolumeDataItem:
        """SQLAlchemy Row를 VolumeDataItem으로 변환"""
        try:
            date_str = getattr(row, "Date").strftime("%Y-%m-%d") if getattr(row, "Date") else None
            return VolumeDataItem(
                date=date_str,
                ticker=str(getattr(row, "Ticker", "") or ""),
                name=str(getattr(row, "Name", "") or ""),
                volume=int(getattr(row, "Volume", 0) or 0),
            )
        except Exception as e:
            logger.error(f"Error converting row to VolumeDataItem: {e}")
            logger.debug(f"Row data: {row}")
            raise

    def _convert_to_stock_factor(self, row) -> StockKrFactorItem:
        """SQLAlchemy Row를 StockKrFactor으로 변환"""
        return StockKrFactorItem(
            ticker=str(getattr(row, "Ticker", "") or ""),
            name=str(getattr(row, "Name", "") or ""),
            prev_close=float(getattr(row, "Prev_Close", 0) or 0),
            week_52_high=float(getattr(row, "Week_52_High", 0) or 0),
            week_52_low=float(getattr(row, "Week_52_Low", 0) or 0),
            all_time_high=float(getattr(row, "All_Time_High", 0) or 0),
            all_time_low=float(getattr(row, "All_Time_Low", 0) or 0),
            momentum_1m=float(getattr(row, "Momentum_1m", 0) or 0),
            momentum_3m=float(getattr(row, "Momentum_3m", 0) or 0),
            momentum_6m=float(getattr(row, "Momentum_6m", 0) or 0),
            momentum_12m=float(getattr(row, "Momentum_12m", 0) or 0),
            rate_of_change_10d=float(getattr(row, "Rate_Of_Change_10d", 0) or 0),
            rate_of_change_30d=float(getattr(row, "Rate_Of_Change_30d", 0) or 0),
            rate_of_change_60d=float(getattr(row, "Rate_Of_Change_60d", 0) or 0),
        )

    async def _execute_query(self, ctry: Country, conditions: dict, columns: List[str]) -> BaseResponse:
        """데이터베이스 쿼리 실행"""
        try:
            table_name = f"stock_{ctry.value}_1d"
            result = self.database._select(table=table_name, columns=columns, order="Date", ascending=True, **conditions)
            return result
        except SQLAlchemyError as e:
            logger.error(f"Database error: {str(e)}")
            raise

    async def read_price_data(
        self, ctry: Country, ticker: str, start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> BaseResponse[List[PriceDataItem]]:
        try:
            start_date, end_date = self._get_date_range(start_date, end_date)
            conditions = self._get_query_conditions(ticker, start_date, end_date)
            result = await self._execute_query(ctry, conditions, self.price_columns)

            if not result:
                return BaseResponse(status="error", message=f"No price data found for {ticker}", data=None)

            price_data = []
            for row in result:
                try:
                    price_item = self._convert_to_price_item(row)
                    price_data.append(price_item)
                except Exception as e:
                    logger.warning(f"Failed to convert row: {str(e)}")
                    continue

            if not price_data:
                return BaseResponse(status="error", message="No valid data found after conversion", data=None)

            return BaseResponse(status="success", message="Data retrieved successfully", data=price_data)

        except Exception as e:
            logger.error(f"Unexpected error in read_price_data: {str(e)}")
            return BaseResponse(status="error", message=f"Internal server error: {str(e)}", data=None)

    async def read_volume_data(
        self, ctry: Country, ticker: str, start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> BaseResponse[List[VolumeDataItem]]:  # 제네릭 타입 명시
        try:
            start_date, end_date = self._get_date_range(start_date, end_date)
            conditions = self._get_query_conditions(ticker, start_date, end_date)
            result = await self._execute_query(ctry, conditions, self.volume_columns)

            if not result:
                return BaseResponse[List[VolumeDataItem]](
                    status="error", message=f"No volume data found for {ticker}", data=None
                )

            volume_data: List[VolumeDataItem] = []
            for row in result:
                try:
                    volume_item = self._convert_to_volume_item(row)
                    volume_data.append(volume_item)
                except Exception as e:
                    logger.warning(f"Failed to convert row: {str(e)}")
                    continue

            if not volume_data:
                return BaseResponse[List[VolumeDataItem]](
                    status="error", message="No valid data found after conversion", data=None
                )

            return BaseResponse[List[VolumeDataItem]](
                status="success", message="Data retrieved successfully", data=volume_data
            )

        except Exception as e:
            logger.error(f"Unexpected error in read_volume_data: {str(e)}")
            return BaseResponse[List[VolumeDataItem]](
                status="error", message=f"Internal server error: {str(e)}", data=None
            )

    async def read_stock_factors(self, ctry: Country, ticker: str) -> BaseResponse[List[StockKrFactorItem]]:
        conditions = self._get_query_conditions(ticker)
        result = await self._execute_query(ctry, conditions, self.stock_factors_columns)

        if not result:
            return BaseResponse(status="error", message=f"No stock factors found for {ticker}", data=None)

        stock_factors = []
        for row in result:
            stock_factor = self._convert_to_stock_factor(row)
            stock_factors.append(stock_factor)

        return BaseResponse(status="success", message="Data retrieved successfully", data=stock_factors)


def get_price_service() -> PriceService:
    return PriceService()
