from fastapi import HTTPException
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel, Field
import logging
from app.modules.common.enum import Country
from app.database.crud import database

logger = logging.getLogger(__name__)


class PriceDataResponse(BaseModel):
    data: List[Dict[str, Any]] = Field(
        ...,
        example=[
            {"date": "2023-01-01", "open": 100, "high": 105, "low": 98, "close": 102, "volume": 1000000},
            {"date": "2023-01-02", "open": 102, "high": 107, "low": 101, "close": 106, "volume": 1200000},
        ],
    )


class PriceService:
    def __init__(self):
        self.database = database
        self.db_columns = [
            "Date",  # datetime
            "Open",  # float
            "High",  # float
            "Low",  # float
            "Close",  # float
            "Volume",  # float
            "Ticker",  # varchar(7)
            "Name",  # varchar(100)
            "Isin",  # varchar(12)
            "Market",  # varchar(10)
            "Category",  # varchar(9)
        ]
        self.MAX_DAYS = 365

    def _convert_row_to_dict(self, row) -> Dict[str, Any]:
        """SQLAlchemy Row를 딕셔너리로 변환"""
        try:
            date_str = getattr(row, "Date").strftime("%Y-%m-%d") if getattr(row, "Date") else None
            return {
                "date": date_str,
                "open": float(getattr(row, "Open", 0) or 0),
                "high": float(getattr(row, "High", 0) or 0),
                "low": float(getattr(row, "Low", 0) or 0),
                "close": float(getattr(row, "Close", 0) or 0),
                "volume": float(getattr(row, "Volume", 0) or 0),
                "code": str(getattr(row, "Ticker", "") or ""),
                "name": str(getattr(row, "Name", "") or ""),
                "isin": str(getattr(row, "Isin", "") or ""),
                "market": str(getattr(row, "Market", "") or ""),
                "category": str(getattr(row, "Category", "") or ""),
            }
        except Exception as e:
            logger.error(f"Error converting row: {e}")
            logger.debug(f"Row data: {row}")
            raise

    async def read_price_data(
        self, ctry: Country, ticker: str, start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> PriceDataResponse:
        try:
            # 날짜 범위 설정
            if end_date is None:
                end_date = date.today()
            if start_date is None:
                start_date = end_date - timedelta(days=30)

            date_diff = (end_date - start_date).days
            if date_diff > self.MAX_DAYS:
                start_date = end_date - timedelta(days=self.MAX_DAYS)

            # 테이블 이름 및 조건 설정
            table_name = f"stock_{ctry.value}_1d"
            conditions = {
                "Ticker": ticker,
                "Date__gte": datetime.combine(start_date, datetime.min.time()),
                "Date__lte": datetime.combine(end_date, datetime.max.time()),
            }

            # 데이터베이스 쿼리 실행
            result = self.database._select(
                table=table_name, columns=self.db_columns, order="Date", ascending=True, **conditions
            )

            if not result:
                raise HTTPException(status_code=404, detail=f"No price data found for {ticker}")

            # 결과를 로깅하여 디버깅
            if result:
                logger.debug(f"First row raw data: {result[0]}")
                logger.debug(f"First row dir: {dir(result[0])}")

            # 결과 데이터 변환
            data = []
            for row in result:
                try:
                    row_dict = self._convert_row_to_dict(row)
                    data.append(row_dict)  # Dictionary를 직접 추가
                except Exception as e:
                    logger.warning(f"Failed to convert row: {str(e)}")
                    logger.debug(f"Problematic row: {row}")
                    continue

            if not data:
                raise HTTPException(status_code=404, detail="No valid data found after conversion")

            return PriceDataResponse(data=data)

        except HTTPException:
            raise
        except SQLAlchemyError as e:
            logger.error(f"Database error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


def get_price_service() -> PriceService:
    return PriceService()
