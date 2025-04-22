from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.models_stock import StockInformation


class SectorService:
    async def get_sectors_2(self, db: AsyncSession, ticker: Optional[str] = None):
        """
        입력받은 Ticker의 sector_2를 찾고, 그 sector_2에 속한 종목들을 찾는다.
        """
        find_sector_by_ticker = select(StockInformation).where(StockInformation.ticker == ticker)
        result = await db.execute(find_sector_by_ticker)

        sector_2 = result.scalars().first().sector_2

        query = select(StockInformation).where(StockInformation.sector_2 == sector_2)
        result = await db.execute(query)

        return result.scalars().all()


def get_sector_service():
    return SectorService()
