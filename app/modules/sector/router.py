from typing import Optional
from fastapi import APIRouter, Depends, Query

from app.modules.sector.services import SectorService, get_sector_service
from app.database.conn import db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


@router.get("/sectors_1", summary="sector 1")
async def get_sectors_1(
    ticker: Optional[str] = Query(description="종목 코드"),
    service: SectorService = Depends(get_sector_service),
    db: AsyncSession = Depends(db.get_async_db),
):
    return await service.get_sectors_2(db, ticker)
