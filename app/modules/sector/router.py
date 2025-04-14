from fastapi import APIRouter

router = APIRouter()


# @router.get("/sectors_1", summary="sector 1")
# async def get_sectors_1(
#     ticker: Optional[str] = Query(description="종목 코드"),
#     service: SectorService = Depends(get_sector_service),
#     db: AsyncSession = Depends(db.get_async_db),
# ):
#     return await service.get_sectors_2(db, ticker)
