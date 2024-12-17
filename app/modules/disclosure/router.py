from typing import Annotated, List, Optional
from fastapi import APIRouter, Query, Depends
from app.modules.common.schemas import PaginationBaseResponse
from app.modules.disclosure.schemas import DisclosureItem
from app.modules.common.enum import FinancialCountry
from app.modules.disclosure.services import DisclosureService, get_disclosure_service

router = APIRouter()


@router.get("", response_model=PaginationBaseResponse[List[DisclosureItem]])
async def get_disclosure(
    ctry: Annotated[FinancialCountry, Query(description="국가 코드 (US)")],
    ticker: Annotated[Optional[str], Query(description="종목 코드, 예시: AAPL")] = None,
    year: Annotated[Optional[str], Query(description="연도, 예시: 2024, 기본값: 올해")] = None,
    page: Annotated[Optional[int], Query(description="페이지 번호, 기본값: 1")] = 1,
    size: Annotated[Optional[int], Query(description="페이지 크기, 기본값: 6")] = 6,
    service: DisclosureService = Depends(get_disclosure_service),
):
    result = await service.get_disclosure(ctry=ctry, ticker=ticker, year=year, page=page, size=size)
    return PaginationBaseResponse(status_code=200, message="Successfully retrieved disclosure data", **result)
