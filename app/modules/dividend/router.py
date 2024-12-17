from typing import Annotated
from fastapi import APIRouter, Depends, Query
from app.modules.common.enum import FinancialCountry
from app.modules.common.schemas import BaseResponse
from app.modules.dividend.schemas import DividendItem
from app.modules.dividend.services import DividendService, get_dividend_service


router = APIRouter()


@router.get("", response_model=BaseResponse[DividendItem], summary="배당 정보 조회(Mock 데이터)")
async def get_dividend(
    ctry: Annotated[FinancialCountry, Query(description="국가 코드")],
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    service: DividendService = Depends(get_dividend_service),
) -> BaseResponse[DividendItem]:
    dividend_data = await service.get_dividend(ctry=ctry, ticker=ticker)
    return BaseResponse(status_code=200, message="배당 정보를 성공적으로 조회했습니다.", data=dividend_data)
