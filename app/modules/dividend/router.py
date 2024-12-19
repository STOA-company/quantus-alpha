from fastapi import APIRouter
from typing import Annotated
from fastapi import Depends, Query
from app.modules.common.schemas import BaseResponse
from app.modules.common.utils import check_ticker_country_len_3
from app.modules.dividend.schemas import DividendItem
from app.modules.dividend.services import DividendService, get_dividend_service


router = APIRouter()


@router.get("", response_model=BaseResponse[DividendItem], summary="배당 정보 조회(Mock 데이터)")
async def get_dividend(
    ticker: Annotated[str, Query(description="종목 코드 (일단 us만 가능)", min_length=1)],
    service: DividendService = Depends(get_dividend_service),
) -> BaseResponse[DividendItem]:
    ctry = check_ticker_country_len_3(ticker).upper()
    dividend_data = await service.get_dividend(ctry=ctry, ticker=ticker)
    return BaseResponse(status_code=200, message="배당 정보를 성공적으로 조회했습니다.", data=dividend_data)
