from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.core.logger import setup_logger
from app.modules.common.schemas import BaseResponse
from app.modules.common.utils import check_ticker_country_len_2
from app.modules.dividend.v2.schemas import DividendItem
from app.modules.dividend.v2.services import DividendService, get_dividend_service


logger = setup_logger(__name__)

router = APIRouter()

@router.get("", response_model=BaseResponse[DividendItem], summary="배당 정보 조회")
async def get_dividend_renewal(
    ticker: Annotated[str, Query(description="종목 코드", min_length=1)],
    service: DividendService = Depends(get_dividend_service),
) -> BaseResponse[DividendItem]:
    ctry = check_ticker_country_len_2(ticker)
    dividend_data = await service.get_dividend_renewal(ctry=ctry, ticker=ticker)
    return BaseResponse(status_code=200, message="배당 정보를 성공적으로 조회했습니다.", data=dividend_data)