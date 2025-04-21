from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.modules.common.enum import TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.news.old_services import NewsService, get_news_service
from app.modules.news.schemas import LatestNewsResponse

router = APIRouter()


@router.get("/latest", response_model=BaseResponse[LatestNewsResponse])
def get_latest_news(
    ticker: Annotated[str, Query(description="종목 코드, 예시: A005930, AAPL")],
    lang: Annotated[TranslateCountry, Query(description="언어, 예시: KO, EN")] = TranslateCountry.KO,
    type: Annotated[str, Query(description="타입, 예시: stock, etf")] = "stock",
    news_service: NewsService = Depends(get_news_service),
):
    if type == "stock":
        result = news_service.get_latest_news(ticker=ticker, lang=lang)
    elif type == "etf":
        result = news_service.get_latest_etf_news(ticker=ticker, lang=lang)
    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=result)
