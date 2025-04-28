from typing import Annotated, List

from fastapi import APIRouter, Depends, HTTPException, Query

from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from app.modules.news.v2.schemas import NewsDetailItemV2, NewsResponse
from app.modules.news.v2.services import NewsService, get_news_service
from app.utils.quantus_auth_utils import get_current_user

router = APIRouter()


@router.get("/renewal/detail/v2", summary="상세 페이지 뉴스", response_model=NewsResponse[List[NewsDetailItemV2]])
def news_detail_v2(
    ticker: Annotated[str, Query(..., description="종목 코드, 예시: AAPL, A110090")],
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = None,
    type: Annotated[str, Query(description="타입, 예시: stock, etf")] = None,
    date: Annotated[str, Query(description="시작 날짜, YYYYMMDD")] = None,
    end_date: Annotated[str, Query(description="종료 날짜, YYYYMMDD")] = None,
    page: Annotated[int, Query(description="페이지 번호, 기본값: 1")] = 1,
    size: Annotated[int, Query(description="페이지 사이즈, 기본값: 6")] = 6,
    news_service: NewsService = Depends(get_news_service),
    user: AlphafinderUser = Depends(get_current_user),
):
    type = type.lower()
    if type == "stock":
        data, total_count, total_page, offset, emotion_count, ctry = news_service.news_detail(
            ticker=ticker, date=date, end_date=end_date, page=page, size=size, lang=lang, user=user
        )
    elif type == "etf":
        data, total_count, total_page, offset, emotion_count, ctry = news_service.etf_news_detail(
            ticker=ticker, date=date, end_date=end_date, page=page, size=size, lang=lang, user=user
        )
    else:
        raise HTTPException(status_code=400, detail="Invalid type")

    return NewsResponse(
        status_code=200,
        message="Successfully retrieved news data",
        data=data,
        total_count=total_count,
        total_pages=total_page,
        current_page=page,
        offset=offset,
        size=size,
        positive_count=emotion_count.get("positive", 0),
        negative_count=emotion_count.get("negative", 0),
        neutral_count=emotion_count.get("neutral", 0),
        ctry=ctry,
    )
