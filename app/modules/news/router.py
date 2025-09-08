from typing import Annotated, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from app.cache.cache_decorator import one_minute_cache
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.news.schemas import NewsDetailItemV2, NewsRenewalResponse, NewsResponse, TopStoriesResponse
from app.modules.news.services import NewsService, get_news_service
from app.utils.oauth_utils import get_current_user

router = APIRouter()


@router.get("/renewal/real_time", summary="실시간 뉴스", response_model=BaseResponse[NewsRenewalResponse])
@one_minute_cache(prefix="news_real_time")
async def news_main(
    ctry: Annotated[str, Query(description="국가 코드, 예시: kr, us")] = None,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = None,
    news_service: NewsService = Depends(get_news_service),
):
    news_data, disclosure_data = await news_service.get_renewal_data(ctry=ctry, lang=lang)

    response_data = NewsRenewalResponse(news=news_data, disclosure=disclosure_data)

    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=response_data).dict()


@router.get("/top_stories", summary="주요소식 모아보기", response_model=BaseResponse[List[TopStoriesResponse]])
# @one_minute_cache(prefix="news_top_stories")
def top_stories(
    request: Request,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en", optional=True)] = None,
    news_service: NewsService = Depends(get_news_service),
    user: Optional[AlphafinderUser] = Depends(get_current_user),
):
    data = news_service.top_stories(request=request, lang=lang, user=user)
    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=data).dict()


@router.post("/api/stories/{ticker}/{type}/{id}", summary="주요소식 조회 여부 업데이트")
async def mark_story_as_viewed(
    ticker: str,
    type: Literal["news", "disclosure"],
    id: int,
    response: Response,
    request: Request,
    news_service: NewsService = Depends(get_news_service),
    user: Optional[AlphafinderUser] = Depends(get_current_user),
):
    """
    Mark a story as viewed by the current user using Redis.
    Works for both authenticated and anonymous users.

    Args:
        ticker: The stock ticker symbol
        type: The type of story ('news' or 'disclosure')
        id: The ID of the story
        response: FastAPI response object
        request: FastAPI request object
        news_service: NewsService instance
        user: Optional authenticated user

    Returns:
        A response indicating success
    """
    news_service.mark_story_as_viewed(ticker=ticker, type=type, id=id, request=request, response=response, user=user)
    return BaseResponse(status_code=200, message="Successfully updated story view status")


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
    print("TYPE : ", type)
    if type == "stock":
        data, total_count, total_page, offset, emotion_count, ctry = news_service.news_detail_v2(
            ticker=ticker, date=date, end_date=end_date, page=page, size=size, lang=lang, user=user
        )
    elif type == "etf":
        print("ETF")
        data, total_count, total_page, offset, emotion_count, ctry = news_service.etf_news_detail_v2(
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


@router.post("/news_inquiry", summary="뉴스 조회수 증가")
def increase_news_search_count(
    news_id: int,
    ticker: str,
    news_service: NewsService = Depends(get_news_service),
):
    news_service.increase_news_search_count(news_id=news_id, ticker=ticker)
    return BaseResponse(status_code=200, message="Successfully increased search count")


@router.post("/disclosure_inquiry", summary="공시 조회수 증가")
def increase_disclosure_search_count(
    disclosure_id: int,
    ticker: str,
    news_service: NewsService = Depends(get_news_service),
):
    news_service.increase_disclosure_search_count(disclosure_id=disclosure_id, ticker=ticker)
    return BaseResponse(status_code=200, message="Successfully increased search count")
