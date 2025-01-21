from typing import Annotated, List, Literal
from fastapi import APIRouter, Depends, Query, Request, Response
from app.modules.common.schemas import BaseResponse
from app.modules.news.services import NewsService, get_news_service
from app.modules.news.schemas import NewsDetailItem, NewsRenewalResponse, NewsResponse, TopStoriesResponse


router = APIRouter()


@router.get("/renewal/real_time", summary="실시간 뉴스", response_model=BaseResponse[NewsRenewalResponse])
def news_main(
    ctry: Annotated[str, Query(description="국가 코드, 예시: kr, us")] = None,
    news_service: NewsService = Depends(get_news_service),
):
    news_data, disclosure_data = news_service.get_renewal_data(ctry=ctry)

    response_data = NewsRenewalResponse(news=news_data, disclosure=disclosure_data)

    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=response_data)


@router.get("/top_stories", summary="주요소식 모아보기", response_model=BaseResponse[List[TopStoriesResponse]])
def top_stories(
    request: Request,
    news_service: NewsService = Depends(get_news_service),
):
    data = news_service.top_stories(request=request)
    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=data)


@router.post("/api/stories/{ticker}/{type}/{id}", summary="주요소식 조회 여부 업데이트")
async def mark_story_as_viewed(
    ticker: str,
    type: Literal["news", "disclosure"],
    id: int,
    response: Response,
    request: Request,
    news_service: NewsService = Depends(get_news_service),
):
    news_service.mark_story_as_viewed(ticker=ticker, type=type, id=id, request=request, response=response)
    return BaseResponse(status_code=200, message="Successfully updated story view status")


@router.get("/renewal/detail", summary="상세 페이지 뉴스", response_model=NewsResponse[List[NewsDetailItem]])
def news_detail(
    ticker: Annotated[str, Query(..., description="종목 코드, 예시: AAPL, A110090")],
    date: Annotated[str, Query(description="날짜, 예시: 20241230")] = None,
    page: Annotated[int, Query(description="페이지 번호, 기본값: 1")] = 1,
    size: Annotated[int, Query(description="페이지 사이즈, 기본값: 6")] = 6,
    news_service: NewsService = Depends(get_news_service),
):
    data, total_count, total_page, offset, emotion_count, ctry = news_service.news_detail(
        ticker=ticker, date=date, page=page, size=size
    )
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
