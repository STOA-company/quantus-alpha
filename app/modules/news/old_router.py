from typing import Annotated, List, Optional
from fastapi import APIRouter, Query, Depends
from app.modules.common.schemas import BaseResponse
from app.modules.news.schemas import LatestNewsResponse, NewsItem, NewsResponse, TopStoriesResponse
from app.modules.news.old_services import NewsService, get_news_service

router = APIRouter()


@router.get("", response_model=NewsResponse[List[NewsItem]])
def get_news(
    ticker: Annotated[Optional[str], Query(description="종목 코드, 예시: A005930, None은 전체 종목")] = None,
    date: Annotated[Optional[str], Query(description="날짜, 예시: 20241210, 기본값: 오늘 날짜")] = None,
    page: Annotated[Optional[int], Query(description="페이지 번호, 기본값: 1")] = 1,
    size: Annotated[Optional[int], Query(description="페이지 크기, 기본값: 6")] = 6,
    news_service: NewsService = Depends(get_news_service),
):
    # TODO: 홈 - 뉴스데이터 로직 수정 필요
    """
    뉴스 데이터를 조회합니다.

    Args:
        ticker: 종목 코드
        date: 조회 날짜
        page: 페이지 번호
        size: 페이지 크기
        news_service: 뉴스 서비스 인스턴스

    Returns:
        NewsResponse: 뉴스 데이터 및 메타 정보
    """
    result = news_service.get_news(page=page, size=size, ticker=ticker, date=date)
    return NewsResponse(status_code=200, message="Successfully retrieved news data", **result)


@router.get("/latest", response_model=BaseResponse[LatestNewsResponse])
def get_latest_news(
    ticker: Annotated[str, Query(description="종목 코드, 예시: A005930, AAPL")],
    news_service: NewsService = Depends(get_news_service),
):
    result = news_service.get_latest_news(ticker=ticker)
    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=result)


@router.get("/old/top_stories", response_model=BaseResponse[List[TopStoriesResponse]])
def get_top_stories(
    news_service: NewsService = Depends(get_news_service),
):
    data = news_service.get_top_stories()
    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=data)
