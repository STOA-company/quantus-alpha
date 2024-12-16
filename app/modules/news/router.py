from datetime import date
from typing import Annotated, List, Optional
from fastapi import APIRouter, Query, Depends
from app.modules.news.schemas import NewsItem, NewsResponse
from app.modules.common.enum import Country
from app.modules.news.services import NewsService, get_news_service

router = APIRouter()

@router.get("/", response_model=NewsResponse[List[NewsItem]])
async def get_news(
    ctry: Annotated[Country, Query(description="국가 코드 (KR / US)")],
    ticker: Annotated[Optional[str], Query(description="종목 코드, 예시: 005930")] = None,
    date: Annotated[Optional[str], Query(description="날짜, 예시: 20241210, 기본값: 오늘 날짜")] = None,
    page: Annotated[Optional[int], Query(description="페이지 번호, 기본값: 1")] = 1,
    size: Annotated[Optional[int], Query(description="페이지 크기, 기본값: 6")] = 6,
    news_service: NewsService = Depends(get_news_service),
):
    """
    뉴스 데이터를 조회합니다.
    
    Args:
        ctry: 국가 코드
        ticker: 종목 코드
        date: 조회 날짜
        page: 페이지 번호
        size: 페이지 크기
        news_service: 뉴스 서비스 인스턴스
        
    Returns:
        NewsResponse: 뉴스 데이터 및 메타 정보
    """
    result = await news_service.get_news(page=page, size=size, ctry=ctry, ticker=ticker, date=date)
    return NewsResponse(
        status_code=200,
        message="Successfully retrieved news data",
        **result
    )
