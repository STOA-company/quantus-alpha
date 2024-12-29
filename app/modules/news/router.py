from typing import Annotated, List
from fastapi import APIRouter, Depends, Query
from app.modules.common.schemas import BaseResponse
from app.modules.news.services import NewsService, get_news_service
from app.modules.news.schemas import NewsRenewalResponse, TopStoriesResponse


router = APIRouter()


@router.get("/real_time", summary="실시간 뉴스", response_model=BaseResponse[NewsRenewalResponse])
def news_main(
    ctry: Annotated[str, Query(description="국가 코드, 예시: kr, us")] = None,
    news_service: NewsService = Depends(get_news_service),
):
    news_data = news_service.news_main(ctry=ctry)
    disclosure_data = news_service.disclosure_main(ctry=ctry)

    response_data = NewsRenewalResponse(news=news_data, disclosure=disclosure_data)

    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=response_data)


@router.get("/top_stories", summary="주요소식 모아보기", response_model=BaseResponse[List[TopStoriesResponse]])
def top_stories(
    news_service: NewsService = Depends(get_news_service),
):
    data = news_service.top_stories()
    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=data)
