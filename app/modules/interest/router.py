from fastapi import APIRouter, Depends, HTTPException, Query, Request
from typing import Literal, List, Optional, Annotated
from app.utils.oauth_utils import get_current_user
from app.models.models_users import AlphafinderUser
from app.modules.interest.service import InterestService, get_interest_service
from app.modules.interest.response import InterestResponse, InterestTable
from app.modules.interest.request import AddInterestRequest
from app.modules.news.services import get_news_service, NewsService
from app.modules.news.schemas import NewsRenewalResponse, TopStoriesResponse
from app.modules.common.schemas import BaseResponse
from app.modules.common.enum import TranslateCountry
from app.cache.leaderboard import NewsLeaderboard
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/groups")
def get_groups(
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return service.get_interest_group(current_user.id)


@router.get("/news-leaderboard/{group_id}")
def get_news_leaderboard(
    group_id: int,
    lang: TranslateCountry = Query(default=TranslateCountry.KO, description="언어 코드, 예시: ko, en"),
    service: InterestService = Depends(get_interest_service),
):
    redis = NewsLeaderboard()
    ticker_infos = service.get_interest_tickers(group_id)
    tickers = [ticker_info["ticker"] for ticker_info in ticker_infos]
    data = redis.get_leaderboard(lang=lang, tickers=tickers)
    return BaseResponse(status_code=200, message="Successfully retrieved leaderboard data", data=data)


@router.post("/")
def add_interest(
    request: AddInterestRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        is_added = service.add_interest(request.group_id, request.ticker)
        if not is_added:
            raise HTTPException(status_code=400, detail="관심 종목에 추가되지 않았습니다.")
        return {"message": f"관심 종목에 {request.ticker}가 추가되었습니다."}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.delete("/")
def delete_interest(
    group_id: int,
    tickers: List[str],
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        is_deleted = service.delete_interest(group_id, tickers)
        if not is_deleted:
            raise HTTPException(status_code=400, detail="관심 종목에 삭제되지 않았습니다.")
        return {"message": f"관심 종목에서 {', '.join(tickers)}가 삭제되었습니다."}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get("/columns")
def get_columns(lang: Literal["ko", "en"] = "ko"):
    columns = ["티커", "종목명", "현재가", "등락율", "거래대금", "거래량"]
    if lang == "en":
        columns = ["Ticker", "Name", "Price", "Change", "Amount", "Volume"]
    return columns


@router.post("/groups")
def create_group(
    name: str,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        return service.create_interest_group(current_user.id, name)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.delete("/groups")
def delete_group(
    group_id: int,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        return service.delete_interest_group(group_id)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.post("/name")
def update_group_name(
    group_id: int,
    name: str,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        is_updated = service.update_interest_group_name(group_id, name)
        if not is_updated:
            raise HTTPException(status_code=400, detail="관심 그룹 이름 수정에 실패했습니다.")
        return {"message": f"관심 그룹 이름이 {name}으로 수정되었습니다."}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get("/news/{group_id}", response_model=BaseResponse[NewsRenewalResponse])
def interest_news(
    group_id: int,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = None,
    news_service: NewsService = Depends(get_news_service),
    service: InterestService = Depends(get_interest_service),
):
    ticker_infos = service.get_interest_tickers(group_id)
    tickers = [ticker_info["ticker"] for ticker_info in ticker_infos]
    news_data, disclosure_data = news_service.get_renewal_data(lang=lang, tickers=tickers)

    response_data = NewsRenewalResponse(news=news_data, disclosure=disclosure_data)

    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=response_data)


@router.get("/stories/{group_id}", response_model=BaseResponse[List[TopStoriesResponse]])
def top_stories(
    group_id: int,
    request: Request,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en", optional=True)] = None,
    news_service: NewsService = Depends(get_news_service),
    service: InterestService = Depends(get_interest_service),
):
    ticker_infos = service.get_interest_tickers(group_id)
    tickers = [ticker_info["ticker"] for ticker_info in ticker_infos]
    data = news_service.top_stories(request=request, tickers=tickers, lang=lang)
    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=data)


@router.get("/{group_id}/tickers")
def get_interest_tickers(
    group_id: int,
    service: InterestService = Depends(get_interest_service),
):
    ticker_infos = service.get_interest_tickers(group_id)
    return [
        {"ticker": ticker_info["ticker"], "name": ticker_info["name"], "country": ticker_info["country"]}
        for ticker_info in ticker_infos
    ]


@router.get("/{group_id}/count")
def get_interest_count(
    group_id: int,
    service: InterestService = Depends(get_interest_service),
):
    count = service.get_interest_count(group_id)
    return {"count": count}


@router.get("/{group_id}")
def get_interest(
    group_id: int,
    lang: Literal["ko", "en"] = "ko",
    offset: int = 0,
    limit: Optional[int] = 50,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
) -> InterestResponse:
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    interests = service.get_interest(group_id, lang, offset, limit)
    data = [InterestTable.from_dict(interest) for interest in interests["data"]]
    return InterestResponse(has_next=interests["has_next"], data=data)
