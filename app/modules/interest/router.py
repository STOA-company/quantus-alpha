from fastapi import APIRouter, Depends, HTTPException, Query, Request
from typing import Literal, List, Optional, Annotated
from app.utils.oauth_utils import get_current_user
from app.models.models_users import AlphafinderUser
from app.modules.interest.service import InterestService, get_interest_service
from app.modules.interest.response import InterestResponse, InterestTable
from app.modules.interest.request import AddInterestRequest, DeleteInterestRequest, UpdateInterestRequest
from app.modules.news.services import get_news_service, NewsService
from app.modules.news.schemas import TopStoriesResponse, InterestNewsResponse, InterestDisclosureResponse
from app.modules.common.schemas import BaseResponse
from app.modules.common.enum import TranslateCountry
from app.core.exception.base import DuplicateException, NotFoundException
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
    data = service.get_interest_news_leaderboard(group_id, lang)
    return BaseResponse(status_code=200, message="Successfully retrieved leaderboard data", data=data)


@router.get("/disclosure-leaderboard/{group_id}")
def get_disclosure_leaderboard(
    group_id: int,
    lang: TranslateCountry = Query(default=TranslateCountry.KO, description="언어 코드, 예시: ko, en"),
    service: InterestService = Depends(get_interest_service),
):
    data = service.get_interest_disclosure_leaderboard(group_id, lang)
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
    request: DeleteInterestRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        is_deleted = service.delete_interest(request.group_id, request.tickers)
        if not is_deleted:
            raise HTTPException(status_code=400, detail="관심 종목에 삭제되지 않았습니다.")
        return {"message": f"관심 종목에서 {', '.join(request.tickers)}가 삭제되었습니다."}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.post("/update")
def update_interest(
    request: UpdateInterestRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    service.update_interest(current_user.id, request.group_ids, request.ticker)
    return {"message": f"종목 : {request.ticker}, 그룹 : {', '.join(map(str, request.group_ids))} 수정되었습니다."}


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
    except DuplicateException as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.message)
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
        service.update_interest_group_name(group_id, name)
        return {"message": f"관심 그룹 이름이 {name}으로 수정되었습니다."}
    except NotFoundException as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except DuplicateException as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get("/news/{group_id}", response_model=BaseResponse[InterestNewsResponse])
def interest_news(
    group_id: int,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = "ko",
    offset: Annotated[int, Query(description="페이지 번호, 기본값: 0")] = 0,
    limit: Annotated[int, Query(description="페이지 사이즈, 기본값: 10")] = 20,
    news_service: NewsService = Depends(get_news_service),
    service: InterestService = Depends(get_interest_service),
):
    ticker_infos = service.get_interest_tickers(group_id)
    if len(ticker_infos) == 0:
        return BaseResponse(
            status_code=200,
            message="Successfully retrieved news data",
            data=InterestNewsResponse(news=[], has_next=False),
        )
    tickers = [ticker_info["ticker"] for ticker_info in ticker_infos]
    total_news_data = news_service.get_news(lang=lang, tickers=tickers)
    news_data = total_news_data[offset * limit : offset * limit + limit]
    has_next = len(total_news_data) > offset * limit + limit

    response_data = InterestNewsResponse(news=news_data, has_next=has_next)

    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=response_data)


@router.get("/disclosure/{group_id}", response_model=BaseResponse[InterestDisclosureResponse])
def interest_disclosure(
    group_id: int,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = "ko",
    offset: Annotated[int, Query(description="페이지 번호, 기본값: 0")] = 0,
    limit: Annotated[int, Query(description="페이지 사이즈, 기본값: 10")] = 20,
    news_service: NewsService = Depends(get_news_service),
    service: InterestService = Depends(get_interest_service),
):
    ticker_infos = service.get_interest_tickers(group_id)
    if len(ticker_infos) == 0:
        return BaseResponse(
            status_code=200,
            message="Successfully retrieved news data",
            data=InterestDisclosureResponse(disclosure=[], has_next=False),
        )
    tickers = [ticker_info["ticker"] for ticker_info in ticker_infos]
    total_disclosure_data = news_service.get_disclosure(lang=lang, tickers=tickers)
    disclosure_data = total_disclosure_data[offset * limit : offset * limit + limit]
    has_next = len(total_disclosure_data) > offset * limit + limit

    response_data = InterestDisclosureResponse(disclosure=disclosure_data, has_next=has_next)

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
    if len(ticker_infos) == 0:
        return BaseResponse(status_code=200, message="Successfully retrieved news data", data=[])
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


@router.get("/info/{ticker}")
def get_interest_info(
    ticker: str,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    if not current_user:
        return {"is_interested": False, "groups": []}
    return service.get_interest_info(current_user.id, ticker)
