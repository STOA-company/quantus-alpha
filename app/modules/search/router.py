from typing import Dict, List, Optional, Union

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.cache.leaderboard import StockLeaderboard
from app.database.conn import db
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from app.modules.common.schemas import InfiniteScrollResponse
from app.modules.community.services import CommunityService, get_community_service
from app.modules.search.schemas import CommunitySearchItem, InterestSearchItem, SearchResponse
from app.modules.search.service import SearchService, get_search_service
from app.utils.quantus_auth_utils import get_current_user

router = APIRouter()


@router.get("", summary="검색 기능")
def search(
    query: Optional[str] = Query(None, description="검색 쿼리"),
    ctry: TranslateCountry = Query(default=TranslateCountry.KO, description="검색 시 나올 기업명 언어(ko, en)"),
    offset: int = Query(0, description="검색 시작 위치"),
    limit: int = Query(20, description="검색 결과 수"),
    service: SearchService = Depends(get_search_service),
    db: Session = Depends(db.get_db),
) -> Union[SearchResponse, List[Dict]]:
    if not query:
        redis = StockLeaderboard()
        return redis.get_leaderboard(ctry)
    # limit + 1개를 요청하여 더 있는지 확인
    search_result = service.search(query, ctry, offset, limit + 1)
    has_more = len(search_result) > limit
    if has_more:
        search_result = search_result[:-1]  # 마지막 항목 제거

    return SearchResponse(status_code=200, message="검색이 완료되었습니다.", data=search_result, has_more=has_more)


@router.get("/community", summary="종목 검색")
async def search_community(
    query: Optional[str] = Query(None, description="검색 쿼리"),
    lang: TranslateCountry = Query(default=TranslateCountry.KO, description="검색 시 나올 기업명 언어(ko, en)"),
    offset: int = Query(0, description="검색 시작 위치"),
    limit: int = Query(20, description="검색 결과 수"),
    service: SearchService = Depends(get_search_service),
) -> InfiniteScrollResponse[CommunitySearchItem]:
    search_result = await service.search_community(query, lang, offset, limit + 1)
    has_more = len(search_result) > limit
    if has_more:
        search_result = search_result[:-1]  # 마지막 항목 제거

    return InfiniteScrollResponse(
        status_code=200, message="검색이 완료되었습니다.", data=search_result, has_more=has_more
    )


@router.get("/interest", summary="관심 종목 검색")
def search_interest(
    query: str | None = None,
    ctry: TranslateCountry = TranslateCountry.KO,
    offset: int = 0,
    limit: int = 10,
    service: SearchService = Depends(get_search_service),
    community_service: CommunityService = Depends(get_community_service),
    user: AlphafinderUser = Depends(get_current_user),
) -> InfiniteScrollResponse[InterestSearchItem]:
    if query is not None:
        query = query.strip()

    if query is None:
        # Get trending stocks
        trending_stocks = community_service.get_trending_stocks()
        # Get interest stocks for the group
        query = """
            SELECT DISTINCT ticker
            FROM alphafinder_interest_stock
            WHERE group_id IN (SELECT group_id FROM alphafinder_interest_group WHERE user_id = :user_id)
        """
        interest_tickers = service.db._execute(text(query), {"user_id": user["uid"]})
        interest_tickers = [ticker[0] for ticker in interest_tickers.fetchall()]

        # Get stock information for trending stocks
        stock_info = service.db._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name", "ctry"],
            ticker__in=[ticker for ticker, _ in trending_stocks],
            is_activate=1,
            is_delisted=0,
        )

        # Convert to InterestSearchItem format
        search_result = []
        for row in stock_info:
            search_result.append(
                InterestSearchItem(
                    ticker=row.ticker,
                    name=row.kr_name if ctry == TranslateCountry.KO else row.en_name,
                    ctry=row.ctry,
                    is_interest=row.ticker in interest_tickers,
                )
            )
    else:
        search_result = service.search_interest(query, user["uid"], ctry, offset, limit + 1)

    has_more = len(search_result) > limit
    if has_more:
        search_result = search_result[:-1]  # 마지막 항목 제거

    return InfiniteScrollResponse(
        status_code=200, message="검색이 완료되었습니다.", data=search_result, has_more=has_more
    )
