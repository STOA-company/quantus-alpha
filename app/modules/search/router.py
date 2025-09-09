from typing import Dict, List, Optional, Union

from fastapi import APIRouter, Depends, Query, HTTPException
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
    if query is not None:
        query = query.strip()

    search_result = await service.search_community(query, lang, offset, limit)
    if query:
        has_more = len(search_result) > limit
        if has_more:
            search_result = search_result[:-1]  # 마지막 항목 제거
    else:
        has_more = True if offset <= 20 else False

    return InfiniteScrollResponse(
        status_code=200, message="검색이 완료되었습니다.", data=search_result, has_more=has_more
    )


@router.get("/interest", summary="관심 종목 검색")
def search_interest(
    query: str | None = None,
    ctry: TranslateCountry = TranslateCountry.KO,
    offset: int = 0,
    limit: int = 10,
    lang: TranslateCountry = TranslateCountry.KO,
    service: SearchService = Depends(get_search_service),
    community_service: CommunityService = Depends(get_community_service),
    user: AlphafinderUser = Depends(get_current_user),
) -> InfiniteScrollResponse[InterestSearchItem]:
    if query is not None:
        query = query.strip()

    if query is None:
        # Get trending stocks
        # trending_stocks = community_service.get_trending_stocks()
        trending_stocks = community_service.get_top_10_stocks(offset, limit, lang)
        # Get interest stocks for the group
        _query = """
            SELECT DISTINCT ticker
            FROM alphafinder_interest_stock
            WHERE group_id IN (SELECT group_id FROM alphafinder_interest_group WHERE user_id = :user_id)
        """
        interest_tickers = service.service_db._execute(text(_query), {"user_id": user["uid"]})
        interest_tickers = [ticker[0] for ticker in interest_tickers.fetchall()]

        # Get stock information for trending stocks
        stock_info_condition = {
            "is_activate": 1,
            "is_delisted": 0,
            # "ticker__in": [ticker for ticker, _ in trending_stocks],
            "ticker__in": trending_stocks,
        }
        if lang == TranslateCountry.KO:
            stock_info_condition["kr_name__not"] = None
        else:
            stock_info_condition["en_name__not"] = None
        stock_info = service.db._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name", "ctry"],
            **stock_info_condition,
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
    if query is None or query == "":
        has_more = True if offset <= 20 else False
    else:
        has_more = len(search_result) > limit
        if has_more:
            search_result = search_result[:-1]  # 마지막 항목 제거

    return InfiniteScrollResponse(
        status_code=200, message="검색이 완료되었습니다.", data=search_result, has_more=has_more
    )


# @router.get("/elasticsearch", summary="Elasticsearch 통합 주식 검색")
# async def elasticsearch_search_stocks(
#     q: Optional[str] = Query(None, description="검색어 (티커, 회사명)"),
#     market: Optional[str] = Query(None, description="시장 (KOSPI, KOSDAQ 등)"),
#     ctry: Optional[str] = Query(None, description="국가"),
#     min_price: Optional[float] = Query(None, description="최소 가격"),
#     max_price: Optional[float] = Query(None, description="최대 가격"),
#     size: int = Query(20, ge=1, le=100, description="결과 개수"),
#     page: int = Query(1, ge=1, description="페이지 번호")
# ):
#     """
#     Elasticsearch를 이용한 주식 통합 검색 API
#     - 모든 quantus 인덱스에서 동시 검색
#     - 티커, 회사명으로 검색 가능
#     - 시장, 국가, 가격대별 필터링 지원
#     """
    
#     # 페이지네이션 계산
#     from_ = (page - 1) * size
    
#     # 필터 구성
#     filters = {}
#     if market:
#         filters["market"] = market
#     if ctry:
#         filters["ctry"] = ctry
#     if min_price is not None or max_price is not None:
#         filters["price_range"] = {}
#         if min_price is not None:
#             filters["price_range"]["min"] = min_price
#         if max_price is not None:
#             filters["price_range"]["max"] = max_price
    
#     try:
#         result = await ElasticsearchService.search_stocks(
#             query_text=q or "",
#             filters=filters if filters else None,
#             size=size,
#             from_=from_
#         )
        
#         return {
#             "success": True,
#             "data": result["results"],
#             "pagination": {
#                 "total": result["total"],
#                 "page": page,
#                 "size": size,
#                 "total_pages": (result["total"] + size - 1) // size
#             },
#             "query_info": result.get("query_info", {})
#         }
        
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


# @router.get("/elasticsearch/{ticker}", summary="Elasticsearch 티커별 주식 조회")
# async def elasticsearch_get_stock_detail(ticker: str):
#     """
#     Elasticsearch에서 티커로 특정 주식 정보 조회
#     """
#     try:
#         result = await ElasticsearchService.get_stock_by_ticker(ticker)
        
#         if not result:
#             raise HTTPException(status_code=404, detail=f"Stock with ticker '{ticker}' not found")
        
#         return {
#             "success": True,
#             "data": result
#         }
        
#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to get stock: {str(e)}")


# @router.get("/elasticsearch/trending", summary="Elasticsearch 인기 주식 조회")
# async def elasticsearch_get_trending_stocks(
#     limit: int = Query(10, ge=1, le=50, description="조회할 종목 수")
# ):
#     """
#     Elasticsearch에서 거래량 기준 인기 주식 조회
#     """
#     try:
#         result = await ElasticsearchService.get_trending_stocks(limit)
        
#         return {
#             "success": True,
#             "data": result,
#             "count": len(result)
#         }
        
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to get trending stocks: {str(e)}")
