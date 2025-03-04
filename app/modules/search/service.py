from typing import List

from fastapi import Depends
from app.database.crud import database
from app.modules.common.enum import TranslateCountry
from app.modules.community.services import CommunityService, get_community_service
from app.modules.search.schemas import CommunitySearchItem, SearchItem
import logging

logger = logging.getLogger(__name__)


class SearchService:
    def __init__(self):
        self.db = database

    def search(self, query: str, ctry: TranslateCountry, offset: int, limit: int) -> List[SearchItem]:
        """
        입력받는 query에 따른 종목 검색 기능

        Args:
            query (str): 검색어
            ctry (TranslateCountry): 언어 설정
            offset (int): 시작 위치
            limit (int): 요청할 항목 수 (실제 반환되는 개수는 limit 또는 limit-1)
            db (AsyncSession): 데이터베이스 세션

        Returns:
            List[SearchItem]: 검색 결과 리스트
        """

        search_term = f"%{query}%"

        search_result = self.db._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name", "ctry"],
            or__=[
                {"ticker": query},
                {"ticker__like": search_term},
                {"kr_name__like": search_term},
                {"en_name__like": search_term},
            ],
            is_activate=1,
            is_delisted=0,
        )

        logger.warning(f"Initial search result for AA: {search_result}")

        if not search_result:
            return []

        sorted_result = sorted(search_result, key=lambda x: 1 if x._mapping["ticker"] == query else 2)

        search_result = sorted_result[offset : offset + limit]

        logger.warning(f"Sorted result: {sorted_result}")

        country_groups = {}
        search_map = {}
        for row in search_result:
            mapping = row._mapping
            ticker = mapping["ticker"]
            country = mapping["ctry"]
            country_groups.setdefault(country.lower(), []).append(ticker)
            search_map[ticker] = {
                "name": mapping["kr_name"] if ctry == TranslateCountry.KO else mapping["en_name"],
                "language": ctry,
            }

        logger.warning(f"Search map: {search_map}")

        prices = {}
        for country, tickers in country_groups.items():
            try:
                price_results = self.db._select(
                    table="stock_trend",
                    columns=["ticker", "current_price", "prev_close", "change_rt"],
                    ticker__in=tickers,
                    order="last_updated",
                    ascending=False,
                    limit=len(tickers),
                )

                logger.warning(f"Price results for {tickers}: {price_results}")

                for row in price_results:
                    mapping = row._mapping
                    ticker = mapping["ticker"]
                    if ticker not in prices:
                        try:
                            current_price = float(mapping["current_price"])
                            rate = float(mapping["change_rt"])
                            prices[ticker] = (current_price, rate)
                        except (ValueError, TypeError):
                            prices[ticker] = (None, None)

            except Exception:
                prices.update({ticker: (None, None) for ticker in tickers})

        search_items = []
        for row in search_result:
            ticker = row._mapping["ticker"]
            item_info = search_map[ticker]
            current_price, rate = prices.get(ticker, (None, None))

            logger.warning(f"Ticker: {ticker}, Item info: {item_info}, Current price: {current_price}, Rate: {rate}")
            if rate is not None:
                search_items.append(
                    SearchItem(
                        ticker=ticker,
                        name=item_info["name"],
                        language=item_info["language"],
                        current_price=current_price,
                        current_price_rate=round(rate, 2),
                    )
                )

        return search_items

    async def search_community(
        self,
        query: str,
        lang: TranslateCountry,
        offset: int,
        limit: int,
        community_service: CommunityService = Depends(get_community_service),
    ) -> List[CommunitySearchItem]:
        """커뮤니티 종목 검색 기능"""
        # if not query:
        #     service = get_community_service()
        #     trending_stocks = await service.get_trending_stocks(limit=limit, lang=lang)
        #     print(f"Trending stocks: {trending_stocks}###")
        #     return [
        #         CommunitySearchItem(
        #             ticker=stock.ticker,
        #             name=stock.name,
        #             ctry=stock.ctry,
        #         )
        #         for stock in trending_stocks
        #     ]
        # else:
        return await self._search_result(query, lang, offset, limit)

    async def _search_result(
        self, query: str, lang: TranslateCountry, offset: int, limit: int
    ) -> List[CommunitySearchItem]:
        """종목 검색"""
        search_term = f"%{query}%"

        search_result = self.db._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name", "ctry"],
            or__=[
                {"ticker": query},
                {"ticker__like": search_term},
                {"kr_name__like": search_term},
                {"en_name__like": search_term},
            ],
            is_activate=1,
        )

        if not search_result:
            return []

        # 정확한 매칭을 우선으로 정렬
        sorted_result = sorted(search_result, key=lambda x: 1 if x._mapping["ticker"] == query else 2)
        search_result = sorted_result[offset : offset + limit]

        return [
            CommunitySearchItem(
                ticker=row._mapping["ticker"],
                name=row._mapping["kr_name"] if lang == TranslateCountry.KO else row._mapping["en_name"],
                ctry=row._mapping["ctry"],
            )
            for row in search_result
        ]


def get_search_service() -> SearchService:
    return SearchService()
