from typing import List

from sqlalchemy import text

from app.core.logger import setup_logger
from app.database.crud import database, database_service
from app.modules.common.enum import TranslateCountry
from app.modules.community.services import get_community_service
from app.modules.search.schemas import CommunitySearchItem, InterestSearchItem, SearchItem

logger = setup_logger(__name__)


class SearchService:
    def __init__(self):
        self.db = database
        self.service_db = database_service

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

        original_query = query.strip()

        # 공백 제거
        normalized_query = original_query.replace(" ", "")

        # 각 글자 사이에 와일드카드
        query_chars = list(normalized_query)
        wildcard_query = "%".join(query_chars)

        # 양방향 LIKE 검색
        original_search_term = f"%{original_query}%"
        normalized_search_term = f"%{normalized_query}%"
        wildcard_search_term = f"%{wildcard_query}%"

        logger.warning(
            f"Search started with: '{original_query}', Normalized: '{normalized_query}', Wildcard: '{wildcard_query}'"
        )

        or_conditions = []

        or_conditions.append({"ticker": original_query})
        or_conditions.append({"ticker__like": original_search_term})
        or_conditions.append({"ticker__like": normalized_search_term})

        # 모든 글자가 순서대로 있지만 사이에 다른 문자가 있을 수 있는 경우 (ex: 메타플 -> 메타 플랫폼스)
        for char in query_chars:
            or_conditions.append({"ticker__like": f"%{char}%"})
            or_conditions.append({"kr_name__like": f"%{char}%"})
            or_conditions.append({"en_name__like": f"%{char}%"})

        or_conditions.append({"kr_name__like": original_search_term})
        or_conditions.append({"en_name__like": original_search_term})
        or_conditions.append({"kr_name__like": normalized_search_term})
        or_conditions.append({"en_name__like": normalized_search_term})
        or_conditions.append({"kr_name__like": wildcard_search_term})
        or_conditions.append({"en_name__like": wildcard_search_term})

        # 각 단어 검색 (띄어쓰기로 구분된 각 단어 검색)
        words = original_query.split()
        if len(words) > 1:
            for word in words:
                if len(word) >= 2:  # 최소 2글자 이상인 단어만 검색
                    or_conditions.append({"kr_name__like": f"%{word}%"})
                    or_conditions.append({"en_name__like": f"%{word}%"})

        search_result = self.db._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name", "ctry"],
            or__=or_conditions,
            is_activate=1,
            is_delisted=0,
        )

        logger.warning(f"Search query: '{original_query}', Normalized: '{normalized_query}'")
        logger.warning(f"Initial search result count: {len(search_result)}")

        if not search_result:
            return []

        # 메모리 레벨에서 향상된 필터링 및 점수 부여
        scored_results = []
        for row in search_result:
            mapping = row._mapping
            ticker = mapping["ticker"].lower() if mapping["ticker"] else ""
            kr_name = mapping["kr_name"].lower() if mapping["kr_name"] else ""
            en_name = mapping["en_name"].lower() if mapping["en_name"] else ""

            # 검색어도 소문자로 변환
            original_lower = original_query.lower()
            normalized_lower = normalized_query.lower()

            # 초기 점수 설정
            score = 0

            # 1. 정확한 일치 시 높은 점수
            if ticker == original_lower or ticker == normalized_lower:
                score += 100
            elif kr_name == original_lower or en_name == original_lower:
                score += 90

            # 2. 부분 문자열 일치 시 점수 추가
            if original_lower in ticker:
                score += 80
            elif normalized_lower in ticker.replace(" ", ""):
                score += 75

            if original_lower in kr_name:
                score += 70
            elif normalized_lower in kr_name.replace(" ", ""):
                score += 65

            if original_lower in en_name:
                score += 70
            elif normalized_lower in en_name.replace(" ", ""):
                score += 65

            # 3. 검색어의 모든 문자가 순서대로 포함되어 있는지 확인
            # 예: '메타플'이 '메타 플랫폼스'에 포함되는지
            name_without_spaces = kr_name.replace(" ", "")
            if normalized_lower in name_without_spaces:
                score += 60

            name_without_spaces = en_name.replace(" ", "")
            if normalized_lower in name_without_spaces:
                score += 60

            # 4. 단어 단위 일치 여부
            if len(words) > 1:
                for word in words:
                    word_lower = word.lower()
                    if word_lower in kr_name.split() or word_lower in en_name.split():
                        score += 40

            # 일정 점수 이상인 결과만 포함
            if score > 0:
                scored_results.append((row, score))

        # 점수 기준으로 정렬하고 상위 결과만 유지
        scored_results.sort(key=lambda x: x[1], reverse=True)
        search_result = [item[0] for item in scored_results]

        logger.warning(f"Scored and filtered results count: {len(search_result)}")

        if scored_results:
            top_results = [(r[0]._mapping["ticker"], r[0]._mapping["kr_name"], r[1]) for r in scored_results[:5]]
            logger.warning(f"Top scoring results: {top_results}")

        search_result = search_result[offset : offset + limit]

        logger.warning(f"Final result count after offset/limit: {len(search_result)}")

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
        lang: TranslateCountry = TranslateCountry.KO,
        offset: int = 0,
        limit: int = 10,
    ) -> List[CommunitySearchItem]:
        """커뮤니티 종목 검색 기능"""
        if not query:
            community_service = get_community_service()
            # trending_stocks = community_service.get_trending_stocks()
            trending_stocks = community_service.get_top_10_stocks(offset, limit, lang)
            # 페이지네이션 적용
            # paginated_stocks = trending_stocks[offset : offset + limit]
            paginated_stocks = trending_stocks
            if lang == TranslateCountry.KO:
                name_column = "kr_name"
            else:
                name_column = "en_name"

            stock_info_condition = {
                # "ticker__in": [ticker for ticker, _ in paginated_stocks],
                "ticker__in": paginated_stocks,
            }
            if lang == TranslateCountry.KO:
                stock_info_condition["kr_name__not"] = None
            else:
                stock_info_condition["en_name__not"] = None

            stock_info = self.db._select(
                table="stock_information",
                columns=["ticker", name_column, "ctry"],
                **stock_info_condition,
            )
            # trending_stocks에 맞춰서 정렬
            # stock_info = [
            #     item for item in stock_info if item._mapping["ticker"] in [ticker for ticker, _ in paginated_stocks]
            # ]
            return [
                CommunitySearchItem(
                    ticker=ticker,
                    name=name,
                    ctry=ctry,
                )
                for ticker, name, ctry in stock_info
            ]
        return await self._search_result(query, lang, offset, limit + 1)

    async def _search_result(
        self, query: str, lang: TranslateCountry, offset: int, limit: int
    ) -> List[CommunitySearchItem]:
        """종목 검색"""
        # 원래 검색어 저장
        original_query = query.strip()

        # 공백 제거된 검색어 생성
        normalized_query = original_query.replace(" ", "")

        # 추가적인 검색 패턴 생성
        query_chars = list(normalized_query)
        wildcard_query = "%".join(query_chars)  # 각 글자 사이에 와일드카드 추가

        # 양방향 LIKE 검색을 위한 패턴 생성
        original_search_term = f"%{original_query}%"
        normalized_search_term = f"%{normalized_query}%"
        wildcard_search_term = f"%{wildcard_query}%"

        # SQL 쿼리 생성
        or_conditions = []

        # 티커 검색 (정확한 매칭)
        or_conditions.append({"ticker": original_query})

        # 티커 검색 (부분 매칭)
        or_conditions.append({"ticker__like": original_search_term})
        or_conditions.append({"ticker__like": normalized_search_term})

        # 모든 글자가 순서대로 있지만 사이에 다른 문자가 있을 수 있는 경우 (ex: 메타플 -> 메타 플랫폼스)
        for char in query_chars:
            or_conditions.append({"ticker__like": f"%{char}%"})
            or_conditions.append({"kr_name__like": f"%{char}%"})
            or_conditions.append({"en_name__like": f"%{char}%"})

        # 한글명/영문명 검색 (부분 매칭)
        or_conditions.append({"kr_name__like": original_search_term})
        or_conditions.append({"en_name__like": original_search_term})
        or_conditions.append({"kr_name__like": normalized_search_term})
        or_conditions.append({"en_name__like": normalized_search_term})
        or_conditions.append({"kr_name__like": wildcard_search_term})
        or_conditions.append({"en_name__like": wildcard_search_term})

        # 각 단어 검색 (띄어쓰기로 구분된 각 단어 검색)
        words = original_query.split()
        if len(words) > 1:
            for word in words:
                if len(word) >= 2:  # 최소 2글자 이상인 단어만 검색
                    or_conditions.append({"kr_name__like": f"%{word}%"})
                    or_conditions.append({"en_name__like": f"%{word}%"})

        # DB에서 검색 실행
        search_result = self.db._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name", "ctry"],
            or__=or_conditions,
            is_activate=1,
            kr_name__not=None,
            en_name__not=None,
            ctry__not=None,
        )

        if not search_result:
            return []

        # 메모리 레벨에서 향상된 필터링 및 점수 부여
        scored_results = []
        for row in search_result:
            mapping = row._mapping
            ticker = mapping["ticker"].lower() if mapping["ticker"] else ""
            kr_name = mapping["kr_name"].lower() if mapping["kr_name"] else ""
            en_name = mapping["en_name"].lower() if mapping["en_name"] else ""

            # 검색어도 소문자로 변환
            original_lower = original_query.lower()
            normalized_lower = normalized_query.lower()

            # 초기 점수 설정
            score = 0

            # 1. 정확한 일치 시 높은 점수
            if ticker == original_lower or ticker == normalized_lower:
                score += 100
            elif kr_name == original_lower or en_name == original_lower:
                score += 90

            # 2. 부분 문자열 일치 시 점수 추가
            if original_lower in ticker:
                score += 80
            elif normalized_lower in ticker.replace(" ", ""):
                score += 75

            if original_lower in kr_name:
                score += 70
            elif normalized_lower in kr_name.replace(" ", ""):
                score += 65

            if original_lower in en_name:
                score += 70
            elif normalized_lower in en_name.replace(" ", ""):
                score += 65

            # 3. 검색어의 모든 문자가 순서대로 포함되어 있는지 확인
            # 예: '메타플'이 '메타 플랫폼스'에 포함되는지
            name_without_spaces = kr_name.replace(" ", "")
            if normalized_lower in name_without_spaces:
                score += 60

            name_without_spaces = en_name.replace(" ", "")
            if normalized_lower in name_without_spaces:
                score += 60

            # 4. 단어 단위 일치 여부
            if len(words) > 1:
                for word in words:
                    word_lower = word.lower()
                    if word_lower in kr_name.split() or word_lower in en_name.split():
                        score += 40

            # 일정 점수 이상인 결과만 포함
            if score > 0:
                scored_results.append((row, score))

        # 점수 기준으로 정렬하고 상위 결과만 유지
        scored_results.sort(key=lambda x: x[1], reverse=True)
        search_result = [item[0] for item in scored_results]

        # 오프셋 및 리밋 적용
        search_result = search_result[offset : offset + limit]

        return [
            CommunitySearchItem(
                ticker=row._mapping["ticker"],
                name=row._mapping["kr_name"] if lang == TranslateCountry.KO else row._mapping["en_name"],
                ctry=row._mapping["ctry"],
            )
            for row in search_result
        ]

    def search_interest(
        self, query: str, user_id: int, ctry: TranslateCountry, offset: int, limit: int
    ) -> List[InterestSearchItem]:
        """
        관심 종목 검색

        Args:
            query (str): 검색어
            user_id (int): 유저 ID
            ctry (TranslateCountry): 언어 설정
            offset (int): 시작 위치
            limit (int): 요청할 항목 수

        Returns:
            List[InterestSearchItem]: 검색 결과 리스트
        """
        original_query = query.strip()

        # 공백 제거
        normalized_query = original_query.replace(" ", "")

        # 각 글자 사이에 와일드카드
        query_chars = list(normalized_query)
        wildcard_query = "%".join(query_chars)

        # 양방향 LIKE 검색
        original_search_term = f"%{original_query}%"
        normalized_search_term = f"%{normalized_query}%"
        wildcard_search_term = f"%{wildcard_query}%"

        logger.warning(
            f"Search started with: '{original_query}', Normalized: '{normalized_query}', Wildcard: '{wildcard_query}'"
        )

        or_conditions = []

        or_conditions.append({"ticker": original_query})
        or_conditions.append({"ticker__like": original_search_term})
        or_conditions.append({"ticker__like": normalized_search_term})

        # 모든 글자가 순서대로 있지만 사이에 다른 문자가 있을 수 있는 경우 (ex: 메타플 -> 메타 플랫폼스)
        for char in query_chars:
            or_conditions.append({"ticker__like": f"%{char}%"})
            or_conditions.append({"kr_name__like": f"%{char}%"})
            or_conditions.append({"en_name__like": f"%{char}%"})

        or_conditions.append({"kr_name__like": original_search_term})
        or_conditions.append({"en_name__like": original_search_term})
        or_conditions.append({"kr_name__like": normalized_search_term})
        or_conditions.append({"en_name__like": normalized_search_term})
        or_conditions.append({"kr_name__like": wildcard_search_term})
        or_conditions.append({"en_name__like": wildcard_search_term})

        # 각 단어 검색 (띄어쓰기로 구분된 각 단어 검색)
        words = original_query.split()
        if len(words) > 1:
            for word in words:
                if len(word) >= 2:  # 최소 2글자 이상인 단어만 검색
                    or_conditions.append({"kr_name__like": f"%{word}%"})
                    or_conditions.append({"en_name__like": f"%{word}%"})

        search_result = self.db._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name", "ctry"],
            or__=or_conditions,
            is_activate=1,
            is_delisted=0,
            kr_name__not=None,
            en_name__not=None,
            ctry__not=None,
        )

        logger.warning(f"Search query: '{original_query}', Normalized: '{normalized_query}'")
        logger.warning(f"Initial search result count: {len(search_result)}")

        if not search_result:
            return []

        # Get interest stocks for the group
        # interest_stocks = self.service_db._select(
        #     table="alphafinder_interest_stock", columns=["ticker"], group_id=group_id
        # )
        # interest_tickers = {stock.ticker for stock in interest_stocks}

        query = """
            SELECT DISTINCT ticker
            FROM alphafinder_interest_stock
            WHERE group_id IN (SELECT group_id FROM alphafinder_interest_group WHERE user_id = :user_id)
        """
        interest_tickers = self.service_db._execute(text(query), {"user_id": user_id})
        interest_tickers = [ticker[0] for ticker in interest_tickers.fetchall()]

        # 메모리 레벨에서 향상된 필터링 및 점수 부여
        scored_results = []
        for row in search_result:
            mapping = row._mapping
            ticker = mapping["ticker"].lower() if mapping["ticker"] else ""
            kr_name = mapping["kr_name"].lower() if mapping["kr_name"] else ""
            en_name = mapping["en_name"].lower() if mapping["en_name"] else ""

            # 검색어도 소문자로 변환
            original_lower = original_query.lower()
            normalized_lower = normalized_query.lower()

            # 초기 점수 설정
            score = 0

            # 1. 정확한 일치 시 높은 점수
            if ticker == original_lower or ticker == normalized_lower:
                score += 100
            elif kr_name == original_lower or en_name == original_lower:
                score += 90

            # 2. 부분 문자열 일치 시 점수 추가
            if original_lower in ticker:
                score += 80
            elif normalized_lower in ticker.replace(" ", ""):
                score += 75

            if original_lower in kr_name:
                score += 70
            elif normalized_lower in kr_name.replace(" ", ""):
                score += 65

            if original_lower in en_name:
                score += 70
            elif normalized_lower in en_name.replace(" ", ""):
                score += 65

            # 3. 검색어의 모든 문자가 순서대로 포함되어 있는지 확인
            # 예: '메타플'이 '메타 플랫폼스'에 포함되는지
            name_without_spaces = kr_name.replace(" ", "")
            if normalized_lower in name_without_spaces:
                score += 60

            name_without_spaces = en_name.replace(" ", "")
            if normalized_lower in name_without_spaces:
                score += 60

            # 4. 단어 단위 일치 여부
            if len(words) > 1:
                for word in words:
                    word_lower = word.lower()
                    if word_lower in kr_name.split() or word_lower in en_name.split():
                        score += 40

            # 일정 점수 이상인 결과만 포함
            if score > 0:
                scored_results.append((row, score))

        # 점수 기준으로 정렬하고 상위 결과만 유지
        scored_results.sort(key=lambda x: x[1], reverse=True)
        search_result = [item[0] for item in scored_results]

        logger.warning(f"Scored and filtered results count: {len(search_result)}")

        if scored_results:
            top_results = [(r[0]._mapping["ticker"], r[0]._mapping["kr_name"], r[1]) for r in scored_results[:5]]
            logger.warning(f"Top scoring results: {top_results}")

        search_result = search_result[offset : offset + limit]

        logger.warning(f"Final result count after offset/limit: {len(search_result)}")

        search_items = []
        for row in search_result:
            mapping = row._mapping
            ticker = mapping["ticker"]
            search_items.append(
                InterestSearchItem(
                    ticker=ticker,
                    name=mapping["kr_name"] if ctry == TranslateCountry.KO else mapping["en_name"],
                    ctry=mapping["ctry"],
                    is_interest=ticker in interest_tickers,
                )
            )

        return search_items


def get_search_service() -> SearchService:
    return SearchService()
