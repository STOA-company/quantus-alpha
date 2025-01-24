from typing import List
from app.database.crud import database
from app.modules.common.enum import TranslateCountry
from app.modules.search.schemas import SearchItem


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
            can_use=1,
        )

        if not search_result:
            return []

        sorted_result = sorted(search_result, key=lambda x: 1 if x._mapping["ticker"] == query else 2)

        search_result = sorted_result[offset : offset + limit]

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

        prices = {}
        for country, tickers in country_groups.items():
            change_rate_column = "change_rt" if country == "us" else "change_1d"
            try:
                price_results = self.db._select(
                    table="stock_trend",
                    columns=["ticker", "current_price", "prev_close", change_rate_column],
                    ticker__in=tickers,
                    order="last_updated",
                    ascending=False,
                    limit=len(tickers),
                )

                for row in price_results:
                    mapping = row._mapping
                    ticker = mapping["ticker"]
                    if ticker not in prices:
                        try:
                            current_price = float(mapping["current_price"])
                            rate = float(mapping[change_rate_column])
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

            search_items.append(
                SearchItem(
                    ticker=ticker,
                    name=item_info["name"],
                    language=item_info["language"],
                    current_price=current_price,
                    current_price_rate=rate,
                )
            )

        return search_items


def get_search_service() -> SearchService:
    return SearchService()
