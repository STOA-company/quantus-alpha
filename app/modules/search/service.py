from typing import List, Optional, Tuple, Dict
from sqlalchemy import select
from sqlalchemy.orm import Session
from app.database.crud import database
from app.models.models_stock import StockInformation
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
            limit=limit,
            offset=offset,
            or__=[
                {"ticker": query},
                {"ticker__like": search_term},
                {"kr_name__like": search_term},
                {"en_name__like": search_term}
            ],
            can_use=1
        )

        if not search_result:
            return []

        tickers = [row._mapping["ticker"] for row in search_result]
        country_codes = {
            row._mapping["ticker"]: row._mapping["ctry"].lower() 
            for row in search_result
        }

        current_prices = self._get_current_prices(tickers, country_codes)

        search_items = []
        for row in search_result:
            ticker = row._mapping["ticker"]
            name = row._mapping["kr_name"] if ctry == TranslateCountry.KO else row._mapping["en_name"]
            current_price = current_prices.get(ticker, (None, None))
            
            search_items.append(
                SearchItem(
                    ticker=ticker,
                    name=name,
                    language=ctry,
                    current_price=current_price[0],
                    current_price_rate=current_price[1],
                )
            )

        return search_items
    
    def _get_current_prices(self, tickers: List[str], country_codes: Dict[str, str]) -> Dict[str, Tuple[float, float]]:
        result = {}
        
        country_groups = {}
        for ticker in tickers:
            country = country_codes.get(ticker)
            if country:
                country_groups.setdefault(country.lower(), []).append(ticker)
        
        for country, country_tickers in country_groups.items():
            table_name = f"stock_{country}_1d"
            try:
                prices = self.db._select(
                    table=table_name,
                    columns=["Ticker", "Close", "Open"],
                    Ticker__in=country_tickers,
                    order="Date",
                    ascending=False
                )
                
                latest_prices = {}
                for row in prices:
                    ticker = row._mapping["Ticker"]
                    if ticker not in latest_prices:
                        try:
                            close = float(row._mapping["Close"])
                            open_price = float(row._mapping["Open"])
                            
                            rate = 0 if open_price == 0 else round(((close - open_price) / open_price) * 100, 2)
                            latest_prices[ticker] = (close, rate)
                        except (KeyError, ValueError, AttributeError, TypeError):
                            latest_prices[ticker] = (None, None)
                
                result.update(latest_prices)
            except Exception:
                result.update({ticker: (None, None) for ticker in country_tickers})

        return result

def get_search_service() -> SearchService:
    return SearchService()
