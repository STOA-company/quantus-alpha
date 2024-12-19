from typing import List
from sqlalchemy import select, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.models_stock import StockInformation
from app.modules.common.enum import TranslateCountry
from app.modules.search.schemas import SearchItem


class SearchService:
    async def search(self, query: str, ctry: str, offset: int, limit: int, db: AsyncSession) -> List[SearchItem]:
        """
        입력받는 query에 따른 종목 검색 기능
        - 무한 스크롤을 위한 페이징 지원
        - 대소문자 구분 없는 검색
        - 부분 문자열 검색 지원

        Args:
            query (str): 검색어
            db (AsyncSession): 데이터베이스 세션
            offset (int): 시작 위치 (기본값: 0)
            limit (int): 반환할 항목 수 (기본값: 20)

        Returns:
            List[SearchItem]: 검색 결과 리스트
        """

        # 검색어 전처리
        search_term = f"%{query}%"

        # 모든 종목에 대해 검색
        search_query = select(StockInformation).where(
            or_(
                func.lower(StockInformation.kr_name).like(func.lower(search_term)),
                func.lower(StockInformation.en_name).like(func.lower(search_term)),
            )
        )

        # 페이징 적용
        search_query = search_query.offset(offset).limit(limit)

        # 쿼리 실행
        result = await db.execute(search_query)
        search_result = result.scalars().all()

        search_items = []
        for item in search_result:
            if ctry == TranslateCountry.KO:
                name = item.kr_name
            elif ctry == TranslateCountry.EN:
                name = item.en_name

            search_items.append(
                SearchItem(
                    ticker=item.ticker,
                    name=name,
                    language=ctry,
                    current_price=None,
                    current_price_rate=None,
                )
            )
        print(f"출력 결과 : {ctry}")
        return search_items


def get_search_service() -> SearchService:
    return SearchService()
