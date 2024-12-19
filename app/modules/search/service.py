from typing import List
from sqlalchemy import select, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.models_stock import StockInformation
from app.modules.common.enum import TranslateCountry
from app.modules.search.schemas import SearchItem


class SearchService:
    async def search(
        self, query: str, ctry: TranslateCountry, offset: int, limit: int, db: AsyncSession
    ) -> List[SearchItem]:
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
        # 검색어 전처리
        search_term = f"%{query}%"

        # 전체 결과 개수 먼저 조회 (디버깅용)
        count_query = (
            select(func.count())
            .select_from(StockInformation)
            .where(
                or_(
                    func.lower(StockInformation.kr_name).like(func.lower(search_term)),
                    func.lower(StockInformation.en_name).like(func.lower(search_term)),
                )
            )
        )
        total_count = await db.scalar(count_query)
        print(f"Total matching records: {total_count}")

        # 검색 쿼리
        search_query = (
            select(StockInformation)
            .where(
                or_(
                    func.lower(StockInformation.kr_name).like(func.lower(search_term)),
                    func.lower(StockInformation.en_name).like(func.lower(search_term)),
                )
            )
            .offset(offset)
            .limit(limit)
        )

        result = await db.execute(search_query)
        search_result = result.scalars().all()
        print(f"Retrieved records: {len(search_result)}, Requested limit: {limit}")

        search_items = []
        for item in search_result:
            name = item.kr_name if ctry == TranslateCountry.KO else item.en_name
            search_items.append(
                SearchItem(
                    ticker=item.ticker,
                    name=name,
                    language=ctry,
                    current_price=None,
                    current_price_rate=None,
                )
            )

        return search_items


def get_search_service() -> SearchService:
    return SearchService()
