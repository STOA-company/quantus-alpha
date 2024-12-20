from typing import List, Optional, Tuple
from sqlalchemy import select, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.crud import database
from app.models.models_stock import StockInformation
from app.modules.common.enum import TranslateCountry
from app.modules.search.schemas import SearchItem


class SearchService:
    def __init__(self):
        self.db = database

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
                    func.lower(StockInformation.ticker).like(func.lower(search_term)),
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
                    func.lower(StockInformation.ticker).like(func.lower(search_term)),
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

            # db 세션 전달
            current_price, price_rate = await self._get_current_price(item.ticker, db)
            print(f"Current price: {current_price}, Price rate: {price_rate}")

            search_items.append(
                SearchItem(
                    ticker=item.ticker,
                    name=name,
                    language=ctry,
                    current_price=current_price,
                    current_price_rate=price_rate,
                )
            )

        return search_items

    async def _get_current_price(self, ticker: str, db: AsyncSession) -> Tuple[Optional[float], Optional[float]]:
        """
        현재 주가 조회, 등락률 계산
        등락률 계산 방법: (Close - Open) / Open * 100
        """
        try:
            country_code = await self.get_country_code(ticker, db)
            country_code = country_code.lower()
            table_name = f"stock_{country_code}_1d"

            result = self.db._select(
                table=table_name, columns=["Close", "Open"], order="Date", ascending=False, limit=1, Ticker=ticker
            )

            if not result or len(result) == 0:
                print(f"No data found for {ticker} in {table_name}")
                return None, None

            row = result[0]
            print(f"Raw data for {ticker}: {row}")

            try:
                close = float(row._mapping["Close"])
                open_price = float(row._mapping["Open"])

                if open_price == 0:
                    rate = 0
                else:
                    rate = round(((close - open_price) / open_price) * 100, 2)

                print(f"{ticker} ({country_code}): close={close}, open={open_price}, rate={rate:.2f}%")
                return close, rate

            except (KeyError, ValueError, AttributeError) as e:
                print(f"Error processing data for {ticker}: {e}")
                print(f"Row structure: {dir(row)}")
                return None, None

        except Exception as e:
            print(f"Database error for {ticker}: {e}")
            return None, None

    async def get_country_code(self, ticker: str, db: AsyncSession) -> str:
        """
        ticker로 국가 코드 조회
        """
        result = await db.execute(select(StockInformation.ctry).where(StockInformation.ticker == ticker))
        return result.scalar_one()


def get_search_service() -> SearchService:
    return SearchService()
