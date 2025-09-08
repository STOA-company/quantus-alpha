from typing import List, Literal

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from app.cache.leaderboard import DisclosureLeaderboard, NewsLeaderboard
from app.core.exception.base import DuplicateException, NotFoundException
from app.database.crud import database, database_service
from app.modules.news.schemas import DisclosureRenewalItem, NewsRenewalItem
from app.modules.news.services import get_news_service


class InterestService:
    def __init__(self):
        self.db = database_service
        self.data_db = database

    def get_interest(self, group_id: int, lang: Literal["ko", "en"] = "ko", offset: int = 0, limit: int = 50):
        interests = self.db._select(table="user_stock_interest", group_id=group_id, order="created_at", ascending=True)
        if not interests:
            return {"has_next": False, "data": []}
        tickers = [interest.ticker for interest in interests]
        name_column = "kr_name" if lang == "ko" else "en_name"
        table = self.data_db._select(
            table="stock_trend",
            columns=["ctry", "ticker", name_column, "current_price", "change_rt", "volume_change_rt", "volume_rt"],
            ticker__in=tickers,
        )

        sorted_table = []
        for interest in interests:
            for row in table:
                if row.ticker == interest.ticker:
                    sorted_table.append(row)
                    break

        has_next = len(sorted_table) > offset * limit + limit
        table = {
            "has_next": has_next,
            "data": [
                {
                    "country": row.ctry,
                    "ticker": row.ticker,
                    "name": row.kr_name if lang == "ko" else row.en_name,
                    "price": {
                        "value": self.get_won_unit(row.current_price, lang)[0]
                        if row.ctry == "kr"
                        else round(self.get_dollar_unit(row.current_price)[0], 2),
                        "unit": self.get_won_unit(row.current_price, lang)[1]
                        if row.ctry == "kr"
                        else self.get_dollar_unit(row.current_price)[1],
                    },
                    "change": {
                        "value": round(row.change_rt, 2),
                        "unit": "%",
                        "sign": "plus" if row.change_rt > 0 else "minus",
                    },
                    "amount": {
                        "value": self.get_won_unit(row.volume_change_rt, lang)[0]
                        if row.ctry == "kr"
                        else round(self.get_dollar_unit(row.volume_change_rt)[0], 2),
                        "unit": self.get_won_unit(row.volume_change_rt, lang)[1]
                        if row.ctry == "kr"
                        else self.get_dollar_unit(row.volume_change_rt)[1],
                    },
                    "volume": {
                        "value": row.volume_rt,
                        "unit": "주" if lang == "ko" else "shs",
                    },
                }
                for row in sorted_table
            ],
        }
        table["data"] = table["data"][offset * limit : offset * limit + limit]
        return table

    async def get_interest_tickers(self, group_id: int, lang: Literal["ko", "en"] = "ko"):
        interests = await self.db._select_async(table="user_stock_interest", columns=["ticker"], group_id=group_id)
        stocks = await self.data_db._select_async(
            table="stock_trend",
            columns=["ticker", "kr_name", "en_name", "ctry"],
            ticker__in=[interest[0] for interest in interests],
        )
        if not stocks:
            return []
        return [
            {"ticker": stock.ticker, "name": stock.kr_name if lang == "ko" else stock.en_name, "country": stock.ctry}
            for stock in stocks
        ]

    def add_interest(self, group_id: int, ticker: str):
        stock = self.db._select(table="user_stock_interest", group_id=group_id, ticker=ticker, limit=1)
        if stock:
            raise HTTPException(status_code=400, detail="이미 관심 종목에 추가되어 있습니다.")
        self.db._insert(table="user_stock_interest", sets={"group_id": group_id, "ticker": ticker})

        confirm = self.db._select(table="user_stock_interest", group_id=group_id, ticker=ticker, limit=1)
        if not confirm:
            return False

        return True

    def delete_interest(self, group_id: int, tickers: List[str]):
        for ticker in tickers:
            stock = self.db._select(table="user_stock_interest", group_id=group_id, ticker=ticker, limit=1)
            if not stock:
                raise HTTPException(status_code=404, detail="관심 종목에 추가되지 않은 종목입니다.")
            self.db._delete(table="user_stock_interest", group_id=group_id, ticker=ticker)

        return True

    def update_interest(self, user_id: int, group_ids: List[int], ticker: str):
        groups = self.db._select(table="interest_group", user_id=user_id)
        if not groups:
            raise HTTPException(status_code=404, detail="관심 그룹이 존재하지 않습니다.")

        self.db._delete(table="user_stock_interest", ticker=ticker, group_id__not_in=group_ids)
        for group_id in group_ids:
            group = self.db._select(table="interest_group", id=group_id, user_id=user_id)
            if not group:
                raise HTTPException(status_code=404, detail=f"그룹 {group_id}이 존재하지 않습니다.")
            self.db._insert(table="user_stock_interest", sets={"group_id": group_id, "ticker": ticker})

        return True

    def get_interest_group(self, user_id: int):
        groups = self.db._select(table="interest_group", user_id=user_id, order="created_at", ascending=True)
        if not groups:
            return []
        return [{"id": group.id, "name": group.name} for group in groups]

    def create_interest_group(self, user_id: int, name: str):
        try:
            group = self.db._select(table="interest_group", user_id=user_id, name=name, limit=1)
            if group:
                raise DuplicateException(message="이미 존재하는 관심 그룹입니다.")
            self.db._insert(table="interest_group", sets={"user_id": user_id, "name": name})
            return True
        except IntegrityError:
            raise HTTPException(status_code=409, detail="이미 사용 중인 그룹 이름입니다.")
        except DuplicateException as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def delete_interest_group(self, group_id: int):
        try:
            self.db._delete(table="interest_group", id=group_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return True

    def update_interest_group_name(self, group_id: int, name: str):
        try:
            group = self.db._select(table="interest_group", id=group_id, limit=1)
            if not group:
                raise NotFoundException(message="관심 그룹이 존재하지 않습니다.")
            if name == group[0].name:
                raise DuplicateException(message="기존 이름과 동일합니다.")
            self.db._update(table="interest_group", id=group_id, sets={"name": name})
            return True

        except NotFoundException as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
        except DuplicateException as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
        except IntegrityError:
            raise HTTPException(status_code=409, detail="이미 사용 중인 그룹 이름입니다.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def get_won_unit(self, number, lang):
        if isinstance(number, str):
            number = int(number.replace(",", ""))

        if lang == "ko":
            if number < 100000000:  # 1억 미만
                return (number, "원")
            elif number < 1000000000000:  # 1조 미만
                return (round(float(number / 100000000), 0), "억원")
            elif number < 10000000000000000:  # 1경 미만
                return (round(float(number / 1000000000000), 2), "조원")
            else:
                return (round(float(number / 10000000000000000), 0), "경원")
        else:
            if number < 1000000:  # 1K 미만
                return (number, "₩")
            elif number < 1000000000000:  # 1T 미만
                return (float(number / 1000000), "B₩")
            else:
                return (float(number / 1000000000000), "T₩")

    def get_dollar_unit(self, number):
        if isinstance(number, str):
            number = float(number.replace(",", ""))

        if number < 1000:  # 1K 미만
            return (number, "$")
        elif number < 1000000:  # 1M 미만
            return (float(number / 1000), "K$")
        elif number < 1000000000:  # 1B 미만
            return (float(number / 1000000), "M$")
        elif number < 1000000000000:  # 1T 미만
            return (float(number / 1000000000), "B$")
        else:
            return (float(number / 1000000000000), "T$")

    def get_interest_count(self, group_id: int):
        count = self.db._select(table="user_stock_interest", group_id=group_id)
        return len(count)

    def get_interest_news_leaderboard(
        self,
        group_id: int,
        lang: Literal["ko", "en"] = "ko",
        subscription_level: int = 1,
    ) -> List[NewsRenewalItem]:
        redis = NewsLeaderboard()
        news_service = get_news_service()
        ticker_infos = self.get_interest_tickers(group_id)
        if len(ticker_infos) == 0:
            return []
        tickers = [ticker_info["ticker"] for ticker_info in ticker_infos]
        leaderboard_data = redis.get_leaderboard(tickers=tickers)[:5]
        print(f"leaderboard_data: {leaderboard_data}")
        news_ids = [item.get("news_id") for item in leaderboard_data]
        print(f"news_ids: {news_ids}")
        news_items = news_service.get_news_by_id(news_ids, lang)
        if news_items is None:
            return []
        print(f"news_items: {news_items}")
        news_tickers = [item.ticker for item in news_items]
        print(f"news_tickers: {news_tickers}")
        # 구독 레벨이 3 미만인 경우에만 마스킹 적용
        if subscription_level < 3 and news_items:
            # 각 티커별 최신 10개 뉴스 ID 조회
            recent_news_ids = news_service.get_recent_news_ids_by_ticker(news_tickers, limit=10, lang=lang)

            # 티커별 ID를 이용한 최적화된 마스킹 적용
            news_items = news_service.mask_news_items_by_id(news_items, recent_news_ids)
        print(f"news_items: {news_items}")
        return news_items

    def get_interest_disclosure_leaderboard(
        self,
        group_id: int,
        lang: Literal["ko", "en"] = "ko",
        subscription_level: int = 1,
    ) -> List[DisclosureRenewalItem]:
        redis = DisclosureLeaderboard()
        news_service = get_news_service()
        ticker_infos = self.get_interest_tickers(group_id)
        if len(ticker_infos) == 0:
            return []
        tickers = [ticker_info["ticker"] for ticker_info in ticker_infos]
        leaderboard_data = redis.get_leaderboard(tickers=tickers)[:5]
        disclosure_ids = [item.get("disclosure_id") for item in leaderboard_data]
        disclosure_items = news_service.get_disclosure_by_id(disclosure_ids, lang)

        # 구독 레벨이 3 미만인 경우에만 마스킹 적용
        if subscription_level < 3 and disclosure_items:
            # 날짜 기반 마스킹 적용 (7일 이전 데이터 마스킹)
            return news_service.mask_disclosure_items_by_date(disclosure_items)

        return disclosure_items

    def get_interest_info(self, user_id: int, ticker: str):
        query = self.db._select(table="interest_group", user_id=user_id)
        if not query:
            return {"is_interested": False, "groups": []}

        total_groups = [
            {
                "id": group.id,
                "name": group.name,
                "included": True
                if self.db._select(table="user_stock_interest", group_id=group.id, ticker=ticker)
                else False,
            }
            for group in query
        ]

        for group in total_groups:
            if group["included"] is True:
                return {"is_interested": True, "groups": total_groups}

        return {"is_interested": False, "groups": total_groups}


def get_interest_service() -> InterestService:
    return InterestService()
