from app.database.crud import database_service, database
from fastapi import HTTPException
from typing import List, Literal


class InterestService:
    def __init__(self):
        self.db = database_service
        self.data_db = database

    def get_interest(self, group_id: int, lang: Literal["kr", "en"] = "kr", offset: int = 0, limit: int = 50):
        interests = self.db._select(table="user_stock_interest", group_id=group_id)
        if not interests:
            return []
        tickers = [interest.ticker for interest in interests]
        name_column = "kr_name" if lang == "kr" else "en_name"
        table = self.data_db._select(
            table="stock_trend",
            columns=["ctry", "ticker", name_column, "current_price", "change_rt", "volume_change_rt", "volume_rt"],
            ticker__in=tickers,
        )
        table = [
            {
                "ctry": row.ctry,
                "ticker": row.ticker,
                "name": row.kr_name if lang == "kr" else row.en_name,
                "price": {
                    "value": self.get_won_unit(row.current_price, lang)[0]
                    if row.ctry == "kr"
                    else self.get_dollar_unit(row.current_price)[0],
                    "unit": self.get_won_unit(row.current_price, lang)[1]
                    if row.ctry == "kr"
                    else self.get_dollar_unit(row.current_price)[1],
                },
                "change": {
                    "value": row.change_rt,
                    "unit": "%",
                    "sign": "plus" if row.change_rt > 0 else "minus",
                },
                "amount": {
                    "value": self.get_won_unit(row.volume_change_rt, lang)[0]
                    if row.ctry == "kr"
                    else self.get_dollar_unit(row.volume_change_rt)[0],
                    "unit": self.get_won_unit(row.volume_change_rt, lang)[1]
                    if row.ctry == "kr"
                    else self.get_dollar_unit(row.volume_change_rt)[1],
                },
                "volume": {
                    "value": row.volume_rt,
                    "unit": "주" if lang == "kr" else "shs",
                },
            }
            for row in table
        ]
        return table[offset : offset + limit]

    def get_interest_tickers(self, group_id: int):
        interests = self.db._select(table="user_stock_interest", group_id=group_id)
        if not interests:
            return []
        return [interest.ticker for interest in interests]

    def add_interest(self, group_id: int, ticker: str):
        stock = self.db._select(table="user_stock_interest", group_id=group_id, ticker=ticker, limit=1)
        if stock:
            raise HTTPException(status_code=400, detail="이미 관심 종목에 추가되어 있습니다.")
        return self.db._insert(table="user_stock_interest", sets={"group_id": group_id, "ticker": ticker})

    def delete_interest(self, group_id: int, tickers: List[str]):
        for ticker in tickers:
            stock = self.db._select(table="user_stock_interest", group_id=group_id, ticker=ticker, limit=1)
            if not stock:
                raise HTTPException(status_code=404, detail="관심 종목에 추가되지 않은 종목입니다.")
            self.db._delete(table="user_stock_interest", group_id=group_id, ticker=ticker)

    def get_interest_group(self, user_id: int):
        groups = self.db._select(table="interest_group", user_id=user_id)
        return [{"id": group.id, "name": group.name} for group in groups]

    def create_interest_group(self, user_id: int, name: str):
        group = self.db._select(table="interest_group", user_id=user_id, name=name, limit=1)
        if group:
            raise HTTPException(status_code=400, detail="이미 존재하는 관심 그룹입니다.")
        return self.db._insert(table="interest_group", sets={"user_id": user_id, "name": name})

    def delete_interest_group(self, group_id: int):
        return self.db._delete(table="interest_group", group_id=group_id)

    def get_won_unit(self, number, lang):
        if isinstance(number, str):
            number = int(number.replace(",", ""))

        if lang == "kr":
            if number < 100000000:  # 1억 미만
                return (number, "원")
            elif number < 1000000000000:  # 1조 미만
                return (float(number / 100000000), "억원")
            elif number < 10000000000000000:  # 1경 미만
                return (float(number / 1000000000000), "조원")
            else:
                return (float(number / 10000000000000000), "경원")
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

    from typing import List, Dict, Any, Union, Tuple


def get_interest_service() -> InterestService:
    return InterestService()
