from typing import List
from app.database.crud import database
from app.database.conn import db
from app.modules.trending.schemas import TrendingStockRequest, TrendingStock, TrendingType


class TrendingService:
    def __init__(self):
        self.database = database
        self.db = db

    def _get_trending_type(self, request: TrendingStockRequest) -> str:
        match request.type:
            case TrendingType.UP | TrendingType.DOWN:
                return f"change_{request.period.value}"
            case TrendingType.VOL:
                return f"volume_{request.period.value}"
            case TrendingType.AMT:
                return f"volume_change_{request.period.value}"

    def get_trending_stocks(self, request: TrendingStockRequest) -> List[TrendingStock]:
        order = self._get_trending_type(request)
        ascending = True if request.type == TrendingType.DOWN else False

        latest_date_query = self.database._select(
            table="stock_trend",
            columns=["last_updated"],
            order="last_updated",
            ascending=False,
            ctry=request.ctry.value,
            limit=1,
        )

        latest_date = latest_date_query[0]._mapping["last_updated"] if latest_date_query else None

        trending_stocks = self.database._select(
            table="stock_trend",
            columns=[
                "ticker",
                "kr_name",
                "current_price",
                "last_updated",
                f"change_{request.period.value}",
                f"volume_{request.period.value}",
                f"volume_change_{request.period.value}",
            ],
            order=order,
            ascending=ascending,
            ctry=request.ctry.value,
            last_updated=latest_date,
            limit=100,
        )

        return [
            TrendingStock(
                num=idx,
                ticker=stock._mapping["ticker"],
                name=f"{stock._mapping['kr_name'].replace('(ADR)', '')} ({stock._mapping['ticker']})",
                current_price=stock._mapping["current_price"],
                current_price_rate=stock._mapping[f"change_{request.period.value}"],
                volume=stock._mapping[f"volume_{request.period.value}"],
                amount=stock._mapping[f"volume_change_{request.period.value}"],
            )
            for idx, stock in enumerate(trending_stocks, 1)
        ]


def get_trending_service():
    return TrendingService()
