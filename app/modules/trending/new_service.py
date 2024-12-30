from typing import List
from app.database.crud import database
from app.database.conn import db
from app.modules.common.enum import TrendingPeriod
from app.modules.trending.new_schemas import TrendingStockRequest, TrendingStock, TrendingType


class NewTrendingService:
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
        ascending = False if request.type == TrendingType.DOWN else True

        # TODO : 실시간 데이터 반영 후 제거
        if request.period == TrendingPeriod.REALTIME:
            request.period = TrendingPeriod.DAY

        trending_stocks = self.database._select(
            table="stock_trend",
            columns=[
                "ticker",
                "en_name",
                "current_price",
                f"change_{request.period.value}",
                f"volume_{request.period.value}",
                f"volume_change_{request.period.value}",
            ],
            order=order,
            ascending=ascending,
            limit=100,
        )

        return [
            TrendingStock(
                num=idx,
                ticker=stock._mapping["ticker"],
                name="Temp_name" if stock._mapping["en_name"] is None else stock._mapping["en_name"],
                current_price=0.0 if stock._mapping["current_price"] is None else stock._mapping["current_price"],
                current_price_rate=0.0
                if stock._mapping[f"change_{request.period.value}"] is None
                else stock._mapping[f"change_{request.period.value}"],
                volume=0.0
                if stock._mapping[f"volume_{request.period.value}"] is None
                else stock._mapping[f"volume_{request.period.value}"],
                amount=0.0
                if stock._mapping[f"volume_change_{request.period.value}"] is None
                else stock._mapping[f"volume_change_{request.period.value}"],
            )
            for idx, stock in enumerate(trending_stocks, 1)
        ]


def new_get_trending_service():
    return NewTrendingService()
