from typing import List
from app.database.crud import database
from app.database.conn import db
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
        ascending = True if request.type == TrendingType.DOWN else False

        trending_stocks = self.database._select(
            table="stock_trend",
            columns=[
                "ticker",
                "ko_name",
                "current_price",
                "last_updated",
                f"change_{request.period.value}",
                f"volume_{request.period.value}",
                f"volume_change_{request.period.value}",
            ],
            order=order,
            ascending=ascending,
            limit=100,
        )

        # 가장 최신 last_updated 찾기
        latest_date = max(
            (stock._mapping["last_updated"].date() for stock in trending_stocks if stock._mapping["last_updated"]),
            default=None,
        )

        # 최신 날짜의 데이터만 필터링
        filtered_stocks = [
            stock
            for stock in trending_stocks
            if stock._mapping["last_updated"] and stock._mapping["last_updated"].date() == latest_date
        ]

        return [
            TrendingStock(
                num=idx,
                ticker=stock._mapping["ticker"],
                name="Temp_name" if stock._mapping["ko_name"] is None else stock._mapping["ko_name"],
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
            for idx, stock in enumerate(filtered_stocks, 1)
        ]


def new_get_trending_service():
    return NewTrendingService()
