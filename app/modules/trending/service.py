from typing import List

from app.database.crud import database
from app.database.conn import db
from app.models.models_stock import StockTrend
from app.modules.common.enum import MarketType, TrendingPeriod, TrendingType
from app.modules.trending.crud import get_trending_stocks
from app.modules.trending.schemas import (
    TrendingStockResponse,
    TrendingStock,
)


class TrendingService:
    def __init__(self):
        self.database = database
        self.db = db

    def get_tranding_stocks_base(self) -> TrendingStock:
        return TrendingStock

    def get_trending_stocks(self) -> TrendingStockResponse:
        for market in MarketType:
            for type in TrendingType:
                for period in TrendingPeriod:
                    List[StockTrend] = get_trending_stocks(self.db, market, type, period)
        return TrendingStockResponse()


def get_trending_service():
    return TrendingService()
