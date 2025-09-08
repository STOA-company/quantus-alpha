from typing import List

from app.database.conn import db
from app.database.crud import JoinInfo, database
from app.modules.common.enum import TranslateCountry
from app.modules.trending.schemas import TrendingStock, TrendingStockRequest, TrendingType


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

    async def get_trending_stocks(
        self, request: TrendingStockRequest, lang: TranslateCountry | None = None
    ) -> List[TrendingStock]:
        if lang is None:
            lang = TranslateCountry.KO

        order = self._get_trending_type(request)
        ascending = True if request.type == TrendingType.DOWN else False

        activate_tickers_data = await self.database._select_async(
            table="stock_information",
            columns=["ticker"],
            ctry=request.ctry.value,
            is_activate=1,
        )

        activate_tickers = [row[0] for row in activate_tickers_data]

        if lang == TranslateCountry.KO:
            name = "kr_name"
        elif lang == TranslateCountry.EN:
            name = "en_name"
        else:
            name = "kr_name"  # noqa

        trending_stocks = await self.database._select_async(
            table="stock_trend",
            columns=[
                "ticker",
                name,
                "current_price",
                "last_updated",
                f"change_{request.period.value}",
                f"volume_{request.period.value}",
                f"volume_change_{request.period.value}",
            ],
            order=order,
            ascending=ascending,
            ctry=request.ctry.value,
            limit=100,
            ticker__in=activate_tickers,
            join_info=JoinInfo(
                primary_table="stock_trend",
                secondary_table="stock_information",
                primary_column="ticker",
                secondary_column="ticker",
                columns=["is_trading_stopped", "is_delisted"],
                secondary_condition={"is_trading_stopped": 0, "is_delisted": 0},
            ),
        )

        return [
            TrendingStock(
                num=idx,
                ticker=stock._mapping["ticker"],
                name="Temp_name"
                if stock._mapping[name] is None
                else f"{stock._mapping[name]} ({stock._mapping['ticker']})",
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


def get_trending_service():
    return TrendingService()
