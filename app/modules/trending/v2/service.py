from typing import List

from app.database.conn import db
from app.database.crud import JoinInfo, database
from app.modules.common.enum import TranslateCountry
from app.modules.trending.schemas import TrendingStock, TrendingStockRequest, TrendingType
from app.elasticsearch.elasticsearch import get_elasticsearch_client
from app.elasticsearch.elasticsearch_service import create_stock_price_query

class TrendingService:
    def __init__(self):
        self.database = database
        self.db = db
        self.es_client = None

    async def _init_elasticsearch(self):
        """엘라스틱서치 클라이언트 초기화"""
        if self.es_client is None:
            self.es_client = await get_elasticsearch_client()

    async def _get_trending_type(self, request: TrendingStockRequest) -> str:
        match request.type:
            case TrendingType.UP | TrendingType.DOWN:
                return f"change_{request.period.value}"
            case TrendingType.VOL:
                return f"volume_{request.period.value}"
            case TrendingType.AMT:
                return f"volume_change_{request.period.value}"

    async def get_trending_stocks(self, request: TrendingStockRequest, lang: TranslateCountry | None = None) -> List[TrendingStock]:
        if lang is None:
            lang = TranslateCountry.KO        

        order = await self._get_trending_type(request)
        sort_order = "asc" if request.type == TrendingType.DOWN else "desc"

        if lang == TranslateCountry.KO:
            name = "kr_name"
        elif lang == TranslateCountry.EN:
            name = "en_name"
        else:
            name = "kr_name"
        
        activate_tickers_data = await self.database._select_async(
            table="stock_information",
            columns=["ticker"],
            ctry=request.ctry.value,
            is_activate=1,
        )

        activate_tickers = [row[0] for row in activate_tickers_data]

        await self._init_elasticsearch()

        trending_stock_query_builder = (create_stock_price_query(activate_tickers)
                                        .term("ctry", request.ctry.value)
                                        .sort(order, sort_order)
                                        .size(100)
                                        .build())

        trending_stock_response = await self.es_client.client.search(
            index="quantus-stock-trend-*",
            body=trending_stock_query_builder
        )

        trending_stock_data = []
        for idx, hit in enumerate(trending_stock_response["hits"]["hits"], 1):
            source = hit["_source"]
            trending_stock = TrendingStock(
                num=idx,
                ticker=source["ticker"],
                name="Temp_name" if source[name] is None else f"{source[name]} ({source['ticker']})",
                current_price=0.0 if source["current_price"] is None else float(source["current_price"]),
                current_price_rate=0.0 if source[f"change_{request.period.value}"] is None else float(source[f"change_{request.period.value}"]),
                volume=0.0 if source[f"volume_{request.period.value}"] is None else float(source[f"volume_{request.period.value}"]),
                amount=0.0 if source[f"volume_change_{request.period.value}"] is None else float(source[f"volume_change_{request.period.value}"])
            )
            
            trending_stock_data.append(trending_stock)

        return trending_stock_data
def get_trending_service():
    return TrendingService()