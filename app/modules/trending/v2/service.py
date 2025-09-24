from typing import List
import time
import logging

from app.database.conn import db
from app.database.crud import JoinInfo, database
from app.modules.common.enum import TranslateCountry
from app.modules.trending.schemas import TrendingStock, TrendingStockRequest, TrendingType
from app.elasticsearch.elasticsearch import get_elasticsearch_client
from app.elasticsearch.elasticsearch_service import create_stock_price_query

logger = logging.getLogger(__name__)

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
        start_time = time.time()
        
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
        
        # 1. 데이터베이스에서 활성 티커 조회
        db_start = time.time()
        activate_tickers_data = await self.database._select_async(
            table="stock_information",
            columns=["ticker"],
            ctry=request.ctry.value,
            is_activate=1,
        )
        db_time = time.time() - db_start
        logger.info(f"[trending] Database query completed in {db_time:.3f}s, found {len(activate_tickers_data)} active tickers")

        activate_tickers = [row[0] for row in activate_tickers_data]

        # 2. Elasticsearch 초기화
        es_init_start = time.time()
        await self._init_elasticsearch()
        es_init_time = time.time() - es_init_start
        logger.info(f"[trending] Elasticsearch init completed in {es_init_time:.3f}s")

        # 3. Elasticsearch 쿼리 실행
        es_query_start = time.time()
        trending_stock_query_builder = (create_stock_price_query(activate_tickers)
                                        .term("ctry", request.ctry.value)
                                        .sort(order, sort_order)
                                        .size(100)
                                        .build())

        trending_stock_response = await self.es_client.client.search(
            index="quantus-stock-trend-*",
            body=trending_stock_query_builder
        )
        es_query_time = time.time() - es_query_start
        logger.info(f"[trending] Elasticsearch query completed in {es_query_time:.3f}s, found {len(trending_stock_response['hits']['hits'])} results")

        # 4. 데이터 변환
        transform_start = time.time()
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
        transform_time = time.time() - transform_start
        logger.info(f"[trending] Data transformation completed in {transform_time:.3f}s, processed {len(trending_stock_data)} items")

        total_time = time.time() - start_time
        logger.info(f"[trending] Total execution completed in {total_time:.3f}s, returning {len(trending_stock_data)} trending stocks")

        return trending_stock_data
def get_trending_service():
    return TrendingService()