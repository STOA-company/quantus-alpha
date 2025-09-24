import asyncio
from typing import Dict, List, Tuple, Optional
import os
from functools import lru_cache
import json

from fastapi import HTTPException

from app.core.logger import setup_logger
from app.database.crud import database, database_service
from app.elasticsearch.elasticsearch import get_elasticsearch_client
from app.elasticsearch.elasticsearch_service import create_trending_tickers_query

logger = setup_logger(__name__)


class StockInfoService:
    def __init__(self):
        self.db = database_service
        self.data_db = database
        self.es_client = None

    async def _init_elasticsearch(self):
        """엘라스틱서치 클라이언트 초기화"""
        if self.es_client is None:
            self.es_client = await get_elasticsearch_client()

    async def get_trending_stock_ticker(self) -> List[str]:
        await self._init_elasticsearch()

        trending_stock_query_builder = create_trending_tickers_query()

        trending_stock_response = await self.es_client.client.search(
            index="quantus-news-analysis-*",
            body=trending_stock_query_builder.build()
        )

        # ticker 리스트 추출
        tickers = []

        # US 티커 추가
        us_buckets = trending_stock_response["aggregations"]["us_tickers"]["top_us"]["buckets"]
        for bucket in us_buckets:
            tickers.append(bucket["key"])

        # KR 티커 추가
        kr_buckets = trending_stock_response["aggregations"]["kr_tickers"]["top_kr"]["buckets"]
        for bucket in kr_buckets:
            tickers.append(bucket["key"])

        return tickers


def get_stock_info_service() -> StockInfoService:
    return StockInfoService()