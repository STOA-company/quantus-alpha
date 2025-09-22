from elasticsearch import AsyncElasticsearch
from typing import Dict, List, Any, Optional
import logging
from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class ElasticsearchClient:
    def __init__(self):
        self.client = AsyncElasticsearch(
            hosts=[settings.ELASTICSEARCH_URL],
            # basic_auth=(settings.ELASTICSEARCH_USER, settings.ELASTICSEARCH_PASSWORD) if hasattr(settings, 'ELASTICSEARCH_USER') else None,
            timeout=30,  # 30초 타임아웃
            max_retries=3,  # 최대 3번 재시도
            retry_on_timeout=True,  # 타임아웃 시 재시도
        )
    
    async def close(self):
        await self.client.close()
    
    async def search_multiple_indices(
        self,
        indices: List[str],
        query: Dict[str, Any],
        size: int = 100,
        from_: int = 0,
        sort: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        여러 인덱스에서 동시에 검색
        """
        try:
            index_pattern = ",".join(indices)
            
            body = {
                "query": query,
                "size": size,
                "from": from_
            }
            
            if sort:
                body["sort"] = sort
            
            response = await self.client.search(
                index=index_pattern,
                body=body
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Elasticsearch search error: {e}")
            raise
    
    async def get_all_indices(self, pattern: str = "quantus-*") -> List[str]:
        """
        패턴에 맞는 모든 인덱스 조회
        """
        try:
            response = await self.client.indices.get(index=pattern)
            return list(response.keys())
        except Exception as e:
            logger.error(f"Failed to get indices: {e}")
            return []
    
    async def search_all_quantus_indices(
        self,
        query: Dict[str, Any],
        size: int = 100,
        from_: int = 0,
        sort: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        모든 quantus 인덱스에서 검색
        """
        indices = await self.get_all_indices("quantus-*")
        if not indices:
            logger.warning("No quantus indices found")
            return {"hits": {"total": {"value": 0}, "hits": []}}
        
        return await self.search_multiple_indices(indices, query, size, from_, sort)


# 전역 클라이언트 인스턴스
es_client = ElasticsearchClient()


# 의존성 주입용 함수
async def get_elasticsearch_client() -> ElasticsearchClient:
    return es_client


# 애플리케이션 종료 시 클린업
async def cleanup_elasticsearch():
    await es_client.close()