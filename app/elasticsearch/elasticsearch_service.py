# from typing import Dict, List, Any, Optional
# from app.elasticsearch.elasticsearch import get_elasticsearch_client
# import logging

# logger = logging.getLogger(__name__)


# class SearchService:
    
#     @staticmethod
#     async def search_stocks(
#         query_text: str = "",
#         filters: Optional[Dict[str, Any]] = None,
#         size: int = 20,
#         from_: int = 0
#     ) -> Dict[str, Any]:
#         """
#         주식 데이터 통합 검색
#         """
#         es_client = await get_elasticsearch_client()
        
#         # 기본 검색 쿼리 구성
#         if query_text:
#             search_query = {
#                 "bool": {
#                     "should": [
#                         {"match": {"ticker.keyword": {"query": query_text, "boost": 3}}},
#                         {"match": {"kr_name": {"query": query_text, "boost": 2}}},
#                         {"match": {"en_name": {"query": query_text, "boost": 2}}},
#                         {"wildcard": {"ticker.keyword": f"*{query_text.upper()}*"}},
#                         {"wildcard": {"kr_name": f"*{query_text}*"}},
#                         {"wildcard": {"en_name": f"*{query_text}*"}}
#                     ],
#                     "minimum_should_match": 1
#                 }
#             }
#         else:
#             search_query = {"match_all": {}}
        
#         # 필터 추가
#         if filters:
#             if "bool" not in search_query:
#                 search_query = {"bool": {"must": [search_query]}}
            
#             filter_conditions = []
            
#             if "market" in filters:
#                 filter_conditions.append({"term": {"market.keyword": filters["market"]}})
            
#             if "ctry" in filters:
#                 filter_conditions.append({"term": {"ctry.keyword": filters["ctry"]}})
            
#             if "price_range" in filters:
#                 price_range = filters["price_range"]
#                 filter_conditions.append({
#                     "range": {
#                         "current_price": {
#                             "gte": price_range.get("min", 0),
#                             "lte": price_range.get("max", 999999)
#                         }
#                     }
#                 })
            
#             if filter_conditions:
#                 search_query["bool"]["filter"] = filter_conditions
        
#         # 정렬 기본값: 거래량 기준
#         sort = [
#             {"volume_rt": {"order": "desc", "missing": "_last"}},
#             {"current_price": {"order": "desc", "missing": "_last"}}
#         ]
        
#         try:
#             response = await es_client.search_all_quantus_indices(
#                 query=search_query,
#                 size=size,
#                 from_=from_,
#                 sort=sort
#             )
            
#             return {
#                 "total": response["hits"]["total"]["value"],
#                 "results": [hit["_source"] for hit in response["hits"]["hits"]],
#                 "query_info": {
#                     "query_text": query_text,
#                     "filters": filters,
#                     "size": size,
#                     "from": from_
#                 }
#             }
            
#         except Exception as e:
#             logger.error(f"Search error: {e}")
#             return {
#                 "total": 0,
#                 "results": [],
#                 "error": str(e)
#             }
    
#     @staticmethod
#     async def get_stock_by_ticker(ticker: str) -> Optional[Dict[str, Any]]:
#         """
#         티커로 특정 주식 조회
#         """
#         es_client = await get_elasticsearch_client()
        
#         query = {
#             "term": {
#                 "ticker.keyword": ticker.upper()
#             }
#         }
        
#         try:
#             response = await es_client.search_all_quantus_indices(
#                 query=query,
#                 size=1
#             )
            
#             hits = response["hits"]["hits"]
#             return hits[0]["_source"] if hits else None
            
#         except Exception as e:
#             logger.error(f"Get stock error: {e}")
#             return None
    
#     @staticmethod
#     async def get_trending_stocks(limit: int = 10) -> List[Dict[str, Any]]:
#         """
#         거래량 기준 인기 주식 조회
#         """
#         es_client = await get_elasticsearch_client()
        
#         query = {"match_all": {}}
#         sort = [{"volume_rt": {"order": "desc", "missing": "_last"}}]
        
#         try:
#             response = await es_client.search_all_quantus_indices(
#                 query=query,
#                 size=limit,
#                 sort=sort
#             )
            
#             return [hit["_source"] for hit in response["hits"]["hits"]]
            
#         except Exception as e:
#             logger.error(f"Trending stocks error: {e}")
#             return []