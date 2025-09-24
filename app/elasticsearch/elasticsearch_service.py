from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class QueryOperator(Enum):
    """쿼리 연산자"""
    AND = "must"
    OR = "should"
    NOT = "must_not"


class ElasticsearchQueryBuilder:
    """엘라스틱서치 쿼리 빌더"""
    
    def __init__(self):
        self.reset()
    
    def reset(self) -> 'ElasticsearchQueryBuilder':
        """쿼리 빌더 초기화"""
        self._query = {"bool": {"must": [], "should": [], "must_not": []}}
        self._size = 10
        self._from_ = 0
        self._sort = []
        self._aggs = {}
        return self
    
    def must(self, *conditions: Dict[str, Any]) -> 'ElasticsearchQueryBuilder':
        """AND 조건 추가"""
        self._query["bool"]["must"].extend(conditions)
        return self
    
    def should(self, *conditions: Dict[str, Any]) -> 'ElasticsearchQueryBuilder':
        """OR 조건 추가"""
        self._query["bool"]["should"].extend(conditions)
        return self
    
    def must_not(self, *conditions: Dict[str, Any]) -> 'ElasticsearchQueryBuilder':
        """NOT 조건 추가"""
        self._query["bool"]["must_not"].extend(conditions)
        return self
    
    def terms(self, field: str, values: List[str]) -> 'ElasticsearchQueryBuilder':
        """terms 쿼리 추가 (여러 값 중 하나와 일치)"""
        condition = {"terms": {field: values}}
        return self.must(condition)
    
    def term(self, field: str, value: Any) -> 'ElasticsearchQueryBuilder':
        """term 쿼리 추가 (정확한 값과 일치)"""
        condition = {"term": {field: value}}
        return self.must(condition)
    
    def range_query(self, field: str, **kwargs) -> 'ElasticsearchQueryBuilder':
        """range 쿼리 추가"""
        condition = {"range": {field: kwargs}}
        return self.must(condition)
    
    def date_range(self, field: str, gte: Optional[datetime] = None, 
                   lte: Optional[datetime] = None, 
                   gt: Optional[datetime] = None, 
                   lt: Optional[datetime] = None) -> 'ElasticsearchQueryBuilder':
        """날짜 범위 쿼리 추가"""
        range_params = {}
        if gte:
            range_params["gte"] = gte.isoformat()
        if lte:
            range_params["lte"] = lte.isoformat()
        if gt:
            range_params["gt"] = gt.isoformat()
        if lt:
            range_params["lt"] = lt.isoformat()
        
        return self.range_query(field, **range_params)
    
    def match(self, field: str, query: str, **kwargs) -> 'ElasticsearchQueryBuilder':
        """match 쿼리 추가"""
        condition = {"match": {field: {"query": query, **kwargs}}}
        return self.must(condition)
    
    def multi_match(self, query: str, fields: List[str], **kwargs) -> 'ElasticsearchQueryBuilder':
        """multi_match 쿼리 추가"""
        condition = {"multi_match": {"query": query, "fields": fields, **kwargs}}
        return self.must(condition)
    
    def exists(self, field: str) -> 'ElasticsearchQueryBuilder':
        """exists 쿼리 추가 (필드가 존재하는지 확인)"""
        condition = {"exists": {"field": field}}
        return self.must(condition)
    
    def wildcard(self, field: str, value: str) -> 'ElasticsearchQueryBuilder':
        """wildcard 쿼리 추가"""
        condition = {"wildcard": {field: value}}
        return self.must(condition)
    
    def regexp(self, field: str, value: str) -> 'ElasticsearchQueryBuilder':
        """regexp 쿼리 추가"""
        condition = {"regexp": {field: value}}
        return self.must(condition)
    
    def nested(self, path: str, query: Dict[str, Any]) -> 'ElasticsearchQueryBuilder':
        """nested 쿼리 추가"""
        condition = {"nested": {"path": path, "query": query}}
        return self.must(condition)
    
    def size(self, size: int) -> 'ElasticsearchQueryBuilder':
        """결과 개수 설정"""
        self._size = size
        return self
    
    def from_(self, from_: int) -> 'ElasticsearchQueryBuilder':
        """시작 위치 설정"""
        self._from_ = from_
        return self
    
    def sort(self, field: str, order: str = "desc") -> 'ElasticsearchQueryBuilder':
        """정렬 설정"""
        self._sort.append({field: {"order": order}})
        return self
    
    def sort_by_date(self, order: str = "desc") -> 'ElasticsearchQueryBuilder':
        """날짜로 정렬"""
        return self.sort("date", order)
    
    def aggregation(self, name: str, agg_type: str, **kwargs) -> 'ElasticsearchQueryBuilder':
        """집계 추가"""
        self._aggs[name] = {agg_type: kwargs}
        return self
    
    def build(self) -> Dict[str, Any]:
        """최종 쿼리 빌드"""
        query_body = {"query": self._query}
        
        if self._size != 10:
            query_body["size"] = self._size
        
        if self._from_ != 0:
            query_body["from"] = self._from_
        
        if self._sort:
            query_body["sort"] = self._sort
        
        if self._aggs:
            query_body["aggs"] = self._aggs
        
        return query_body
    
    def build_query_only(self) -> Dict[str, Any]:
        """쿼리 부분만 반환"""
        return self._query


class ElasticsearchService:
    """엘라스틱서치 서비스 클래스"""
    
    def __init__(self, client):
        self.client = client
    
    def create_query_builder(self) -> ElasticsearchQueryBuilder:
        """쿼리 빌더 생성"""
        return ElasticsearchQueryBuilder()
    
    async def search_with_builder(
        self, 
        index: str, 
        query_builder: ElasticsearchQueryBuilder
    ) -> Dict[str, Any]:
        """쿼리 빌더를 사용한 검색"""
        query_body = query_builder.build()
        return await self.client.search(index=index, body=query_body)
    
    async def search_multiple_indices_with_builder(
        self,
        indices: List[str],
        query_builder: ElasticsearchQueryBuilder
    ) -> Dict[str, Any]:
        """여러 인덱스에서 쿼리 빌더를 사용한 검색"""
        query_body = query_builder.build()
        index_pattern = ",".join(indices)
        return await self.client.search(index=index_pattern, body=query_body)
    


# 편의 함수들
def create_stock_price_query(tickers: List[str]) -> ElasticsearchQueryBuilder:
    """주식 가격 조회용 쿼리 생성"""
    return ElasticsearchQueryBuilder().terms("ticker.keyword", tickers)


def create_news_query(
    tickers: List[str],
    start_date: datetime,
    end_date: datetime,
    lang: str = "ko-KR",
    is_exist: bool = True,
    is_related: bool = True
) -> ElasticsearchQueryBuilder:
    """뉴스 조회용 쿼리 생성"""
    builder = ElasticsearchQueryBuilder()
    
    if is_exist:
        builder.term("is_exist", True)
    
    if is_related:
        builder.term("is_related", True)
    
    builder.terms("ticker.keyword", tickers)
    builder.date_range("date", gte=start_date, lte=end_date)
    builder.term("lang.keyword", lang)
    builder.sort_by_date("desc")
    
    return builder


def create_disclosure_query(
    tickers: List[str],
    start_date: datetime,
    end_date: datetime,
    lang: str = "ko-KR",
    is_exist: bool = True
) -> ElasticsearchQueryBuilder:
    """공시 조회용 쿼리 생성"""
    builder = ElasticsearchQueryBuilder()
    
    if is_exist:
        builder.term("is_exist", True)
    
    builder.terms("ticker.keyword", tickers)
    builder.date_range("date", gte=start_date, lte=end_date)
    builder.term("lang.keyword", lang)
    builder.sort_by_date("desc")
    
    return builder


def create_ticker_search_query(ticker: str) -> ElasticsearchQueryBuilder:
    """티커 검색용 쿼리 생성"""
    return ElasticsearchQueryBuilder().term("ticker.keyword", ticker)


def create_company_name_search_query(company_name: str, lang: str = "ko") -> ElasticsearchQueryBuilder:
    """회사명 검색용 쿼리 생성"""
    builder = ElasticsearchQueryBuilder()
    
    if lang == "ko":
        builder.multi_match(company_name, ["kr_name", "company_name"])
    else:
        builder.multi_match(company_name, ["en_name", "company_name"])
    
    return builder


def create_trending_tickers_query() -> ElasticsearchQueryBuilder:
    """실시간 인기 티커 조회용 쿼리 생성 (US 6개, KR 5개)"""
    builder = ElasticsearchQueryBuilder()
    builder.term("is_related", True)
    builder.term("is_exist", True)
    builder.range_query("date", gte="now-24h", lte="now+5m")
    builder.size(0)  # 집계만 사용하므로 문서는 반환하지 않음

    # US 티커 집계 (A로 시작하지 않는 것들)
    us_agg = {
        "filter": {
            "bool": {
                "must_not": {
                    "prefix": {
                        "ticker.keyword": "A"
                    }
                }
            }
        },
        "aggs": {
            "top_us": {
                "terms": {
                    "field": "ticker.keyword",
                    "size": 6,
                    "order": {
                        "latest_date": "desc"
                    }
                },
                "aggs": {
                    "latest_date": {
                        "max": {
                            "field": "date"
                        }
                    }
                }
            }
        }
    }

    # KR 티커 집계 (A로 시작하는 것들)
    kr_agg = {
        "filter": {
            "prefix": {
                "ticker.keyword": "A"
            }
        },
        "aggs": {
            "top_kr": {
                "terms": {
                    "field": "ticker.keyword",
                    "size": 5,
                    "order": {
                        "latest_date": "desc"
                    }
                },
                "aggs": {
                    "latest_date": {
                        "max": {
                            "field": "date"
                        }
                    }
                }
            }
        }
    }

    builder._aggs["us_tickers"] = us_agg
    builder._aggs["kr_tickers"] = kr_agg

    return builder


def create_stock_price_query(tickers: List[str]) -> ElasticsearchQueryBuilder:
    """주식 가격 조회용 쿼리 생성"""
    return ElasticsearchQueryBuilder().terms("ticker.keyword", tickers)
