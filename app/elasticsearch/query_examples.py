"""
엘라스틱서치 쿼리 빌더 사용 예시
"""

from datetime import datetime, timedelta
from app.elasticsearch.elasticsearch_service import (
    ElasticsearchQueryBuilder,
    create_stock_price_query,
    create_news_query,
    create_disclosure_query,
    create_ticker_search_query,
    create_company_name_search_query
)


def example_basic_query():
    """기본 쿼리 빌더 사용 예시"""
    print("=== 기본 쿼리 빌더 사용 예시 ===")
    
    # 기본 쿼리 생성
    query = ElasticsearchQueryBuilder() \
        .term("status", "active") \
        .terms("category.keyword", ["tech", "finance"]) \
        .size(20) \
        .sort_by_date("desc")
    
    print("생성된 쿼리:")
    print(query.build())
    print()


def example_stock_price_query():
    """주식 가격 조회 쿼리 예시"""
    print("=== 주식 가격 조회 쿼리 예시 ===")
    
    tickers = ["AAPL", "GOOGL", "MSFT"]
    query = create_stock_price_query(tickers).size(10)
    
    print("생성된 쿼리:")
    print(query.build())
    print()


def example_news_query():
    """뉴스 조회 쿼리 예시"""
    print("=== 뉴스 조회 쿼리 예시 ===")
    
    tickers = ["AAPL", "GOOGL"]
    start_date = datetime.now() - timedelta(days=1)
    end_date = datetime.now()
    
    query = create_news_query(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        lang="ko-KR",
        is_exist=True,
        is_related=True
    ).size(100)
    
    print("생성된 쿼리:")
    print(query.build())
    print()


def example_complex_query():
    """복잡한 쿼리 예시"""
    print("=== 복잡한 쿼리 예시 ===")
    
    query = ElasticsearchQueryBuilder() \
        .must(
            {"term": {"is_active": True}},
            {"terms": {"market.keyword": ["KOSPI", "KOSDAQ"]}}
        ) \
        .should(
            {"match": {"title": "삼성전자"}},
            {"match": {"content": "반도체"}}
        ) \
        .must_not(
            {"term": {"is_deleted": True}}
        ) \
        .date_range("created_at", 
                   gte=datetime.now() - timedelta(days=7),
                   lte=datetime.now()) \
        .size(50) \
        .sort("score", "desc") \
        .sort_by_date("desc")
    
    print("생성된 쿼리:")
    print(query.build())
    print()


def example_aggregation_query():
    """집계 쿼리 예시"""
    print("=== 집계 쿼리 예시 ===")
    
    query = ElasticsearchQueryBuilder() \
        .term("market.keyword", "KOSPI") \
        .aggregation("avg_price", "avg", field="price") \
        .aggregation("price_stats", "stats", field="price") \
        .aggregation("market_buckets", "terms", field="sector.keyword", size=10)
    
    print("생성된 쿼리:")
    print(query.build())
    print()


def example_search_queries():
    """검색 쿼리 예시"""
    print("=== 검색 쿼리 예시 ===")
    
    # 티커 검색
    ticker_query = create_ticker_search_query("AAPL")
    print("티커 검색 쿼리:")
    print(ticker_query.build())
    print()
    
    # 회사명 검색
    company_query = create_company_name_search_query("삼성전자", lang="ko")
    print("회사명 검색 쿼리:")
    print(company_query.build())
    print()


if __name__ == "__main__":
    example_basic_query()
    example_stock_price_query()
    example_news_query()
    example_complex_query()
    example_aggregation_query()
    example_search_queries()
