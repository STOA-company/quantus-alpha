from datetime import datetime, timedelta
from sqlalchemy import text
from typing import List, Dict, Any

from app.database.conn import db
from app.models.models_stock import StockTrend

def get_latest_stock_data(db) -> List[Dict[str, Any]]:
    """최신 1분봉 데이터 조회"""
    query = text("""
        SELECT s1.* 
        FROM stock_us_1m s1
        INNER JOIN (
            SELECT Ticker, MAX(Date) as MaxDate
            FROM stock_us_1m
            GROUP BY Ticker
        ) s2 ON s1.Ticker = s2.Ticker AND s1.Date = s2.MaxDate
    """)
    return [dict(row) for row in db.execute(query)]

def run_stock_trend_batch():
    """주식 트렌드 정보 업데이트 배치 (1분 데이터만)"""
    try:
        # 최신 데이터 조회
        latest_data = get_latest_stock_data(db)

        for stock in latest_data:
            # 기존 StockTrend 레코드 조회 또는 새로 생성
            trend = db.query(StockTrend).filter(
                StockTrend.ticker == stock['Ticker']
            ).first() or StockTrend(ticker=stock['Ticker'])

            # 기본 정보 업데이트
            trend.last_updated = stock['Date']
            trend.market = stock['Market']
            trend.current_price = stock['Close']

            # 1분 전 데이터 조회
            one_min_ago = stock['Date'] - timedelta(minutes=1)
            prev_min_query = text("""
                SELECT Close 
                FROM stock_us_1m 
                WHERE Ticker = :ticker 
                AND Date <= :date 
                ORDER BY Date DESC 
                LIMIT 1
            """)
            prev_min = db.execute(
                prev_min_query, 
                {"ticker": stock['Ticker'], "date": one_min_ago}
            ).first()

            # 1분 등락률 계산
            if prev_min:
                trend.change_1m = ((stock['Close'] - prev_min[0]) / prev_min[0]) * 100
                trend.volume_1m = stock['Volume']
                trend.volume_change_1m = stock['Volume'] * stock['Close']  # 거래대금
            else:
                trend.change_1m = 0
                trend.volume_1m = stock['Volume']
                trend.volume_change_1m = stock['Volume'] * stock['Close']

            db.merge(trend)
        
        db.commit()

    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()