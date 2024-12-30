from typing import Sequence
from sqlalchemy.orm import Session
from app.models.models_stock import StockTrend
from sqlalchemy import select

from app.modules.common.enum import MarketType, TrendingPeriod, TrendingType


def get_trending_stocks(
    db: Session, market: MarketType, type: TrendingType, period: TrendingPeriod
) -> Sequence[StockTrend]:
    # stock_kr_1m이 아직 존재하지 않아 market 미적용, 추후 추가 예정
    stmt = select(StockTrend)
    match type:
        case TrendingType.UP:
            match period:
                case TrendingPeriod.REALTIME:
                    stmt.order_by(StockTrend.change_1m.desc())
                case TrendingPeriod.DAY:
                    stmt.order_by(StockTrend.change_1d.desc())
                case TrendingPeriod.WEEK:
                    stmt.order_by(StockTrend.change_1w.desc())
                case TrendingPeriod.MONTH:
                    stmt.order_by(StockTrend.change_1mo.desc())
                case TrendingPeriod.SIX_MONTH:
                    stmt.order_by(StockTrend.change_6mo.desc())
                case TrendingPeriod.YEAR:
                    stmt.order_by(StockTrend.change_1y.desc())
        case TrendingType.DOWN:
            match period:
                case TrendingPeriod.REALTIME:
                    stmt.order_by(StockTrend.change_1m.asc())
                case TrendingPeriod.DAY:
                    stmt.order_by(StockTrend.change_1d.asc())
                case TrendingPeriod.WEEK:
                    stmt.order_by(StockTrend.change_1w.asc())
                case TrendingPeriod.MONTH:
                    stmt.order_by(StockTrend.change_1mo.asc())
                case TrendingPeriod.SIX_MONTH:
                    stmt.order_by(StockTrend.change_6mo.asc())
                case TrendingPeriod.YEAR:
                    stmt.order_by(StockTrend.change_1y.asc())
        case TrendingType.VOL:
            match period:
                case TrendingPeriod.REALTIME:
                    stmt.order_by(StockTrend.volume_1m.desc())
                case TrendingPeriod.DAY:
                    stmt.order_by(StockTrend.volume_1d.desc())
                case TrendingPeriod.WEEK:
                    stmt.order_by(StockTrend.volume_1w.desc())
                case TrendingPeriod.MONTH:
                    stmt.order_by(StockTrend.volume_1mo.desc())
                case TrendingPeriod.SIX_MONTH:
                    stmt.order_by(StockTrend.volume_6mo.desc())
                case TrendingPeriod.YEAR:
                    stmt.order_by(StockTrend.volume_1y.desc())

        case TrendingType.AMT:
            match period:
                case TrendingPeriod.REALTIME:
                    stmt.order_by(StockTrend.volume_change_1m.desc())
                case TrendingPeriod.DAY:
                    stmt.order_by(StockTrend.volume_change_1d.desc())
                case TrendingPeriod.WEEK:
                    stmt.order_by(StockTrend.volume_change_1w.desc())
                case TrendingPeriod.MONTH:
                    stmt.order_by(StockTrend.volume_change_1mo.desc())
                case TrendingPeriod.SIX_MONTH:
                    stmt.order_by(StockTrend.volume_change_6mo.desc())
                case TrendingPeriod.YEAR:
                    stmt.order_by(StockTrend.volume_change_1y.desc())
    stmt.limit(100)
    return db.execute(stmt).scalars().all()


# def get_trending_stocks_base(db: Session, ctry: str, test_ticker: str = None) -> pd.DataFrame:
#     """트렌딩 주식 데이터 조회 쿼리"""
#     table_name = f"stock_{ctry.lower()}_1m"
#     ticker_condition = f"AND ticker = '{test_ticker}'" if test_ticker else ""

#     query = f"""
#     WITH LatestDate AS (
#         SELECT DATE(MAX(date)) as latest_date
#         FROM {table_name}
#         WHERE 1=1 {ticker_condition}
#     ),
#     DailyData AS (
#         SELECT
#             ticker,
#             date,
#             ROUND(open, 2) as open,
#             ROUND(close, 2) as close,
#             volume,
#             SUM(volume) OVER (PARTITION BY ticker, DATE(date)) as cumulative_volume,
#             SUM((open + high + low + close) / 4 * volume) OVER (PARTITION BY ticker, DATE(date)) as cumulative_trading_value,
#             ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date ASC) as first_row,
#             ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as last_row
#         FROM {table_name}
#         JOIN LatestDate ON DATE(date) = LatestDate.latest_date
#         WHERE 1=1 {ticker_condition}
#     )
#     SELECT
#         d1.ticker,
#         d2.close as close_price,
#         d2.cumulative_volume as total_volume,
#         ROUND(d2.cumulative_trading_value, 2) as total_trading_value,
#         ROUND(((d2.close - d1.open) / d1.open * 100), 2) as daily_change_rate
#     FROM
#         DailyData d1
#         JOIN DailyData d2 ON d1.ticker = d2.ticker
#     WHERE
#         d1.first_row = 1
#         AND d2.last_row = 1
#     """

#     return pd.read_sql(query, db.engine)


# def update_stock_trend(db: Session, stock_data: dict) -> None:
#     """stock_trend 테이블 업데이트 쿼리"""
#     query = """
#     INSERT INTO stock_trend (
#         ticker,
#         current_price,
#         change_1m,
#         volume_1m,
#         volume_change_1m,
#         last_updated,
#         market
#     ) VALUES (
#         :ticker,
#         :current_price,
#         :change_1m,
#         :volume,
#         :volume_change,
#         NOW(),
#         :market
#     ) ON DUPLICATE KEY UPDATE
#         current_price = VALUES(current_price),
#         change_1m = VALUES(change_1m),
#         volume_1m = VALUES(volume_1m),
#         volume_change_1m = VALUES(volume_change_1m),
#         last_updated = VALUES(last_updated)
#     """

#     db.execute(text(query), stock_data)
