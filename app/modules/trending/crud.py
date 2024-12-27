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
