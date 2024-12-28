from typing import List, Dict, Any

from app.database.crud import database
from app.database.conn import db
from app.modules.trending.crud import get_trending_stocks_base, update_stock_trend


class TrendingService:
    def __init__(self):
        self.database = database
        self.db = db

    def get_tranding_stocks_base(self, db, ctry: str = "US", test_ticker: str = None) -> List[Dict[Any, Any]]:
        """트렌딩 주식 데이터 조회 및 stock_trend 테이블 업데이트"""
        try:
            # crud에서 데이터 조회
            df = get_trending_stocks_base(db, ctry, test_ticker)

            if df.empty:
                print(f"No data found for {'ticker ' + test_ticker if test_ticker else ctry + ' market'}")
                return []

            # 데이터 포맷팅
            formatted_data = []
            for _, row in df.iterrows():
                formatted_row = {
                    "ticker": row["ticker"],
                    "current_price": round(float(row["close_price"]), 2),
                    "change_1m": round(float(row["daily_change_rate"]), 2),
                    "volume": int(row["total_volume"]),
                    "volume_change": round(float(row["total_trading_value"]), 2),
                    "market": ctry,
                }
                formatted_data.append(formatted_row)

            # 트랜잭션으로 데이터 업데이트
            with db.session() as session:
                try:
                    for stock in formatted_data:
                        update_stock_trend(session, stock)
                    session.commit()
                    print(f"Successfully updated {len(formatted_data)} records in stock_trend table")
                except Exception as e:
                    session.rollback()
                    print(f"Error during update: {e}")
                    raise

            return formatted_data

        except Exception as e:
            print(f"Error in get_tranding_stocks_base: {e}")
            return []

    def update_stock_trends(self, ctry: str = "US", test_ticker: str = None) -> None:
        """stock_trend 테이블 업데이트 실행"""
        try:
            stocks_data = self.get_tranding_stocks_base(self.db, ctry, test_ticker)
            if stocks_data:
                print(f"Updated {len(stocks_data)} stocks in {ctry} market")
        except Exception as e:
            print(f"Error updating stock trends: {e}")


def get_trending_service():
    return TrendingService()
