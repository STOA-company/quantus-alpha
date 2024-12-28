from sqlalchemy import text
from app.database.crud import database


# 가장 최근 날짜, 시간 조회
def get_latest_date_time():
    table_name = "stock_us_1m"
    query = text(f"""
        SELECT s.*
        FROM {table_name} s
        JOIN (
            SELECT date
            FROM business_days
            WHERE market = 'US'
            AND is_trading = 1
            ORDER BY date DESC
            LIMIT 1 OFFSET 1
        ) b ON DATE(s.date) = b.date
    """)

    with database.session() as session:
        result = session.execute(query)
        return result.fetchall()


def run_stock_trend_realtime_batch():
    pass


def run_stock_trend_batch():
    pass
