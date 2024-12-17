import yfinance as yf
from typing import Tuple

from app.modules.stock_indices.schemas import IndicesData, IndexSummary, IndicesResponse, TimeData
from app.database.crud import database


class StockIndicesService:
    def __init__(self):
        self.db = database
        self.symbols = {"kospi": "^KS11", "kosdaq": "^KQ11", "nasdaq": "^IXIC", "sp500": "^GSPC"}

    def get_yesterday_indices_data(self) -> dict:
        try:
            result = {}
            for name, symbol in self.symbols.items():
                ticker = yf.Ticker(symbol)
                df = ticker.history(period="1d")

                # 기본값 설정
                prev_close = 0.00
                change = 0.00
                change_percent = 0.00
                rise_ratio = 0.00
                fall_ratio = 0.00
                unchanged_ratio = 0.00

                # yfinance 데이터 처리
                if not df.empty:
                    open_price = round(float(df["Open"].iloc[0]), 2)
                    prev_close = round(float(df["Close"].iloc[0]), 2)
                    change = round(prev_close - open_price, 2)
                    change_percent = round((change / open_price) * 100, 2) if open_price != 0 else 0.00

                # 비율 데이터 조회 (yfinance 데이터와 독립적으로 처리)
                rise_ratio, fall_ratio, unchanged_ratio = self.get_kospi_advance_decline_ratio(name)

                result[name] = IndexSummary(
                    prev_close=prev_close,
                    change=change,
                    change_percent=change_percent,
                    rise_ratio=rise_ratio,
                    fall_ratio=fall_ratio,
                    unchanged_ratio=unchanged_ratio,
                )

            return result

        except Exception as e:
            print(f"Error in get_yesterday_indices_data: {str(e)}")
            empty_summary = IndexSummary(
                prev_close=0.00, change=0.00, change_percent=0.00, rise_ratio=0.00, fall_ratio=0.00, unchanged_ratio=0.00
            )
            return {key: empty_summary for key in self.symbols.keys()}

    def get_daily_5min_data(self, symbol: str) -> dict:
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="1d", interval="5m")

            result = {}
            for index, row in df.iterrows():
                time_key = index.strftime("%Y-%m-%d %H:%M:%S")
                result[time_key] = TimeData(
                    open=round(float(row["Open"]), 2),
                    high=round(float(row["High"]), 2),
                    low=round(float(row["Low"]), 2),
                    close=round(float(row["Close"]), 2),
                    volume=round(float(row["Volume"]), 2),
                )

            return result
        except Exception as e:
            print(f"Error in get_daily_5min_data: {str(e)}")
            return {}

    async def get_indices_data(self) -> IndicesData:
        try:
            # yfinance에서 모든 지수의 전일 데이터 조회
            indices_summary = self.get_yesterday_indices_data()

            # 5분봉 데이터 조회
            indices_data = {}
            for name, symbol in self.symbols.items():
                indices_data[name] = self.get_daily_5min_data(symbol)

            return IndicesData(
                status_code=200,
                message="데이터를 성공적으로 조회했습니다.",
                kospi=indices_summary["kospi"],
                kosdaq=indices_summary["kosdaq"],
                nasdaq=indices_summary["nasdaq"],
                sp500=indices_summary["sp500"],
                data=IndicesResponse(
                    kospi=indices_data["kospi"],
                    kosdaq=indices_data["kosdaq"],
                    nasdaq=indices_data["nasdaq"],
                    sp500=indices_data["sp500"],
                ),
            )

        except Exception as e:
            empty_summary = IndexSummary(
                prev_close=0.00, change=0.00, change_percent=0.00, rise_count=0, fall_count=0, unchanged_count=0
            )
            return IndicesData(
                status_code=404,
                message=f"데이터 조회 중 오류가 발생했습니다: {str(e)}",
                kospi=empty_summary,
                kosdaq=empty_summary,
                nasdaq=empty_summary,
                sp500=empty_summary,
                data=None,
            )

    # 코스피 상승, 하락, 보합 비율 조회
    def get_kospi_advance_decline_ratio(self, market: str) -> Tuple[float, float, float]:
        try:
            # 입력받은 market을 대문자로 변환하여 매핑
            market_mapping = {"kospi": "KOSPI", "kosdaq": "KOSDAQ", "nasdaq": "NASDAQ", "sp500": "S&P500"}
            market_filter = market_mapping.get(market.lower(), market)

            # 시장별 테이블 설정
            table = "stock_kr_1d" if market_filter in ["KOSPI", "KOSDAQ"] else "stock_us_1d"

            # 최신 날짜 조회
            latest_date = self.db._select(
                table=table, columns=["Date"], order="Date", ascending=False, limit=1, Market=market_filter
            )

            if not latest_date:
                return 0.0, 0.0, 0.0

            # 해당 날짜의 데이터 조회
            result = self.db._select(
                table=table, columns=["Open", "Close"], Market=market_filter, Date=latest_date[0].Date
            )

            if result:
                advance = 0
                decline = 0
                unchanged = 0
                total_valid = 0

                for row in result:
                    if row.Open == 0:  # 0으로 나누기 방지
                        continue

                    total_valid += 1
                    change_percent = (row.Close - row.Open) / row.Open * 100

                    if change_percent > 1:
                        advance += 1
                    elif change_percent < -1:
                        decline += 1
                    else:
                        unchanged += 1

                if total_valid > 0:
                    # 소수점 2자리까지 반올림
                    advance_ratio = round((advance / total_valid) * 100, 2)
                    decline_ratio = round((decline / total_valid) * 100, 2)
                    unchanged_ratio = round((unchanged / total_valid) * 100, 2)

                    return advance_ratio, decline_ratio, unchanged_ratio

            return 0.0, 0.0, 0.0

        except Exception as e:
            print(f"Error in get_kospi_advance_decline_ratio for {market}: {str(e)}")
            return 0.0, 0.0, 0.0

    def get_kosdaq_advance_decline_ratio(self) -> Tuple[int, int, int]:
        try:
            query = """
                SELECT
                    SUM(CASE WHEN ((Close - Open) / Open * 100) > 1 THEN 1 ELSE 0 END) as advance,
                    SUM(CASE WHEN ((Close - Open) / Open * 100) < -1 THEN 1 ELSE 0 END) as decline,
                    SUM(CASE WHEN ABS(((Close - Open) / Open * 100)) <= 1 THEN 1 ELSE 0 END) as unchanged
                FROM stock_kr_1d
                WHERE Market = 'KOSDAQ'
                AND Date = (SELECT MAX(Date) FROM stock_kr_1d WHERE Market = 'KOSDAQ')
            """

            result = self.db.execute_raw_query(query)

            if result and result[0]:
                return (int(result[0].advance or 0), int(result[0].decline or 0), int(result[0].unchanged or 0))

            return 0, 0, 0

        except Exception as e:
            print(f"Error in get_kosdaq_advance_decline_ratio: {str(e)}")
            return 0, 0, 0

    # 나스닥 상승, 하락, 보합 비율 조회
    def get_nasdaq_advance_decline_ratio(self) -> Tuple[int, int, int]:
        try:
            query = """
                SELECT
                    SUM(CASE WHEN ((Close - Open) / Open * 100) > 1 THEN 1 ELSE 0 END) as advance,
                    SUM(CASE WHEN ((Close - Open) / Open * 100) < -1 THEN 1 ELSE 0 END) as decline,
                    SUM(CASE WHEN ABS(((Close - Open) / Open * 100)) <= 1 THEN 1 ELSE 0 END) as unchanged
                FROM stock_us_1d
                WHERE Market = 'NASDAQ'
                AND Date = (SELECT MAX(Date) FROM stock_us_1d WHERE Market = 'NASDAQ')
            """

            result = self.db.execute_raw_query(query)

            if result and result[0]:
                return (int(result[0].advance or 0), int(result[0].decline or 0), int(result[0].unchanged or 0))

            return 0, 0, 0

        except Exception as e:
            print(f"Error in get_nasdaq_advance_decline_ratio: {str(e)}")
            return 0, 0, 0
