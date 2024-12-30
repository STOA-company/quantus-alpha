import datetime
import logging
import pandas as pd
from app.database.crud import database
from sqlalchemy.sql import text
from app.utils.date_utils import now_utc
from app.utils.ctry_utils import get_current_market_country


def kr_run_stock_indices_batch():
    pass


def us_run_stock_indices_batch():
    """
    나스닥, S&P500 지수 데이터 배치 로직
    """
    try:
        current_time = datetime.datetime.now()

        nasdaq_result = _process_nasdaq_data()
        sp500_result = _process_sp500_data()

        # stock_indices 테이블 업데이트
        for result in [nasdaq_result, sp500_result]:
            database._update(
                table="stock_indices",
                sets={
                    "rise_ratio": result["상승"],
                    "fall_ratio": result["하락"],
                    "unchanged_ratio": result["보합"],
                    "date": current_time,
                },
                ticker=result["ticker"],
            )

        logging.info(f"US market status batch completed at {current_time}")

    except Exception as e:
        logging.error(f"Error in us_run_stock_indices_batch: {str(e)}")


def _is_market_open(ticker: str) -> bool:
    """현재 시간 기준으로 해당 시장이 열렸는지 확인"""
    current_market = get_current_market_country()

    if ticker in ["nasdaq", "sp500"]:
        return current_market == "us"

    return False


def _process_nasdaq_data():
    try:
        # 나스닥 100 종목 조회
        logging.info("Fetching NASDAQ 100 tickers...")
        result = database._select(table="stock_information", columns=["ticker"], is_nasdaq_100=1)

        if not result:
            logging.warning("No NASDAQ 100 tickers found")
            return {"ticker": "nasdaq", "상승": 0.0, "하락": 0.0, "보합": 0.0}

        tickers = [row[0] for row in result]
        logging.info(f"Found {len(tickers)} NASDAQ tickers: {tickers[:5]}")

        # 최신 날짜 조회 - database._execute 사용
        latest_date_query = text("""
           SELECT DATE(MAX(Date)) as latest_date
           FROM stock_us_1m
           WHERE Ticker IN :tickers
       """)
        latest_date = database._execute(latest_date_query, {"tickers": tickers}).scalar()
        logging.info(f"Latest date for NASDAQ: {latest_date}")

        # 당일 시가/종가 데이터 조회
        daily_prices = database._select(
            table="stock_us_1m", columns=["Ticker", "Open", "Close"], Ticker__in=tickers, Date__date=latest_date
        )

        if not daily_prices:
            return {"ticker": "nasdaq", "상승": 0.0, "하락": 0.0, "보합": 0.0}

        # DataFrame 생성 시 컬럼명 대문자로 지정
        df = pd.DataFrame(daily_prices, columns=["Ticker", "Open", "Close"])

        # 종목별 데이터 집계
        df = df.groupby("Ticker").agg({"Open": "first", "Close": "last"}).reset_index()

        # 변동률 계산
        df["변동률"] = ((df["Close"] - df["Open"]) / df["Open"]) * 100
        total = len(df)

        # 상승/하락/보합 계산
        rise = len(df[df["변동률"] > 0.1])
        fall = len(df[df["변동률"] < -0.1])
        unchanged = len(df[df["변동률"].abs() <= 0.1])

        result = {
            "ticker": "nasdaq",
            "상승": round(rise / total * 100, 2),
            "하락": round(fall / total * 100, 2),
            "보합": round(unchanged / total * 100, 2),
        }

        logging.info(f"Processing NASDAQ data: {result}")

        # 기존 데이터 확인
        existing_data = database._select(table="stock_indices", columns=["ticker"], ticker="nasdaq")

        if not existing_data:
            # INSERT
            logging.info("Inserting new NASDAQ data")
            is_open = _is_market_open("nasdaq")
            logging.info(f"Market status for NASDAQ: {'Open' if is_open else 'Closed'}")

            database._insert(
                table="stock_indices",
                sets={
                    "ticker": "nasdaq",
                    "rise_ratio": result["상승"],
                    "fall_ratio": result["하락"],
                    "unchanged_ratio": result["보합"],
                    "date": now_utc(),
                    "included_indices": is_open,
                },
            )
        else:
            # UPDATE
            logging.info("Updating existing NASDAQ data")
            is_open = _is_market_open("nasdaq")
            logging.info(f"Market status for NASDAQ: {'Open' if is_open else 'Closed'}")

            database._update(
                table="stock_indices",
                sets={
                    "rise_ratio": result["상승"],
                    "fall_ratio": result["하락"],
                    "unchanged_ratio": result["보합"],
                    "date": now_utc(),
                    "included_indices": is_open,
                },
                ticker="nasdaq",
            )

        return result

    except Exception as e:
        logging.error(f"Error in _process_nasdaq_data: {str(e)}")
        raise


def _process_sp500_data():
    try:
        # S&P 500 종목 조회
        logging.info("Fetching S&P 500 tickers...")
        result = database._select(table="stock_information", columns=["ticker"], is_snp_500=1)

        if not result:
            logging.warning("No S&P 500 tickers found")
            return {"ticker": "sp500", "상승": 0.0, "하락": 0.0, "보합": 0.0}

        tickers = [row[0] for row in result]
        logging.info(f"Found {len(tickers)} S&P 500 tickers: {tickers[:5]}")

        # 최신 날짜 조회 - database._execute 사용
        latest_date_query = text("""
           SELECT DATE(MAX(Date)) as latest_date
           FROM stock_us_1m
           WHERE Ticker IN :tickers
       """)
        latest_date = database._execute(latest_date_query, {"tickers": tickers}).scalar()
        logging.info(f"Latest date for S&P 500: {latest_date}")

        # 당일 시가/종가 데이터 조회
        daily_prices = database._select(
            table="stock_us_1m", columns=["Ticker", "Open", "Close"], Ticker__in=tickers, Date__date=latest_date
        )

        if not daily_prices:
            return {"ticker": "sp500", "상승": 0.0, "하락": 0.0, "보합": 0.0}

        # DataFrame 생성 시 컬럼명 대문자로 지정
        df = pd.DataFrame(daily_prices, columns=["Ticker", "Open", "Close"])

        # 종목별 데이터 집계
        df = df.groupby("Ticker").agg({"Open": "first", "Close": "last"}).reset_index()

        # 변동률 계산
        df["변동률"] = ((df["Close"] - df["Open"]) / df["Open"]) * 100
        total = len(df)

        # 상승/하락/보합 계산
        rise = len(df[df["변동률"] > 0.1])
        fall = len(df[df["변동률"] < -0.1])
        unchanged = len(df[df["변동률"].abs() <= 0.1])

        result = {
            "ticker": "sp500",
            "상승": round(rise / total * 100, 2),
            "하락": round(fall / total * 100, 2),
            "보합": round(unchanged / total * 100, 2),
        }

        logging.info(f"Processing S&P 500 data: {result}")

        # 기존 데이터 확인
        existing_data = database._select(table="stock_indices", columns=["ticker"], ticker="sp500")

        if not existing_data:
            # INSERT
            logging.info("Inserting new S&P 500 data")
            is_open = _is_market_open("sp500")
            logging.info(f"Market status for S&P 500: {'Open' if is_open else 'Closed'}")

            database._insert(
                table="stock_indices",
                sets={
                    "ticker": "sp500",
                    "rise_ratio": result["상승"],
                    "fall_ratio": result["하락"],
                    "unchanged_ratio": result["보합"],
                    "date": now_utc(),
                    "included_indices": is_open,
                },
            )
        else:
            # UPDATE
            logging.info("Updating existing S&P 500 data")
            is_open = _is_market_open("sp500")
            logging.info(f"Market status for S&P 500: {'Open' if is_open else 'Closed'}")

            database._update(
                table="stock_indices",
                sets={
                    "rise_ratio": result["상승"],
                    "fall_ratio": result["하락"],
                    "unchanged_ratio": result["보합"],
                    "date": now_utc(),
                    "included_indices": is_open,
                },
                ticker="sp500",
            )

        return result

    except Exception as e:
        logging.error(f"Error in _process_sp500_data: {str(e)}")
        return {"ticker": "sp500", "상승": 0.0, "하락": 0.0, "보합": 0.0}


def run_immediately():
    """
    주가지수 배치 작업을 즉시 실행하는 함수
    """
    logging.info("Starting immediate execution of stock indices batch")
    us_run_stock_indices_batch()
    kr_run_stock_indices_batch()
    logging.info("Completed immediate execution of stock indices batch")


if __name__ == "__main__":
    run_immediately()
