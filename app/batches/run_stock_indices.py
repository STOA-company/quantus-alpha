import datetime
import logging
import pandas as pd
from sqlalchemy.sql import text
from app.database.crud import database
from app.utils.date_utils import now_utc, check_market_status


def kr_run_stock_indices_batch():
    """
    코스피, 코스닥 지수 데이터 배치 로직
    """
    try:
        current_time = datetime.datetime.now()

        results = [
            _process_market_data("kospi", "KOSPI", "stock_kr_1d"),
            _process_market_data("kosdaq", "KOSDAQ", "stock_kr_1d"),
        ]

        # stock_indices 테이블 업데이트
        for result in results:
            if result:  # None이 아닌 경우에만 처리
                logging.info(f"Updating data for {result['ticker']}")
                _update_market_data(result["ticker"], result)

        logging.info(f"KR market status batch completed at {current_time}")

    except Exception as e:
        logging.error(f"Error in kr_run_stock_indices_batch: {str(e)}")


def us_run_stock_indices_batch():
    """
    나스닥, S&P500 지수 데이터 배치 로직
    """
    try:
        current_time = datetime.datetime.now()

        results = [
            _process_market_data("nasdaq", "NAS", "stock_us_1m"),
            _process_market_data("sp500", None, "stock_us_1m", is_snp_500=True),
        ]

        # stock_indices 테이블 업데이트
        for result in results:
            if result:  # None이 아닌 경우에만 처리
                logging.info(f"Updating data for {result['ticker']}")
                _update_market_data(result["ticker"], result)

        logging.info(f"US market status batch completed at {current_time}")

    except Exception as e:
        logging.error(f"Error in us_run_stock_indices_batch: {str(e)}")


def _process_market_data(ticker: str, market: str = None, price_table: str = None, is_snp_500: bool = False):
    """
    개별 시장 데이터 처리를 위한 공통 함수

    Args:
        ticker (str): 시장 식별자 (kospi, kosdaq, nasdaq, sp500)
        market (str): 시장 구분 (KOSPI, KOSDAQ, NAS)
        price_table (str): 가격 데이터 테이블명
        is_snp_500 (bool): S&P 500 여부
    """
    try:
        query = text(f"""
            WITH filtered_tickers AS (
                SELECT ticker
                FROM stock_information
                WHERE {('is_snp_500 = 1' if is_snp_500 else f"market = '{market}'")}
            ),
            latest_date AS (
                SELECT DATE(MAX(Date)) as max_date
                FROM {price_table} p
                JOIN filtered_tickers ft ON p.Ticker = ft.ticker
            )
            SELECT p.Ticker, p.Open, p.Close
            FROM {price_table} p
            JOIN filtered_tickers ft ON p.Ticker = ft.ticker
            JOIN latest_date ld ON DATE(p.Date) = ld.max_date
        """)

        daily_prices = database._execute(query).fetchall()

        if not daily_prices:
            return {
                "ticker": ticker,
                "rise_ratio": 0.0,
                "rise_soft_ratio": 0.0,
                "fall_ratio": 0.0,
                "fall_soft_ratio": 0.0,
                "unchanged_ratio": 0.0,
            }

        # DataFrame 처리 및 변동률 계산
        result = _calculate_market_ratios(daily_prices, ticker)

        # DB 업데이트
        _update_market_data(ticker, result)

        return result

    except Exception as e:
        logging.error(f"Error in _process_market_data for {ticker}: {str(e)}")
        raise


def _calculate_market_ratios(daily_prices, ticker: str) -> dict:
    """
    시장 데이터로부터 각종 비율 계산
    """
    df = pd.DataFrame(daily_prices, columns=["Ticker", "Open", "Close"])
    df = df.groupby("Ticker").agg({"Open": "first", "Close": "last"}).reset_index()

    df["변동률"] = ((df["Close"] - df["Open"]) / df["Open"]) * 100
    total = len(df)

    strong_rise = len(df[df["변동률"] > 3.0])  # 3% 초과
    weak_rise = len(df[(df["변동률"] <= 3.0) & (df["변동률"] > 0.5)])  # 0.5% ~ 3%
    unchanged = len(df[(df["변동률"] <= 0.5) & (df["변동률"] >= -0.5)])  # -0.5% ~ 0.5%
    weak_fall = len(df[(df["변동률"] < -0.5) & (df["변동률"] >= -3.0)])  # -3% ~ -0.5%
    strong_fall = len(df[df["변동률"] < -3.0])  # -3% 미만

    return {
        "ticker": ticker,
        "rise_ratio": round(strong_rise / total * 100, 2),
        "rise_soft_ratio": round(weak_rise / total * 100, 2),
        "unchanged_ratio": round(unchanged / total * 100, 2),
        "fall_soft_ratio": round(weak_fall / total * 100, 2),
        "fall_ratio": round(strong_fall / total * 100, 2),
    }


def _update_market_data(ticker: str, result: dict):
    """
    시장 데이터 DB 업데이트
    """
    existing_data = database._select(table="stock_indices", columns=["ticker"], ticker=ticker)
    is_open = _is_market_open(ticker)

    sets_data = {
        "ticker": ticker,
        "rise_ratio": result["rise_ratio"],
        "rise_soft_ratio": result["rise_soft_ratio"],
        "unchanged_ratio": result["unchanged_ratio"],
        "fall_soft_ratio": result["fall_soft_ratio"],
        "fall_ratio": result["fall_ratio"],
        "date": now_utc(),
        "included_indices": is_open,
    }

    if not existing_data:
        database._insert(table="stock_indices", sets=sets_data)
    else:
        database._update(table="stock_indices", sets=sets_data, ticker=ticker)


def _is_market_open(ticker: str) -> bool:
    """현재 시간 기준으로 해당 시장이 열렸는지 확인"""
    if ticker in ["nasdaq", "sp500"]:
        return check_market_status("US")
    else:
        return check_market_status("KR")


if __name__ == "__main__":
    logging.info("Starting US market batch job from command line")
    us_run_stock_indices_batch()
