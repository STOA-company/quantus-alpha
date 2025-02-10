import datetime
from functools import wraps
import logging
import time
from typing import Callable
import numpy as np
import pandas as pd
from sqlalchemy.sql import text
from app.database.crud import database
from app.utils.date_utils import now_utc, check_market_status
from app.kispy.api import KISAPI
from app.kispy.sdk import auth


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


#################주가 지수 수집 로직#################
kisapi = KISAPI(auth=auth)


def get_overseas_index_data(ticker: str):
    result = kisapi.get_global_index_minute(ticker)

    # 분봉 데이터를 DataFrame으로 변환
    df = pd.DataFrame(result["output2"])
    df["stck_cntg_hour"] = df["stck_cntg_hour"].astype(int)
    df = df[(df["stck_cntg_hour"] >= 90000) & (df["stck_cntg_hour"] <= 162000)]
    df["stck_cntg_hour"] = df["stck_cntg_hour"].astype(str).str.zfill(6)

    # 컬럼 이름 변경
    df = df.rename(
        columns={
            "optn_prpr": "close",
            "optn_oprc": "open",
            "optn_hgpr": "high",
            "optn_lwpr": "low",
            "cntg_vol": "volume",
            "stck_bsop_date": "date",
            "stck_cntg_hour": "time",
        }
    )

    # API에서 제공하는 변화량과 변동률 사용
    df["change"] = float(result["output1"]["ovrs_nmix_prdy_vrss"])
    if result["output1"]["prdy_vrss_sign"] == "5":  # 하락
        df["change"] = -df["change"]
    df["change_rate"] = float(result["output1"]["prdy_ctrt"])

    # 데이터 타입 변환
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    # 날짜와 시간 처리
    df["date"] = pd.to_datetime(df["date"] + df["time"], format="%Y%m%d%H%M%S")
    # 시간대 변경 (미국 동부 -> utc)
    df["date"] = df["date"].dt.tz_localize("America/New_York").dt.tz_convert("UTC")

    # 필요한 컬럼만 선택하고 DB 저장 형식에 맞게 재구성
    df["ticker"] = ticker
    df = df[["ticker", "date", "open", "high", "low", "close", "volume", "change", "change_rate"]]

    # 기존 데이터 확인
    existing_data = database._select(
        table="stock_indices_1m",
        columns=["ticker", "date"],
        ticker=ticker,
        date__gte=df["date"].min(),
        date__lte=df["date"].max(),
    )

    # 기존 데이터의 (ticker, date) 조합을 set으로 생성
    existing_keys = {(row[0], row[1]) for row in existing_data}

    # 새로운 데이터만 필터링
    new_records = []
    for record in df.to_dict("records"):
        if (record["ticker"], record["date"]) not in existing_keys:
            new_records.append(record)

    # 새로운 데이터만 insert
    if new_records:
        try:
            database._insert(table="stock_indices_1m", sets=new_records)
            logging.info(f"Inserted {len(new_records)} new records for {ticker}")
        except Exception as e:
            logging.error(f"Failed to insert data: {str(e)}")
            raise
    else:
        logging.info(f"No new data to insert for {ticker}")

    return len(df)


def get_domestic_index_data(ticker: str):
    result = kisapi.get_domestic_index_minute(period="1m", market=ticker)
    df = pd.DataFrame(result)

    # 시간을 정수로 변환하여 필터링 (9:00 ~ 15:30)
    df["bsop_hour"] = df["bsop_hour"].astype(int)
    df = df[(df["bsop_hour"] >= 90000) & (df["bsop_hour"] <= 153000)]

    # 부호에 따른 변화량 조정
    df["bstp_nmix_prdy_vrss"] = df["bstp_nmix_prdy_vrss"].astype(float)
    df["bstp_nmix_prdy_vrss"] = np.where(
        df["prdy_vrss_sign"] == "5", -df["bstp_nmix_prdy_vrss"], df["bstp_nmix_prdy_vrss"]
    )

    # 컬럼 이름 변경
    df = df.rename(
        columns={
            "bstp_nmix_prpr": "close",
            "bstp_nmix_prdy_vrss": "change",
            "bstp_nmix_prdy_ctrt": "change_rate",
            "cntg_vol": "volume",
            "bsop_hour": "time",
        }
    )

    # 데이터 타입 변환
    for col in ["close", "change", "change_rate", "volume"]:
        df[col] = df[col].astype(float)

    # OHLC 데이터가 없으므로 0 대체
    df["open"] = 0
    df["high"] = 0
    df["low"] = 0

    today = datetime.datetime.now().strftime("%Y%m%d")

    # 장 시작 시점의 데이터가 있는 경우에만 OHLC 데이터 가져오기
    if len(df) <= 10 and 90000 in df["time"].values:
        open_data = kisapi.get_domestic_index_1d(ticker, today)
        df.loc[df["time"] == 90000, "open"] = float(open_data["output1"]["bstp_nmix_oprc"])
        df.loc[df["time"] == 90000, "high"] = float(open_data["output1"]["bstp_nmix_hgpr"])
        df.loc[df["time"] == 90000, "low"] = float(open_data["output1"]["bstp_nmix_lwpr"])

    # 날짜와 시간 처리
    df["time"] = df["time"].astype(str).str.zfill(6)
    df["date"] = pd.to_datetime(today + df["time"].str[:4], format="%Y%m%d%H%M")
    df["date"] = df["date"].dt.tz_localize("Asia/Seoul").dt.tz_convert("UTC")

    # 필요한 컬럼만 선택하고 DB 저장 형식에 맞게 재구성
    df["ticker"] = ticker
    df = df[["ticker", "date", "open", "high", "low", "close", "volume", "change", "change_rate"]]

    # 기존 데이터 확인
    existing_data = database._select(
        table="stock_indices_1m",
        columns=["ticker", "date"],
        ticker=ticker,
        date__gte=df["date"].min(),
        date__lte=df["date"].max(),
    )

    # 기존 데이터의 (ticker, date) 조합을 set으로 생성
    existing_keys = {(row[0], row[1].strftime("%Y-%m-%d %H:%M:%S")) for row in existing_data}

    # 새로운 데이터만 필터링
    new_records = []
    for record in df.to_dict("records"):
        key = (record["ticker"], record["date"].strftime("%Y-%m-%d %H:%M:%S"))
        if key not in existing_keys:
            new_records.append(record)

    # 새로운 데이터만 insert
    if new_records:
        try:
            database._insert(table="stock_indices_1m", sets=new_records)
            logging.info(f"Inserted {len(new_records)} new records for {ticker}")
        except Exception as e:
            logging.error(f"Failed to insert data: {str(e)}")
            raise
    else:
        logging.info(f"No new data to insert for {ticker}")

    return len(df)


def retry_on_rate_limit(max_retries: int = 3, retry_delay: int = 60) -> Callable:
    """
    API 유량 제한에 대한 재시도 데코레이터

    Args:
        max_retries (int): 최대 재시도 횟수
        retry_delay (int): 재시도 간 대기 시간(초)
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        logging.error(f"Max retries ({max_retries}) exceeded: {str(e)}")
                        raise

                    logging.warning(
                        f"Rate limit exceeded. Retrying in {retry_delay} seconds... " f"(Attempt {retries}/{max_retries})"
                    )
                    time.sleep(retry_delay)
            return None

        return wrapper

    return decorator


@retry_on_rate_limit(max_retries=3, retry_delay=10)
def get_stock_indices_data(ticker: str):
    if ticker in ["NASDAQ", "SNP500"]:
        return get_overseas_index_data(ticker)

    elif ticker in ["KOSPI", "KOSDAQ"]:
        return get_domestic_index_data(ticker)
    else:
        raise ValueError(f"Invalid ticker: {ticker}")


def _is_market_open(ticker: str) -> bool:
    """현재 시간 기준으로 해당 시장이 열렸는지 확인"""
    if ticker in ["nasdaq", "sp500"]:
        return check_market_status("US")
    else:
        return check_market_status("KR")


# if __name__ == "__main__":
# logging.info("Starting US market batch job from command line")
# kr_run_stock_indices_batch()

# get_stock_indices_data("KOSPI")
# get_stock_indices_data("KOSDAQ")
