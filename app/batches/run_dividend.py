import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from app.common.constants import ETF_DATA_DIR
from app.database.crud import database
from app.kispy.api import KISAPI
from app.kispy.sdk import auth
from app.modules.screener.etf.utils import ETFDataDownloader

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def process_dividend_data(ctry: str, type: str):
    """
    배당금 데이터를 처리하고 데이터프레임을 반환합니다.

    Args:
        ctry (str): 국가코드 (US, KR)
        type (str): 자산 타입 (stock, etf)

    Returns:
        tuple: (df_dividend, contry) - 전처리된 배당 데이터프레임과 국가코드
    """
    if ctry == "KR":
        contry = "kr"
    elif ctry == "US":
        contry = "us"
    else:
        raise ValueError("ctry must be US or KR")

    if type not in ["stock", "etf"]:
        raise ValueError("type must be stock or etf")

    # parquet 파일 읽기
    df_dividend = pd.read_parquet(os.path.join(ETF_DATA_DIR, f"{contry}_{type}_dividend.parquet"))

    # 과거 배당금 데이터 전처리
    df_dividend = df_dividend.rename(
        columns={
            "Ticker": "ticker",
            "배당금": "per_share",
            "배당지급일": "payment_date",
            "배당락일": "ex_date",
            "Per Share": "per_share",
        }
    )
    df_dividend["ex_date"] = pd.to_datetime(df_dividend["ex_date"])
    df_dividend["payment_date"] = pd.to_datetime(df_dividend["payment_date"])

    # 한국 티커의 경우 'K'를 'A'로 변경
    if ctry == "KR":
        df_dividend["ticker"] = df_dividend["ticker"].apply(lambda x: "A" + x[1:] if x.startswith("K") else x)

    # adj_factor가 없으면 1로 설정
    if "adj_factor" not in df_dividend.columns:
        df_dividend["adj_factor"] = 1.0
    else:
        # NaN 값을 1.0으로 설정
        df_dividend["adj_factor"] = df_dividend["adj_factor"].fillna(1.0)

    # 유효한 ticker 필터링
    information_tickers = database._select(
        table="stock_information",
        columns=["ticker"],
        type=type,
    )
    list_information_tickers = [ticker[0] for ticker in information_tickers]
    df_dividend = df_dividend[df_dividend["ticker"].isin(list_information_tickers)]
    df_dividend = df_dividend.drop_duplicates()

    # Special 배당금 처리
    if "dividend_type_desc" in df_dividend.columns:
        df_dividend = (
            df_dividend.groupby(["ticker", "payment_date", "ex_date"])
            .agg(
                {
                    "per_share": "sum",
                    "adj_factor": "first",  # adj_factor 컬럼 유지
                }
            )
            .reset_index()
        )

    return df_dividend, contry


def get_all_price_data(df_dividend, contry, type):
    """
    모든 필요한 가격 데이터를 한 번에 가져오는 함수

    Args:
        df_dividend (DataFrame): 배당금 데이터프레임
        contry (str): 국가 코드
        type (str): 자산 유형

    Returns:
        DataFrame: 티커와 날짜에 대한 가격 데이터
    """
    # 필요한 모든 티커와 날짜 조합 수집
    all_tickers = df_dividend["ticker"].unique().tolist()
    all_dates = df_dividend["ex_date"].dt.strftime("%Y-%m-%d").unique().tolist()

    # 필요한 모든 가격 데이터를 한 번에 가져옴
    all_price_data = database._select(
        table=f"{type}_{contry}_1d", columns=["Ticker", "Date", "Close"], Ticker__in=all_tickers, Date__in=all_dates
    )

    # 데이터프레임 변환 및 정리
    df_prices = pd.DataFrame(all_price_data, columns=["ticker", "ex_date", "price"])

    return df_prices


def process_ticker_batch(
    ticker_batch, df_dividend, df_prices, existing_records_dict, table, action="insert", max_batch_size=1000
):
    """
    티커 배치를 처리하는 함수 (삽입 또는 업데이트)

    Args:
        ticker_batch (list): 처리할 티커 목록
        df_dividend (DataFrame): 배당금 데이터
        df_prices (DataFrame): 가격 데이터
        existing_records_dict (dict): 기존 레코드 정보
        table (str): 데이터베이스 테이블
        action (str): "insert" 또는 "update"
        max_batch_size (int): 최대 배치 크기

    Returns:
        int: 처리된 레코드 수
    """
    records_processed = 0
    batch_data = []

    for ticker in ticker_batch:
        # 해당 티커의 배당 데이터 필터링
        df_ticker = df_dividend[df_dividend["ticker"] == ticker]

        # 업데이트인 경우 adj_factor가 1이 아닌 레코드만 필터링
        if action == "update":
            df_ticker = df_ticker[abs(df_ticker["adj_factor"] - 1.0) > 0.00001]
            if df_ticker.empty:
                continue

        # 해당 티커의 가격 데이터 가져오기
        df_ticker_prices = df_prices[df_prices["ticker"] == ticker]

        # 배당금 데이터와 가격 데이터 병합
        ticker_data = pd.merge(df_ticker, df_ticker_prices, on=["ticker", "ex_date"], how="left")

        # 수익률 계산
        ticker_data["yield_rate"] = round((ticker_data["per_share"] / ticker_data["price"]) * 100, 2)

        # 각 행 처리
        for _, row in ticker_data.iterrows():
            payment_date_str = row["payment_date"].strftime("%Y-%m-%d") if pd.notna(row["payment_date"]) else None
            ex_date_str = row["ex_date"].strftime("%Y-%m-%d")

            key = (row["ticker"], ex_date_str, payment_date_str)

            # 값 준비
            per_share_val = float(row["per_share"]) if pd.notna(row["per_share"]) else None
            yield_rate_val = float(row["yield_rate"]) if pd.notna(row["yield_rate"]) else None

            # 기본 레코드 데이터
            record = {
                "ticker": row["ticker"],
                "payment_date": payment_date_str,
                "ex_date": ex_date_str,
                "per_share": per_share_val,
                "yield_rate": yield_rate_val,
            }

            # 삽입 또는 업데이트 로직
            if action == "insert":
                # DB에 없는 데이터만 삽입
                if key not in existing_records_dict:
                    batch_data.append(record)
                    records_processed += 1
            else:  # update
                # 기존 DB에 데이터가 있는 경우 값 비교
                if key in existing_records_dict:
                    old_values = existing_records_dict[key]
                    old_per_share = old_values.get("per_share")
                    new_per_share = per_share_val

                    old_yield_rate = old_values.get("yield_rate")
                    new_yield_rate = yield_rate_val

                    # 값이 다른지 비교
                    per_share_changed = (
                        (old_per_share is None and new_per_share is not None)
                        or (old_per_share is not None and new_per_share is None)
                        or (
                            old_per_share is not None
                            and new_per_share is not None
                            and abs(old_per_share - new_per_share) > 0.00001
                        )
                    )

                    yield_rate_changed = (
                        (old_yield_rate is None and new_yield_rate is not None)
                        or (old_yield_rate is not None and new_yield_rate is None)
                        or (
                            old_yield_rate is not None
                            and new_yield_rate is not None
                            and abs(old_yield_rate - new_yield_rate) > 0.00001
                        )
                    )

                    if per_share_changed or yield_rate_changed:
                        batch_data.append(record)
                        records_processed += 1

            # 배치 처리
            if len(batch_data) >= max_batch_size:
                try:
                    if action == "insert":
                        database._insert(table=table, sets=batch_data)
                    else:  # update
                        for record in batch_data:
                            database._update(
                                table=table,
                                sets={"per_share": record["per_share"], "yield_rate": record["yield_rate"]},
                                ticker=record["ticker"],
                                ex_date=record["ex_date"],
                                payment_date=record["payment_date"],
                            )
                except Exception as e:
                    logger.error(f"Error processing batch: {e}")
                batch_data = []

    # 남은 데이터 처리
    if batch_data:
        try:
            if action == "insert":
                database._insert(table=table, sets=batch_data)
            else:  # update
                for record in batch_data:
                    database._update(
                        table=table,
                        sets={"per_share": record["per_share"], "yield_rate": record["yield_rate"]},
                        ticker=record["ticker"],
                        ex_date=record["ex_date"],
                        payment_date=record["payment_date"],
                    )
        except Exception as e:
            logger.error(f"Error processing final batch: {e}")

    return records_processed


def insert_dividend_records(ctry: str, type: str, max_workers=4):
    """
    배당금 데이터 중 DB에 없는 데이터를 삽입합니다. (병렬 처리 최적화 버전)

    Args:
        ctry (str): 국가코드 (US, KR)
        type (str): 자산 타입 (stock, etf)
        max_workers (int): 동시 작업자 수
    """
    logger.info(f"Starting insert_dividend_records for {ctry} {type}")
    start_time = time.time()

    # 데이터 준비
    df_dividend, contry = process_dividend_data(ctry, type)
    tickers = df_dividend["ticker"].unique().tolist()
    total_tickers = len(tickers)

    logger.info(f"Processing {total_tickers} tickers for insertion")

    # 필요한 ex_date 목록 생성
    all_ex_dates = df_dividend["ex_date"].dt.strftime("%Y-%m-%d").tolist()

    # 기존 레코드 한 번에 조회
    logger.info("Fetching existing records...")
    existing_records = database._select(
        table="dividend_information",
        columns=["ticker", "ex_date", "payment_date"],
        ticker__in=tickers,
        ex_date__in=all_ex_dates,
    )

    # 중복 체크를 위한 dictionary 생성
    existing_records_dict = {}
    for record in existing_records:
        ticker, ex_date, payment_date = record
        key = (ticker, ex_date.strftime("%Y-%m-%d"), payment_date.strftime("%Y-%m-%d") if payment_date else None)
        existing_records_dict[key] = {}

    logger.info(f"Found {len(existing_records_dict)} existing records")

    # 모든 필요한 가격 데이터 한 번에 가져오기
    logger.info("Fetching all price data...")
    df_prices = get_all_price_data(df_dividend, contry, type)

    # 티커를 작업자 수에 맞게 배치로 나누기
    batch_size = max(1, len(tickers) // max_workers)
    ticker_batches = [tickers[i : i + batch_size] for i in range(0, len(tickers), batch_size)]

    # 병렬 처리
    total_inserted = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for batch in ticker_batches:
            future = executor.submit(
                process_ticker_batch,
                batch,
                df_dividend,
                df_prices,
                existing_records_dict,
                "dividend_information",
                "insert",
            )
            futures.append(future)

        for future in as_completed(futures):
            try:
                total_inserted += future.result()
            except Exception as e:
                logger.error(f"Error in worker thread: {e}")

    elapsed_time = time.time() - start_time
    logger.info(f"Inserted {total_inserted} records in {elapsed_time:.2f} seconds")


def update_dividend_records(ctry: str, type: str, max_workers=4):
    """
    배당금 데이터 중 adj_factor가 1이 아닌 데이터에 대해 값을 업데이트합니다. (병렬 처리 최적화 버전)

    Args:
        ctry (str): 국가코드 (US, KR)
        type (str): 자산 타입 (stock, etf)
        max_workers (int): 동시 작업자 수
    """
    logger.info(f"Starting update_dividend_records for {ctry} {type}")
    start_time = time.time()

    # 데이터 준비
    df_dividend, contry = process_dividend_data(ctry, type)

    # adj_factor가 1이 아닌 레코드만 필터링
    df_dividend_filtered = df_dividend[abs(df_dividend["adj_factor"] - 1.0) > 0.00001]
    tickers = df_dividend_filtered["ticker"].unique().tolist()
    total_tickers = len(tickers)
    total_records = len(df_dividend_filtered)

    if not tickers:
        logger.info("No records to update")
        return

    logger.info(f"Processing {total_tickers} tickers with {total_records} potential records for updates")

    # 필요한 ex_date 목록 생성
    all_ex_dates = df_dividend_filtered["ex_date"].dt.strftime("%Y-%m-%d").tolist()

    # 기존 레코드 한 번에 조회
    logger.info("Fetching existing records...")
    existing_records = database._select(
        table="dividend_information",
        columns=["ticker", "ex_date", "payment_date", "per_share", "yield_rate"],
        ticker__in=tickers,
        ex_date__in=all_ex_dates,
    )

    # 중복 체크를 위한 dictionary 생성
    existing_records_dict = {}
    for record in existing_records:
        ticker, ex_date, payment_date, per_share, yield_rate = record
        key = (ticker, ex_date.strftime("%Y-%m-%d"), payment_date.strftime("%Y-%m-%d") if payment_date else None)
        existing_records_dict[key] = {
            "per_share": float(per_share) if per_share is not None else None,
            "yield_rate": float(yield_rate) if yield_rate is not None else None,
        }

    logger.info(f"Found {len(existing_records_dict)} existing records to check for updates")

    # 모든 필요한 가격 데이터 한 번에 가져오기
    logger.info("Fetching all price data...")
    df_prices = get_all_price_data(df_dividend_filtered, contry, type)

    # 데이터 처리를 위한 함수 정의
    def calculate_values(df_dividend, df_prices):
        """
        배당금 및 수익률 값을 계산하여 반환하는 함수
        정확한 문자열 형식의 값으로 반환하여 정밀도 문제를 해결
        """
        # 데이터프레임 병합 및 계산
        merged_data = []

        # 티커 별로 처리
        for ticker in df_dividend["ticker"].unique():
            df_ticker = df_dividend[df_dividend["ticker"] == ticker]
            df_ticker_prices = df_prices[df_prices["ticker"] == ticker]

            # 배당금 데이터와 가격 데이터 병합
            ticker_data = pd.merge(df_ticker, df_ticker_prices, on=["ticker", "ex_date"], how="left")

            # 수익률 계산
            for _, row in ticker_data.iterrows():
                payment_date_str = row["payment_date"].strftime("%Y-%m-%d") if pd.notna(row["payment_date"]) else None
                ex_date_str = row["ex_date"].strftime("%Y-%m-%d")

                # per_share 및 yield_rate 계산
                per_share = float(row["per_share"]) if pd.notna(row["per_share"]) else None

                # 수익률 계산 (정밀한 계산을 위해 문자열로 변환 전 계산)
                if pd.notna(row["per_share"]) and pd.notna(row["price"]) and row["price"] > 0:
                    yield_rate = round((row["per_share"] / row["price"]) * 100, 2)
                else:
                    yield_rate = None

                # 키 및 레코드 데이터
                key = (row["ticker"], ex_date_str, payment_date_str)

                # 계산된 값을 문자열로 정확히 표현 (정밀도 문제 해결)
                per_share_str = f"{per_share:.8f}" if per_share is not None else None
                yield_rate_str = f"{yield_rate:.8f}" if yield_rate is not None else None

                merged_data.append(
                    {
                        "key": key,
                        "per_share": per_share,
                        "yield_rate": yield_rate,
                        "per_share_str": per_share_str,
                        "yield_rate_str": yield_rate_str,
                    }
                )

        return merged_data

    # 계산된 최신 값 얻기
    calculated_data = calculate_values(df_dividend_filtered, df_prices)

    # 해시 기반 업데이트 접근방식
    # 각 레코드를 해시로 변환하여 변경 사항이 실제로 있는지 비교
    def get_exact_hash(per_share, yield_rate):
        """
        부동소수점 값을 정확히 비교하기 위한 해시 생성 함수
        """
        per_share_str = f"{per_share:.8f}" if per_share is not None else "NULL"
        yield_rate_str = f"{yield_rate:.8f}" if yield_rate is not None else "NULL"
        return f"{per_share_str}_{yield_rate_str}"

    # 업데이트가 필요한 레코드만 필터링
    records_to_update = []
    records_checked = 0

    for item in calculated_data:
        key = item["key"]
        records_checked += 1

        if key in existing_records_dict:
            old_values = existing_records_dict[key]
            old_per_share = old_values.get("per_share")
            old_yield_rate = old_values.get("yield_rate")

            new_per_share = item["per_share"]
            new_yield_rate = item["yield_rate"]

            # 해시 기반 비교 - 더 정확한 비교
            old_hash = get_exact_hash(old_per_share, old_yield_rate)
            new_hash = get_exact_hash(new_per_share, new_yield_rate)

            if old_hash != new_hash:
                # 업데이트 필요
                record = {
                    "ticker": key[0],
                    "ex_date": key[1],
                    "payment_date": key[2],
                    "per_share": new_per_share,
                    "yield_rate": new_yield_rate,
                }
                records_to_update.append(record)

    logger.info(f"Found {len(records_to_update)} records requiring updates out of {records_checked} checked")

    # 병렬 처리 함수
    def update_records_batch(batch):
        """배치 단위로 레코드 업데이트"""
        updated = 0
        try:
            for record in batch:
                database._update(
                    table="dividend_information",
                    sets={"per_share": record["per_share"], "yield_rate": record["yield_rate"]},
                    ticker=record["ticker"],
                    ex_date=record["ex_date"],
                    payment_date=record["payment_date"],
                )
                updated += 1
        except Exception as e:
            logger.error(f"Error updating batch: {e}")
        return updated

    # 배치 업데이트
    max_batch_size = 500
    batches = [records_to_update[i : i + max_batch_size] for i in range(0, len(records_to_update), max_batch_size)]

    total_updated = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for batch in batches:
            future = executor.submit(update_records_batch, batch)
            futures.append(future)

        for future in as_completed(futures):
            try:
                total_updated += future.result()
            except Exception as e:
                logger.error(f"Error in worker thread: {e}")

    elapsed_time = time.time() - start_time
    logger.info(f"Checked {records_checked} records, Updated {total_updated} records in {elapsed_time:.2f} seconds")

    # 업데이트가 없는 경우에 대한 로그
    if total_updated == 0:
        logger.info("No records needed updates. All values were already up-to-date.")


def insert_dividend(ctry: str, type: str, max_workers=4):
    """
    배당금 데이터를 삽입하고 업데이트합니다.
    (기존 함수와의 호환성을 위해 유지)

    Args:
        ctry (str): 국가코드 (US, KR)
        type (str): 자산 타입 (stock, etf)
        max_workers (int): 병렬 처리를 위한 최대 작업자 수
    """
    logger.info(f"Starting dividend data processing for {ctry} {type}")
    total_start_time = time.time()

    insert_dividend_records(ctry, type, max_workers)
    update_dividend_records(ctry, type, max_workers)

    total_elapsed_time = time.time() - total_start_time
    logger.info(f"Completed all dividend processing in {total_elapsed_time:.2f} seconds")


def get_etf_top_constituents(etf_ticker: str) -> list[dict]:  # kis대신 krx를 사용 # 현재 사용안하는 함수
    """
    ETF의 상위 100개 구성종목 정보를 가져옵니다.

    Args:
        etf_ticker (str): ETF 종목코드

    Returns:
        list[dict]: 상위 100개 구성종목 정보 리스트
    """
    api = KISAPI(auth)
    result = api.get_etf_constituents(etf_ticker)

    if not result.get("constituents"):
        print(f"Failed to fetch constituents for ETF {etf_ticker}")
        return []

    # Sort constituents by weight in descending order
    sorted_constituents = sorted(result["constituents"], key=lambda x: x["weight"], reverse=True)

    db_data = []
    for stock in sorted_constituents:
        db_data.append({"etf_ticker": etf_ticker, "constituent_ticker": stock["ticker"], "weight": stock["weight"]})

    return db_data


def run_update_etf_constituents(ctry: str):  # kis대신 krx를 사용 # 현재 사용안하는 함수
    if ctry not in ["US", "KR"]:
        raise ValueError("ctry must be either 'US' or 'KR'")

    etf_tickers = database._select(table="stock_information", columns=["ticker"], ctry=ctry, type="etf")
    if ctry == "KR":
        etf_tickers = [ticker[1:] for ticker in etf_tickers]
    else:
        etf_tickers = [ticker for ticker in etf_tickers]

    api = KISAPI(auth)
    for etf_ticker in etf_tickers:
        result = api.get_etf_constituents(etf_ticker)

    if not result.get("constituents"):
        print("Failed to fetch ETF constituents")
        return

    # Sort constituents by weight in descending order
    sorted_constituents = sorted(result["constituents"], key=lambda x: x["weight"], reverse=True)
    etf_constituents = []
    # Get top 10 holdings
    top_10 = sorted_constituents[:10]
    for stock in top_10:
        etf_constituents.append(
            {
                "etf_ticker": etf_ticker,
                "ticker": stock["ticker"],
                "name": stock["name"],
                "weight": stock["weight"],
            }
        )


class StockDividendDataDownloader(ETFDataDownloader):
    def __init__(self):
        super().__init__()

    def download_stock_dividend(self, ctry: str, download: bool = False):
        """
        주식 배당 데이터 다운로드

        Args:
            ctry (str): 국가코드 (US, KR)

        Returns:
            pd.DataFrame: 데이터프레임
        """
        if ctry not in ["US", "KR"]:
            raise ValueError("ctry must be 'US' or 'KR'")
        if ctry == "US":
            country = "us"
            query = """
            WITH DSINFO AS (
                SELECT infocode
                FROM DS2CtryQtInfo
                WHERE Region = 'us'
                AND StatusCode IN ('A', 'S')
                AND TypeCode = 'EQ'  -- ET :ETF, EQ: 주식
            ),
            DSSUM AS (
                SELECT D.INFOCODE,
                    D.EFFECTIVEDATE,
                    D.PayDate,
                    SUM(D.DIVRATE) AS DSSUM
                FROM DS2DIV D WITH (INDEX(DS2Div_1))
                INNER JOIN DSINFO I ON D.INFOCODE = I.INFOCODE
                GROUP BY D.INFOCODE, D.EFFECTIVEDATE, D.PayDate
            ),
            DSSUMADJ AS (
                SELECT A.INFOCODE,
                    A.ADJDATE,
                    A.CUMADJFACTOR,
                    C.EFFECTIVEDATE,
                    C.DSSUM AS UNADJ_DIV,
                    C.PayDate,
                    ROW_NUMBER() OVER(PARTITION BY A.INFOCODE ORDER BY A.ADJDATE) AS RN
                FROM DS2ADJ A WITH (INDEX(DS2Adj_1))
                INNER JOIN DSSUM C ON C.INFOCODE = A.INFOCODE
                                AND C.EFFECTIVEDATE BETWEEN A.ADJDATE AND ISNULL(A.ENDADJDATE, GETDATE())
                WHERE A.ADJDATE BETWEEN '1960-01-01' AND GETDATE()
                AND A.ADJTYPE = '2'
            ),
            LatestTicker AS (
                SELECT
                    InfoCode,
                    Ticker,
                    ROW_NUMBER() OVER (PARTITION BY InfoCode ORDER BY EndDate DESC) AS rn
                FROM Ds2MnemChg
            )
            SELECT
                T.Ticker as 'ticker',
                A.EFFECTIVEDATE as 'ex_date',
                A.UNADJ_DIV,
                A.PayDate as 'payment_date',
                CASE
                    WHEN A.EFFECTIVEDATE <> A.ADJDATE THEN A.CUMADJFACTOR
                    ELSE B.CUMADJFACTOR
                END as 'adj_factor',
                CASE
                    WHEN A.EFFECTIVEDATE <> A.ADJDATE THEN A.CUMADJFACTOR * A.UNADJ_DIV
                    ELSE A.UNADJ_DIV * B.CUMADJFACTOR
                END AS 'per_share'
            FROM DSSUMADJ AS A
            LEFT OUTER JOIN DSSUMADJ AS B ON A.INFOCODE = B.INFOCODE AND A.RN - 1 = B.RN
            LEFT OUTER JOIN LatestTicker T ON T.InfoCode = A.INFOCODE AND T.rn = 1
            WHERE A.EFFECTIVEDATE IS NOT NULL
            ORDER BY T.Ticker, A.EFFECTIVEDATE;
            """

        if ctry == "KR":
            country = "kr"
            query = """
            WITH DSINFO AS (
                SELECT infocode, DsLocalCode
                FROM DS2CtryQtInfo
                WHERE Region = 'kr'
                AND StatusCode IN ('A', 'S')
                AND TypeCode = 'EQ'
            ),
            DSSUM AS (
                SELECT D.INFOCODE,
                    D.EFFECTIVEDATE,
                    D.PayDate,
                    SUM(D.DIVRATE) AS DSSUM
                FROM DS2DIV D WITH (INDEX(DS2Div_1))
                INNER JOIN DSINFO I ON D.INFOCODE = I.INFOCODE
                GROUP BY D.INFOCODE, D.EFFECTIVEDATE, D.PayDate
            ),
            DSSUMADJ AS (
                SELECT A.INFOCODE,
                    A.ADJDATE,
                    A.CUMADJFACTOR,
                    C.EFFECTIVEDATE,
                    C.DSSUM AS UNADJ_DIV,
                    C.PayDate,
                    ROW_NUMBER() OVER(PARTITION BY A.INFOCODE ORDER BY A.ADJDATE) AS RN
                FROM DS2ADJ A WITH (INDEX(DS2Adj_1))
                INNER JOIN DSSUM C ON C.INFOCODE = A.INFOCODE
                                AND C.EFFECTIVEDATE BETWEEN A.ADJDATE AND ISNULL(A.ENDADJDATE, GETDATE())
                WHERE A.ADJDATE BETWEEN '1960-01-01' AND GETDATE()
                AND A.ADJTYPE = '2'
            ),
            LatestTicker AS (
                SELECT
                    InfoCode,
                    Ticker,
                    ROW_NUMBER() OVER (PARTITION BY InfoCode ORDER BY EndDate DESC) AS rn
                FROM Ds2MnemChg
            )
            SELECT
                C.DsLocalCode as 'ticker',
                A.EFFECTIVEDATE as 'ex_date',
                A.UNADJ_DIV,
                A.PayDate as 'payment_date',
                CASE
                    WHEN A.EFFECTIVEDATE <> A.ADJDATE THEN A.CUMADJFACTOR
                    ELSE B.CUMADJFACTOR
                END as 'adj_factor',
                CASE
                    WHEN A.EFFECTIVEDATE <> A.ADJDATE THEN A.CUMADJFACTOR * A.UNADJ_DIV
                    ELSE A.UNADJ_DIV * B.CUMADJFACTOR
                END AS 'per_share'
            FROM DSSUMADJ AS A
            LEFT OUTER JOIN DSSUMADJ AS B ON A.INFOCODE = B.INFOCODE AND A.RN - 1 = B.RN
            LEFT OUTER JOIN DSINFO AS C ON A.INFOCODE = C.INFOCODE
            LEFT OUTER JOIN LatestTicker T ON T.InfoCode = A.INFOCODE AND T.rn = 1
            WHERE A.EFFECTIVEDATE IS NOT NULL
            ORDER BY T.Ticker, A.EFFECTIVEDATE;
            """
        df = self._get_refinitiv_data(query)

        list_db_tickers = self._get_db_tickers_list(ctry=country, type="stock")

        if ctry == "KR":
            list_db_tickers = [self.kr_pattern.sub("K", ticker) for ticker in list_db_tickers]
        df = df[df["ticker"].isin(list_db_tickers)]

        if download:
            if ctry == "KR":
                df.to_parquet(os.path.join(self.DATA_DIR, "kr_stock_dividend.parquet"), index=False)
            elif ctry == "US":
                df.to_parquet(os.path.join(self.DATA_DIR, "us_stock_dividend.parquet"), index=False)
        return df


if __name__ == "__main__":
    insert_dividend(ctry="KR", type="stock")
    # downloader = StockDividendDataDownloader()
    # downloader.download_stock_dividend(ctry="US", download=True)
    # downloader.download_stock_dividend(ctry="KR", download=True)
