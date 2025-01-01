import datetime
import logging
from app.database.crud import JoinInfo, database
from datetime import timedelta


def run_stock_trend_tickers_batch():
    try:
        table_name = "stock_kr_1d"
        current_time = datetime.datetime.now()
        logging.info(f"배치 작업 시작: {current_time}")

        # 전체 종목 수 카운트
        total_count = database._count(table=table_name)
        logging.info(f"{table_name} 전체 종목 수: {total_count}개")

        # stock_kr_1d에서 모든 고유 Ticker 조회
        stock_tickers = database._select(table=table_name, columns=["Ticker", "Market"], join_info=None)
        unique_tickers = {(row.Ticker, row.Market) for row in stock_tickers}

        # stock_trend 테이블의 기존 종목 수 카운트
        existing_count = database._count(table="stock_trend")
        logging.info(f"기존 stock_trend 종목 수: {existing_count}개")

        # stock_trend 테이블의 기존 Ticker 조회
        existing_tickers = database._select(table="stock_trend", columns=["ticker"], join_info=None)
        existing_ticker_set = {row.ticker for row in existing_tickers}

        # 새로운 종목 필터링
        new_tickers = [
            {"ticker": ticker, "market": market, "last_updated": current_time}
            for ticker, market in unique_tickers
            if ticker not in existing_ticker_set
        ]

        if new_tickers:
            logging.info(f"새로운 종목 수: {len(new_tickers)}개")
            database._insert(table="stock_trend", sets=new_tickers)
            logging.info("새로운 종목 추가 완료")
        else:
            logging.info("새로 추가할 종목이 없습니다")

        end_time = datetime.datetime.now()
        duration = end_time - current_time
        logging.info(f"배치 작업 완료. 소요 시간: {duration}")

    except Exception as e:
        logging.error(f"Error in run_stock_trend_tickers_batch: {str(e)}")
        raise e


def run_stock_trend_by_realtime_batch():
    """
    실시간 주가 추세 배치 로직
    stock_trend 테이블 업데이트 (1분봉 데이터 기준)
    """
    try:
        current_time = datetime.datetime.now()
        logging.info(f"배치 작업 시작: {current_time}")

        table_name = "stock_us_1m"  # 미국 주식 1분봉 테이블
        logging.info(f"{table_name} 데이터 조회 시작")

        # 최신 데이터와 직전 데이터를 한번에 조회하는 CTE 사용
        query = """
            WITH latest_records AS (
                SELECT
                    ticker,
                    date,
                    close,
                    volume,
                    LAG(close) OVER (PARTITION BY ticker ORDER BY date) as prev_close,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
                FROM stock_us_1m
            )
            SELECT
                ticker,
                date as last_updated,
                close as current_price,
                prev_close,
                CASE
                    WHEN close > prev_close THEN 2  -- 상승
                    WHEN close < prev_close THEN 4  -- 하락
                    ELSE 3  -- 보합
                END as change_sign,
                ((close - prev_close) / prev_close * 100) as change_1m,
                volume as volume_1m,
                (close * volume) as volume_change_1m
            FROM latest_records
            WHERE rn = 1
        """

        results = database._execute(query).fetchall()
        logging.info(f"{table_name} 데이터 조회 완료: {len(results)}개 종목")

        # 청크 단위로 벌크 업데이트
        chunk_size = 1000
        total_chunks = (len(results) + chunk_size - 1) // chunk_size

        for i in range(0, len(results), chunk_size):
            chunk = results[i : i + chunk_size]
            current_chunk = (i // chunk_size) + 1
            logging.info(f"{table_name} 업데이트 진행 중: {current_chunk}/{total_chunks} 청크")

            update_data = [
                {
                    "last_updated": result.last_updated,
                    "current_price": result.current_price,
                    "prev_close": result.prev_close,
                    "change_sign": result.change_sign,
                    "change_1m": result.change_1m,
                    "volume_1m": result.volume_1m,
                    "volume_change_1m": result.volume_change_1m,
                }
                for result in chunk
            ]
            database._bulk_update(table="stock_trend", data=update_data, key_column="ticker")

        logging.info(f"{table_name} 업데이트 완료")

        end_time = datetime.datetime.now()
        duration = end_time - current_time
        logging.info(f"배치 작업 완료. 소요 시간: {duration}")

    except Exception as e:
        logging.error(f"Error in run_stock_trend_by_realtime_batch: {str(e)}")
        raise e


def run_stock_trend_by_1d_batch():
    """
    실시간 주가 추세 배치 로직
    stock_trend 테이블 업데이트
    """
    try:
        current_time = datetime.datetime.now()
        logging.info(f"배치 작업 시작: {current_time}")

        table_name = "stock_kr_1d"
        logging.info(f"{table_name} 데이터 조회 시작")
        results = _get_stock_trend(table_name)
        logging.info(f"{table_name} 데이터 조회 완료: {len(results)}개 종목")

        # 청크 단위로 벌크 업데이트
        chunk_size = 1000
        total_chunks = (len(results) + chunk_size - 1) // chunk_size

        for i in range(0, len(results), chunk_size):
            chunk = results[i : i + chunk_size]
            current_chunk = (i // chunk_size) + 1
            logging.info(f"{table_name} 업데이트 진행 중: {current_chunk}/{total_chunks} 청크")

            update_data = [
                {
                    "last_updated": current_time,
                    "current_price": result["current_price"],
                    "prev_close": result["prev_close"],
                    "change_1d": result["change_1d"],
                    "change_1w": result["change_1w"],
                    "change_1m": result["change_1m"],
                    "change_6m": result["change_6m"],
                    "change_1y": result["change_1y"],
                    "volume_1d": result["volume_1d"],
                    "volume_1w": result["volume_1w"],
                    "volume_1m": result["volume_1m"],
                    "volume_6m": result["volume_6m"],
                    "volume_1y": result["volume_1y"],
                    "volume_change_1d": result["volume_change_1d"],
                    "volume_change_1w": result["volume_change_1w"],
                    "volume_change_1m": result["volume_change_1m"],
                    "volume_change_6m": result["volume_change_6m"],
                    "volume_change_1y": result["volume_change_1y"],
                }
                for result in chunk
            ]
            database._bulk_update(table="stock_trend", data=update_data, key_column="Ticker")

        logging.info(f"{table_name} 업데이트 완료")

        end_time = datetime.datetime.now()
        duration = end_time - current_time
        logging.info(f"배치 작업 완료. 소요 시간: {duration}")

    except Exception as e:
        logging.error(f"Error in run_stock_trend_by_1d_batch: {str(e)}")
        raise e


def _get_stock_trend(table_name: str):
    """
    Database 클래스를 활용하여 주가 추세 데이터 조회
    """
    # 최신 데이터 조회 - group by 대신 서브쿼리 사용
    latest_dates = database._select(table=table_name, columns=["Ticker", "Date"], order="Date", ascending=False)

    # Ticker별 최신 데이터만 필터링
    latest_ticker_dates = {}
    for row in latest_dates:
        if row.Ticker not in latest_ticker_dates:
            latest_ticker_dates[row.Ticker] = row.Date

    # 메인 데이터 조회
    main_data = database._select(
        table=table_name,
        columns=["Ticker", "Date", "Market", "Close", "Volume"],
        join_info=None,
        **{"or__": [{"Ticker": ticker, "Date": date} for ticker, date in latest_ticker_dates.items()]},
    )

    # 전일 데이터 조회를 위한 JoinInfo
    prev_join = JoinInfo(
        primary_table=table_name,
        secondary_table=table_name,
        primary_column="Ticker",
        secondary_column="Ticker",
        columns=["close", "volume"],
        is_outer=True,
        secondary_condition={"Date__lt": main_data[0].Date, "volume__gt": 0},
    )

    # 1주일 전 데이터 조회를 위한 JoinInfo
    week_join = JoinInfo(
        primary_table=table_name,
        secondary_table=table_name,
        primary_column="Ticker",
        secondary_column="Ticker",
        columns=["close", "volume"],
        is_outer=True,
        secondary_condition={"Date__gte": main_data[0].Date - timedelta(days=7), "volume__gt": 0},
    )

    # 1개월 전 데이터 조회를 위한 JoinInfo
    month_1_join = JoinInfo(
        primary_table=table_name,
        secondary_table=table_name,
        primary_column="Ticker",
        secondary_column="Ticker",
        columns=["close", "volume"],
        is_outer=True,
        secondary_condition={"Date__gte": main_data[0].Date - timedelta(days=30), "volume__gt": 0},
    )

    # 6개월 전 데이터 조회를 위한 JoinInfo
    month_6_join = JoinInfo(
        primary_table=table_name,
        secondary_table=table_name,
        primary_column="Ticker",
        secondary_column="Ticker",
        columns=["close", "volume"],
        is_outer=True,
        secondary_condition={"Date__gte": main_data[0].Date - timedelta(days=180), "volume__gt": 0},
    )

    # 1년 전 데이터 조회를 위한 JoinInfo
    year_join = JoinInfo(
        primary_table=table_name,
        secondary_table=table_name,
        primary_column="Ticker",
        secondary_column="Ticker",
        columns=["close", "volume"],
        is_outer=True,
        secondary_condition={"Date__gte": main_data[0].Date - timedelta(days=365), "volume__gt": 0},
    )

    # 최종 데이터 조회
    result = database._select(
        table=table_name,
        columns=["Ticker", "Date", "Market", "Close", "Volume"],
        join_info=[prev_join, week_join, month_1_join, month_6_join, year_join],
    )

    # 결과 데이터 가공
    processed_results = []
    for r in result:
        processed_result = {
            "Ticker": r.Ticker,
            "last_updated": r.Date,
            "market": r.Market,
            "current_price": r.Close,
            "prev_close": r.prev_close,
            "change_sign": 2 if r.Close > r.prev_close else 4 if r.Close < r.prev_close else 3,
            "change_1d": calculate_change(r.Close, r.prev_close),
            "change_1w": calculate_change(r.Close, r.w1_close),
            "change_1m": calculate_change(r.Close, r.mo1_close),
            "change_6m": calculate_change(r.Close, r.mo6_close),
            "change_1y": calculate_change(r.Close, r.y1_close),
            "volume_1d": r.Volume,
            "volume_1w": r.w1_volume,
            "volume_1m": r.mo1_volume,
            "volume_6m": r.mo6_volume,
            "volume_1y": r.y1_volume,
            "volume_change_1d": r.Close * r.Volume,
            "volume_change_1w": r.w1_close * r.w1_volume if r.w1_close and r.w1_volume else 0,
            "volume_change_1m": r.mo1_close * r.mo1_volume if r.mo1_close and r.mo1_volume else 0,
            "volume_change_6m": r.mo6_close * r.mo6_volume if r.mo6_close and r.mo6_volume else 0,
            "volume_change_1y": r.y1_close * r.y1_volume if r.y1_close and r.y1_volume else 0,
        }
        processed_results.append(processed_result)

    return processed_results


def calculate_change(current: float, previous: float) -> float:
    """변화율 계산 함수"""
    if not previous or previous == 0:
        return 0
    return ((current - previous) / previous) * 100


if __name__ == "__main__":
    run_stock_trend_by_1d_batch()
    run_stock_trend_by_realtime_batch()
    run_stock_trend_tickers_batch()
