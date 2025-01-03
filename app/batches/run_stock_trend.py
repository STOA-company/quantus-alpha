import datetime
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from app.database.crud import database


def run_stock_trend_tickers_batch():
    """티커 정보 배치 처리"""
    try:
        table_name = "stock_us_tickers"

        # 전체 종목 수 카운트
        total_count = database._count(table=table_name)
        logging.info(f"{table_name} 전체 종목 수: {total_count}개")

        stock_tickers = database._select(
            table=table_name, columns=["ticker", "market", "korean_name", "english_name"], join_info=None
        )
        unique_tickers = {(row.ticker, row.market, row.korean_name, row.english_name) for row in stock_tickers}

        # stock_trend 테이블의 기존 종목 수 카운트
        existing_count = database._count(table="stock_trend")
        logging.info(f"기존 stock_trend 종목 수: {existing_count}개")

        # stock_trend 테이블의 기존 Ticker 조회
        existing_tickers = database._select(table="stock_trend", columns=["ticker"], join_info=None)
        existing_ticker_set = {row.ticker for row in existing_tickers}

        # 새로운 종목 필터링
        new_tickers = [
            {"ticker": ticker, "market": market, "korean_name": korean_name, "english_name": english_name}
            for ticker, market, korean_name, english_name in unique_tickers
            if ticker not in existing_ticker_set
        ]

        if new_tickers:
            logging.info(f"새로운 종목 수: {len(new_tickers)}개")
            database._insert(table="stock_trend", sets=new_tickers)
            logging.info("새로운 종목 추가 완료")
        else:
            logging.info("새로 추가할 종목이 없습니다")

    except Exception as e:
        logging.error(f"Error in run_stock_trend_tickers_batch: {str(e)}")
        raise e


def fetch_latest_stock_data_1d(df):
    """일별 주가 데이터 분석"""
    try:
        # 날짜별로 정렬
        df = df.sort_values(["ticker", "date"], ascending=[True, False])

        # 현재 가격과 이전 종가 구하기
        current_data = df.groupby("ticker").first().reset_index()
        prev_data = df.groupby("ticker").nth(1).reset_index()

        # 결과 데이터프레임 초기화
        results = pd.DataFrame()
        results["ticker"] = current_data["ticker"]
        results["last_updated"] = current_data["date"]
        results["current_price"] = current_data["close"]
        results["prev_close"] = prev_data["close"]
        results["change_1d"] = (current_data["close"] - prev_data["close"]) / prev_data["close"] * 100
        results["volume_1d"] = current_data["volume"]
        results["volume_change_1d"] = current_data["volume"] * current_data["close"]

        # 각 기간별 데이터 계산
        periods = {"1w": 7, "1m": 30, "6m": 180, "1y": 365}

        for period, days in periods.items():
            # 각 티커의 기준 날짜 계산
            cutoff_dates = current_data.set_index("ticker").apply(lambda x: x["date"] - pd.Timedelta(days=days), axis=1)

            # 기간별 데이터 필터링을 위한 조건 생성
            df_period = df.copy()
            df_period["cutoff_date"] = df_period["ticker"].map(cutoff_dates)
            period_data = df_period[df_period["date"] >= df_period["cutoff_date"]]

            # 기간별 시작 가격 찾기
            period_start_prices = period_data.groupby("ticker").last()[["close"]].reset_index()

            # 기간별 거래량 계산
            period_volumes = period_data.groupby("ticker").agg({"volume": "sum"}).reset_index()

            # 결과 데이터프레임에 추가
            results = results.merge(period_start_prices, on="ticker", suffixes=("", f"_start_{period}"))
            results[f"change_{period}"] = (results["current_price"] - results["close"]) / results["close"] * 100
            results = results.drop(columns=["close"])

            # 거래량 데이터 추가
            results = results.merge(period_volumes, on="ticker", suffixes=("", f"_{period}"))
            results[f"volume_{period}"] = results["volume"]
            results[f"volume_change_{period}"] = results["volume"] * results["current_price"]
            results = results.drop(columns=["volume"])

        return results

    except Exception as e:
        logging.error(f"Error fetching daily stock data: {str(e)}")
        return pd.DataFrame()


def fetch_latest_stock_data_1m(engine, df_tickers):
    """실시간 주가 데이터 조회"""
    try:
        # WITH절을 사용하여 각 티커별 최신 데이터 조회
        query = """
            WITH latest_dates AS (
                SELECT ticker, MAX(date) as max_date
                FROM stock_us_1m
                WHERE ticker IN %(tickers)s
                GROUP BY ticker
            )
            SELECT
                s.ticker,
                s.date as last_updated,
                s.close as current_price,
                t.prev_close,
                s.volume as volume_rt
            FROM latest_dates ld
            JOIN stock_us_1m s ON s.ticker = ld.ticker AND s.date = ld.max_date
            LEFT JOIN stock_trend t ON s.ticker = t.ticker
        """

        df = pd.read_sql(query, engine, params={"tickers": tuple(df_tickers["ticker"].tolist())})

        # 변화율 계산
        df["change_rt"] = ((df["current_price"] - df["prev_close"]) / df["prev_close"] * 100).round(4)
        df.loc[df["prev_close"] == 0, "change_rt"] = 0

        # 거래대금 계산
        df["volume_change_rt"] = df["volume_rt"] * df["current_price"]

        # 등락 기호 계산 (2: 상승, 3: 보합, 4: 하락)
        df["change_sign"] = np.where(
            df["current_price"] > df["prev_close"], 2, np.where(df["current_price"] < df["prev_close"], 4, 3)
        )

        return df

    except Exception as e:
        logging.error(f"Error fetching realtime stock data: {e}")
        return pd.DataFrame()


def run_stock_trend_by_1d_batch():
    """일별 주가 트렌드 배치 처리"""
    try:
        current_time = datetime.datetime.now()
        logging.info(f"일별 배치 작업 시작: {current_time}")

        # DB 연결 정보 가져오기
        db_config = database.conn.engine.url
        engine = create_engine(str(db_config))

        # 전체 종목 조회
        table_name = "stock_us_1d"  # 미국 주식 데이터 테이블
        chunk_size = 1000

        # 1. Ticker 조회
        query = """
            SELECT DISTINCT ticker
            FROM stock_us_1d
        """
        df_tickers = pd.read_sql(query, engine)
        logging.info(f"전체 종목 수: {len(df_tickers)}개")

        # 2. 주가 데이터 조회
        all_tickers = "','".join(df_tickers["ticker"].tolist())
        stock_query = f"""
            SELECT ticker, date, close, volume
            FROM {table_name}
            WHERE ticker IN ('{all_tickers}')
                AND date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)
            ORDER BY ticker asc, date desc
        """

        chunks = []
        for chunk_df in pd.read_sql(stock_query, engine, parse_dates=["date"], chunksize=200000):
            chunks.append(chunk_df)

        df = pd.concat(chunks, ignore_index=True)
        logging.info(f"데이터 로드 완료: shape={df.shape}")

        # 3. 데이터 분석
        stock_data = fetch_latest_stock_data_1d(df)
        logging.info(f"데이터 분석 완료: {len(stock_data)}개 종목")

        # 4. 테이블 업데이트
        update_stock_trend_table_1d(engine, stock_data)

        end_time = datetime.datetime.now()
        duration = end_time - current_time
        logging.info(f"일별 배치 작업 완료. 소요 시간: {duration}")

    except Exception as e:
        logging.error(f"Error in run_stock_trend_by_1d_batch: {str(e)}")
        raise e
    finally:
        if "engine" in locals():
            engine.dispose()


def run_stock_trend_by_realtime_batch():
    """실시간 주가 트렌드 배치 처리"""
    try:
        current_time = datetime.datetime.now()
        logging.info(f"실시간 배치 작업 시작: {current_time}")

        # DB 연결 정보 가져오기
        db_config = database.conn.engine.url
        engine = create_engine(str(db_config))

        # 전체 종목 조회
        table_name = "stock_us_1m"  # 미국 주식 1분봉 데이터 테이블

        # 1. Ticker 조회
        query = """
            SELECT DISTINCT ticker
            FROM stock_us_1m
        """
        df_tickers = pd.read_sql(query, engine)
        logging.info(f"전체 종목 수: {len(df_tickers)}개")

        # 2. 최신 주가 데이터 조회 및 계산
        stock_data = fetch_latest_stock_data_1m(engine, df_tickers)
        logging.info(f"데이터 분석 완료: {len(stock_data)}개 종목")

        # 3. 테이블 업데이트
        update_stock_trend_table_1m(engine, stock_data)

        end_time = datetime.datetime.now()
        duration = end_time - current_time
        logging.info(f"실시간 배치 작업 완료. 소요 시간: {duration}")

    except Exception as e:
        logging.error(f"Error in run_stock_trend_by_realtime_batch: {str(e)}")
        raise e
    finally:
        if "engine" in locals():
            engine.dispose()


def update_stock_trend_table_1d(engine, df: pd.DataFrame, chunk_size: int = 1000):
    """일별 stock_trend 테이블 업데이트"""
    try:
        query = """
            INSERT INTO stock_trend (
                ticker, last_updated, current_price, prev_close,
                change_1d, change_1w, change_1m, change_6m, change_1y,
                volume_1d, volume_1w, volume_1m, volume_6m, volume_1y,
                volume_change_1d, volume_change_1w, volume_change_1m,
                volume_change_6m, volume_change_1y
            ) VALUES (
                :ticker, :last_updated, :current_price, :prev_close,
                :change_1d, :change_1w, :change_1m, :change_6m, :change_1y,
                :volume_1d, :volume_1w, :volume_1m, :volume_6m, :volume_1y,
                :volume_change_1d, :volume_change_1w, :volume_change_1m,
                :volume_change_6m, :volume_change_1y
            )
            ON DUPLICATE KEY UPDATE
                last_updated = VALUES(last_updated),
                current_price = VALUES(current_price),
                prev_close = VALUES(prev_close),
                change_1d = VALUES(change_1d),
                change_1w = VALUES(change_1w),
                change_1m = VALUES(change_1m),
                change_6m = VALUES(change_6m),
                change_1y = VALUES(change_1y),
                volume_1d = VALUES(volume_1d),
                volume_1w = VALUES(volume_1w),
                volume_1m = VALUES(volume_1m),
                volume_6m = VALUES(volume_6m),
                volume_1y = VALUES(volume_1y),
                volume_change_1d = VALUES(volume_change_1d),
                volume_change_1w = VALUES(volume_change_1w),
                volume_change_1m = VALUES(volume_change_1m),
                volume_change_6m = VALUES(volume_change_6m),
                volume_change_1y = VALUES(volume_change_1y)
        """

        # 데이터 청크별로 업데이트
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    for _, row in chunk.iterrows():
                        row_dict = row.where(pd.notna(row), None).to_dict()
                        conn.execute(text(query), row_dict)
                    trans.commit()
                    logging.info(f"Updated records {i} to {i+len(chunk)}")
                except Exception as e:
                    trans.rollback()
                    logging.error(f"Error during update of chunk {i}: {e}")
                    raise

        logging.info(f"Successfully updated {len(df)} records in total")

    except Exception as e:
        logging.error(f"Error updating stock trend table: {e}")
        raise


def update_stock_trend_table_1m(engine, df: pd.DataFrame, chunk_size: int = 1000):
    """실시간 stock_trend 테이블 업데이트"""
    try:
        # 활성 티커 업데이트 쿼리
        active_update_query = """
            INSERT INTO stock_trend (
                ticker, last_updated, current_price, prev_close,
                change_sign, change_rt, volume_rt, volume_change_rt
            ) VALUES (
                :ticker, :last_updated, :current_price, :prev_close,
                :change_sign, :change_rt, :volume_rt, :volume_change_rt
            )
            ON DUPLICATE KEY UPDATE
                last_updated = VALUES(last_updated),
                current_price = VALUES(current_price),
                change_sign = VALUES(change_sign),
                change_rt = VALUES(change_rt),
                volume_rt = VALUES(volume_rt),
                volume_change_rt = VALUES(volume_change_rt)
        """

        # 비활성 티커 업데이트 쿼리
        inactive_update_query = """
            UPDATE stock_trend
            SET change_rt = 0,
                volume_rt = 0,
                volume_change_rt = 0
            WHERE ticker NOT IN (
                SELECT DISTINCT ticker
                FROM stock_us_1m
            )
        """

        # 1. 활성 티커 업데이트
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    for _, row in chunk.iterrows():
                        row_dict = row.where(pd.notna(row), None).to_dict()
                        conn.execute(text(active_update_query), row_dict)
                    trans.commit()
                    logging.info(f"Updated active records {i} to {i+len(chunk)}")
                except Exception as e:
                    trans.rollback()
                    logging.error(f"Error during update of chunk {i}: {e}")
                    raise

        # 2. 비활성 티커 업데이트
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                conn.execute(text(inactive_update_query))
                trans.commit()
                logging.info("Updated inactive tickers")
            except Exception as e:
                trans.rollback()
                logging.error(f"Error updating inactive tickers: {e}")
                raise

        logging.info("Successfully updated all records")

    except Exception as e:
        logging.error(f"Error updating stock trend table: {e}")
        raise
    finally:
        if "engine" in locals():
            engine.dispose()
