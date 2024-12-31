import datetime
import logging
from app.database.crud import JoinInfo, database


def run_stock_trend_realtime_batch():
    """
    실시간 주가 추세 배치 로직
    stock_trend 테이블 업데이트
    """
    try:
        current_time = datetime.datetime.now()

        # 한국/미국 주식 데이터 조회
        for table_name in ["stock_kr_1d", "stock_us_1d"]:
            results = _get_stock_trend(table_name)

            # 벌크 업데이트를 위한 데이터 준비
            update_data = [
                {
                    "ticker": result["ticker"],
                    "last_updated": current_time,
                    "market": result["market"],
                    "current_price": result["current_price"],
                    "prev_close": result["prev_close"],
                    "change_sign": result["change_sign"],
                    "change_1d": result["change_1d"],
                    "change_1w": result["change_1w"],
                    "change_1mo": result["change_1mo"],
                    "change_6mo": result["change_6mo"],
                    "change_1y": result["change_1y"],
                    "volume_1d": result["volume_1d"],
                    "volume_1w": result["volume_1w"],
                    "volume_1mo": result["volume_1mo"],
                    "volume_6mo": result["volume_6mo"],
                    "volume_1y": result["volume_1y"],
                    "volume_change_1d": result["volume_change_1d"],
                    "volume_change_1w": result["volume_change_1w"],
                    "volume_change_1mo": result["volume_change_1mo"],
                    "volume_change_6mo": result["volume_change_6mo"],
                    "volume_change_1y": result["volume_change_1y"],
                }
                for result in results
            ]

            # 벌크 업데이트 실행
            database._bulk_update(table="stock_trend", data=update_data, key_column="ticker")

        logging.info(f"Stock trend batch completed at {current_time}")

    except Exception as e:
        logging.error(f"Error in run_stock_trend_realtime_batch: {str(e)}")
        raise e


def _get_stock_trend(table_name: str):
    """
    Database 클래스를 활용하여 주가 추세 데이터 조회
    """
    # 최신 데이터 조회를 위한 서브쿼리
    # latest_dates = database._select(
    #     table=table_name, columns=["ticker", "date"], join_info=None, order="date", ascending=False
    # )

    # 메인 데이터 조회
    main_data = database._select(
        table=table_name, columns=["ticker", "date", "market", "close", "volume"], join_info=None
    )

    # 전일 데이터 조회를 위한 JoinInfo
    prev_join = JoinInfo(
        primary_table=table_name,
        secondary_table=table_name,
        primary_column="ticker",
        secondary_column="ticker",
        columns=["close", "volume"],
        is_outer=True,
        secondary_condition={"date__lt": main_data.date},
    )

    # 1주일 전 데이터 조회를 위한 JoinInfo
    week_join = JoinInfo(
        primary_table=table_name,
        secondary_table=table_name,
        primary_column="ticker",
        secondary_column="ticker",
        columns=["close", "volume"],
        is_outer=True,
        secondary_condition={"date__lte": f"DATE_SUB({main_data.date}, INTERVAL 1 WEEK)"},
    )

    # 1개월 전 데이터 조회를 위한 JoinInfo
    month_1_join = JoinInfo(
        primary_table=table_name,
        secondary_table=table_name,
        primary_column="ticker",
        secondary_column="ticker",
        columns=["close", "volume"],
        is_outer=True,
        secondary_condition={"date__lte": f"DATE_SUB({main_data.date}, INTERVAL 1 MONTH)"},
    )

    # 6개월 전 데이터 조회를 위한 JoinInfo
    month_6_join = JoinInfo(
        primary_table=table_name,
        secondary_table=table_name,
        primary_column="ticker",
        secondary_column="ticker",
        columns=["close", "volume"],
        is_outer=True,
        secondary_condition={"date__lte": f"DATE_SUB({main_data.date}, INTERVAL 6 MONTH)"},
    )

    # 1년 전 데이터 조회를 위한 JoinInfo
    year_join = JoinInfo(
        primary_table=table_name,
        secondary_table=table_name,
        primary_column="ticker",
        secondary_column="ticker",
        columns=["close", "volume"],
        is_outer=True,
        secondary_condition={"date__lte": f"DATE_SUB({main_data.date}, INTERVAL 1 YEAR)"},
    )

    # 최종 데이터 조회
    result = database._select(
        table=table_name,
        columns=["ticker", "date", "market", "close", "volume"],
        join_info=[prev_join, week_join, month_1_join, month_6_join, year_join],
    )

    return result


def _get_stock_trend_1m(table_name: str):
    """
    1분 전 데이터 조회
    """
    pass
