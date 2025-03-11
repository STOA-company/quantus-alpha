from datetime import timedelta
import pandas as pd
from app.database.crud import database
from app.utils.date_utils import get_business_days, now_kr, now_utc
from sqlalchemy import text
from app.common.constants import US_EXCLUDE_DATES, KR_EXCLUDE_DATES
from app.core.logging.config import get_logger

logger = get_logger(__name__)


def renewal_us_run_disclosure_batch(date: str = None):
    """
    미국 공시 데이터 배치 처리 함수

    Args:
        date: 처리할 날짜 (YYYYMMDD)
    Returns:
        int: 처리한 레코드 수
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_utc(is_date=True)

    check_date_str = check_date.strftime("%Y-%m-%d")

    # Raw SQL 쿼리 작성
    query = text("""
    SELECT
        a.filing_id, a.ai_summary as en_summary, a.market_impact as en_market_impact, a.impact_reason as en_impact_reason, a.key_points as en_key_points,
        d.form_type, d.ticker, d.url, d.filing_date,
        t.ai_summary as kr_summary, t.impact_reason as kr_impact_reason, t.key_points as kr_key_points
    FROM usa_disclosure_analysis_translation t
    LEFT JOIN usa_disclosure d ON t.filing_id = d.filing_id
    LEFT JOIN usa_disclosure_analysis a ON t.filing_id = a.filing_id
    WHERE DATE(t.created_at) = :check_date
    AND t.lang = 'ko-KR'
    """)

    # _execute 메서드로 쿼리 실행
    result = database._execute(query, {"check_date": check_date_str})

    if result.rowcount == 0:
        error_msg = f"""
        `미국 공시 데이터 누락: usa_disclosure_analysis_translation 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    # 결과를 DataFrame으로 변환
    df_disclosure = pd.DataFrame(
        result.fetchall(),
        columns=[
            "filing_id",
            "en_summary",
            "en_market_impact",
            "en_impact_reason",
            "en_key_points",
            "form_type",
            "ticker",
            "url",
            "filing_date",
            "kr_summary",
            "kr_impact_reason",
            "kr_key_points",
        ],
    )
    ticker_list = df_disclosure["ticker"].unique().tolist()
    filing_dates = df_disclosure["filing_date"].dt.date.unique()

    if len(filing_dates) == 0:
        error_msg = f"""
        `미국 공시 데이터 누락: usa_disclosure_analysis_translation 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    # 영업일 목록 조회
    max_date = max(filing_dates)
    min_date = min(filing_dates)

    business_days = get_business_days(country="US", start_date=min_date - timedelta(days=7), end_date=max_date)
    business_days = sorted(business_days)
    business_days = [bd for bd in business_days if bd.strftime("%Y-%m-%d") not in US_EXCLUDE_DATES]
    # business_days의 모든 요소를 date 타입으로 변환
    business_days = [bd.date() if isinstance(bd, pd.Timestamp) else bd for bd in business_days]

    # 각 날짜의 가격 데이터 매핑 생성
    price_date_mapping = {}
    current_time = now_kr(is_date=False)
    today_str = current_time.strftime("%Y-%m-%d")

    for disclosure_date in pd.to_datetime(filing_dates):
        disclosure_date = disclosure_date.date()  # date 객체로 변환
        disclosure_date_str = disclosure_date.strftime("%Y-%m-%d")

        if disclosure_date_str == today_str:
            if disclosure_date_str not in business_days:
                price_date_mapping[disclosure_date] = business_days[-1].strftime("%Y-%m-%d")
            else:
                price_date_mapping[disclosure_date] = business_days[-2].strftime("%Y-%m-%d")
        else:
            if disclosure_date in business_days:
                price_date_mapping[disclosure_date] = disclosure_date_str
            else:
                for bd in reversed(business_days):
                    if bd < disclosure_date:
                        price_date_mapping[disclosure_date] = bd.strftime("%Y-%m-%d")
                        break

    # 종목 이름 데이터 조회
    df_stock_data = pd.DataFrame(
        database._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name"],
            **dict(ticker__in=ticker_list),
        )
    )
    if df_stock_data.empty:
        error_msg = f"""
        `미국 공시 데이터 누락: stock_information 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    existing_tickers = df_stock_data["ticker"].unique().tolist()

    df_merge = pd.merge(df_disclosure, df_stock_data, on="ticker", how="left")

    # 모든 가격 데이터 조회
    # unique_price_dates = list(set(price_date_mapping.values()))
    # df_price = pd.DataFrame(
    #     database._select(
    #         table="stock_us_1d",
    #         columns=["Ticker", "Date", "Close"],
    #         **dict(Date__in=unique_price_dates, Ticker__in=ticker_list),
    #     )
    # )
    # if df_price.empty:
    #     error_msg = f"""
    #     `미국 공시 데이터 누락: stock_us_1d 테이블 데이터 체크 필요합니다.`
    #     * business_day: {check_date}
    #     """
    #     raise ValueError(error_msg)

    # # price_dates 매핑을 사용하여 가격 데이터 병합
    # df_merge["price_date"] = df_merge["filing_date"].dt.date.map(lambda x: price_date_mapping.get(x))

    # # Date 컬럼을 문자열로 변환
    # df_price["Date"] = df_price["Date"].dt.strftime("%Y-%m-%d")

    # # 가격 데이터 병합
    # df_merge = pd.merge(df_merge, df_price, left_on=["ticker", "price_date"], right_on=["Ticker", "Date"], how="left")

    # 필수 컬럼 추가
    df_merge["ctry"] = "US"
    # df_merge["that_time_price"] = df_merge["Close"]
    df_merge["is_top_story"] = False
    df_merge["is_exist"] = df_merge["ticker"].isin(existing_tickers)

    # insert할 레코드 생성
    disclosure_records = []
    for _, row in df_merge.iterrows():
        disclosure_record = {
            "filing_id": row["filing_id"],
            "ticker": row["ticker"],
            "ko_name": row["kr_name"],
            "en_name": row["en_name"],
            "ctry": row["ctry"],
            "date": row["filing_date"],
            "title": None,
            "url": row["url"],
            "summary": row["kr_summary"],
            "impact_reason": row["kr_impact_reason"],
            "key_points": row["kr_key_points"],
            "en_summary": row["en_summary"],
            "en_impact_reason": row["en_impact_reason"],
            "en_key_points": row["en_key_points"],
            "emotion": row["en_market_impact"],
            "form_type": row["form_type"],
            "category_type": None,
            "extra_info": None,
            "that_time_price": 0,
            "is_top_story": False,
            "is_exist": row["is_exist"],
        }
        disclosure_records.append(disclosure_record)

    def batch_insert(records, batch_size=500):
        """
        레코드를 배치 크기로 나누어 삽입하는 함수
        중복된 filing_id는 skip
        """

        def replace_nan(records_batch):
            return [{k: (None if pd.isna(v) else v) for k, v in record.items()} for record in records_batch]

        # 기존 filing_id 조회 (chunk 단위로)
        chunk_size = 5000
        existing_filing_ids = set()
        offset = 0
        while True:
            chunk = pd.DataFrame(
                database._select(
                    table="disclosure_information",
                    columns=["filing_id"],
                    limit=chunk_size,
                    offset=offset,
                    **dict(ctry="US"),
                )
            )
            if chunk.empty:
                break
            existing_filing_ids.update(chunk["filing_id"].dropna())
            offset += chunk_size

        total = len(records)
        processed = 0
        skipped = 0

        # 배치 단위로 처리
        for i in range(0, total, batch_size):
            batch_records = records[i : i + batch_size]
            batch_df = pd.DataFrame(batch_records)

            # 중복 체크 (filing_id 기준)
            unique_batch = batch_df[~batch_df["filing_id"].isin(existing_filing_ids)]

            if not unique_batch.empty:
                cleaned_batch = replace_nan(unique_batch.to_dict("records"))
                try:
                    database._insert(table="disclosure_information", sets=cleaned_batch)
                    processed += len(cleaned_batch)
                    existing_filing_ids.update(unique_batch["filing_id"])
                except Exception as e:
                    logger.error(f"배치 처리 중 오류: {str(e)}")
                    raise

            skipped += len(batch_records) - len(unique_batch)
            logger.info(f"진행률: {i+len(batch_records)}/{total} (처리: {processed}, 스킵: {skipped})")

    # DB에 데이터 입력
    if disclosure_records:
        logger.info(f"총 입력할 레코드 수: {len(disclosure_records)}")

        try:
            batch_insert(disclosure_records)
            logger.info("모든 데이터 입력 완료")
        except Exception as e:
            logger.error(f"데이터베이스 입력 중 오류 발생: {str(e)}")
            error_msg = f"""
            미국 공시 데이터 처리 실패
            * 처리 날짜: {check_date}
            """
            raise ValueError(error_msg)

    return len(disclosure_records)


def renewal_kr_run_disclosure_batch(date: str = None):
    """
    한국 공시 데이터 배치 처리
    Args:
        date: 처리할 날짜 (YYYYMMDD)
    Returns:
        int: 처리한 레코드 수
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d")
    else:
        check_date = now_utc(is_date=False)

    check_date_str = check_date.strftime("%Y-%m-%d")

    # Raw SQL 쿼리 작성
    query = text("""
    SELECT
        a.filing_id, a.ai_summary as en_summary, a.market_impact as en_market_impact, a.impact_reason as en_impact_reason, a.key_points as en_key_points,
        d.company_name as ko_name, d.form_type, d.category_type, d.extra_info, d.ticker, d.url, d.filing_date,
        t.ai_summary as kr_summary, t.impact_reason as kr_impact_reason, t.key_points as kr_key_points
    FROM kor_disclosure_analysis_translation t
    LEFT JOIN kor_disclosure d ON t.filing_id = d.filing_id
    LEFT JOIN kor_disclosure_analysis a ON t.filing_id = a.filing_id
    WHERE DATE(t.created_at) = :check_date
    AND t.lang = 'ko-KR'
    """)

    # _execute 메서드로 쿼리 실행
    result = database._execute(query, {"check_date": check_date_str})
    if result.rowcount == 0:
        error_msg = f"""
        `한국 공시 데이터 누락: kor_disclosure_analysis_translation 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    # 결과를 DataFrame으로 변환
    df_disclosure = pd.DataFrame(
        result.fetchall(),
        columns=[
            "filing_id",
            "en_summary",
            "en_market_impact",
            "en_impact_reason",
            "en_key_points",
            "ko_name",
            "form_type",
            "category_type",
            "extra_info",
            "ticker",
            "url",
            "filing_date",
            "kr_summary",
            "kr_impact_reason",
            "kr_key_points",
        ],
    )
    ticker_list = df_disclosure["ticker"].unique().tolist()
    ticker_list = ["A" + ticker for ticker in ticker_list]

    filing_dates = df_disclosure["filing_date"].dt.date.unique()

    if len(filing_dates) == 0:
        return 0

    # 영업일 목록 조회
    max_date = max(filing_dates)
    min_date = min(filing_dates)

    business_days = get_business_days(country="KR", start_date=min_date - timedelta(days=7), end_date=max_date)
    business_days = sorted(business_days)
    # business_days의 모든 요소를 date 타입으로 변환
    business_days = [bd.date() if isinstance(bd, pd.Timestamp) else bd for bd in business_days]
    business_days = [bd for bd in business_days if bd.strftime("%Y-%m-%d") not in KR_EXCLUDE_DATES]
    # 각 날짜의 가격 데이터 매핑 생성
    price_dates = {}
    current_time = now_kr(is_date=False)
    today_str = current_time.date()

    for filing_date in filing_dates:
        if filing_date == today_str:
            if filing_date not in business_days:
                price_dates[filing_date] = business_days[-1]
            else:
                price_dates[filing_date] = business_days[-2]
        else:
            # 해당 날짜가 영업일인지 확인
            if filing_date in business_days:
                price_dates[filing_date] = filing_date
            else:
                # 해당 날짜 이전의 가장 최근 영업일 찾기
                found_previous_day = False  # noqa
                for bd in reversed(business_days):
                    if bd < filing_date:
                        price_dates[filing_date] = bd
                        found_previous_day = True  # noqa
                        break

    df_stock_data = pd.DataFrame(
        database._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name"],
            **dict(ticker__in=ticker_list),
        )
    )
    if df_stock_data.empty:
        error_msg = f"""
        `한국 공시 데이터 누락: stock_information 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    df_stock_data["ticker"] = df_stock_data["ticker"].str.replace("A", "")
    existing_tickers = df_stock_data["ticker"].unique().tolist()

    # 모든 가격 데이터 조회
    # unique_price_dates = list(set(price_dates.values()))
    # df_price = pd.DataFrame(
    #     database._select(
    #         table="stock_kr_1d",
    #         columns=["Ticker", "Date", "Close"],
    #         **dict(Date__in=unique_price_dates, Ticker__in=ticker_list),
    #     )
    # )
    # if df_price.empty:
    #     error_msg = f"""
    #     `한국 공시 데이터 누락: stock_kr_1d 테이블 데이터 체크 필요합니다.`
    #     * business_day: {check_date}
    #     """
    #     raise ValueError(error_msg)

    # # price_dates 매핑을 사용하여 가격 데이터 병합
    df_merge = df_disclosure.copy()
    # df_merge["price_date"] = df_merge["filing_date"].dt.date.map(lambda x: price_dates.get(x))

    # # Date 컬럼을 문자열로 변환
    # df_price["Date"] = df_price["Date"].dt.strftime("%Y-%m-%d")
    # df_price["Ticker"] = df_price["Ticker"].str.replace("A", "")

    # # 가격 데이터 병합
    # df_merge = pd.merge(df_merge, df_price, left_on=["ticker", "price_date"], right_on=["Ticker", "Date"], how="left")
    df_merge = pd.merge(df_merge, df_stock_data, on="ticker", how="left")

    # 필수 컬럼 추가
    df_merge["ctry"] = "KR"
    # df_merge["that_time_price"] = df_merge["Close"]
    df_merge["is_top_story"] = False
    df_merge["is_exist"] = df_merge["ticker"].isin(existing_tickers)

    # insert할 레코드 생성
    disclosure_records = []
    for _, row in df_merge.iterrows():
        disclosure_record = {
            "filing_id": row["filing_id"],
            "ticker": "A" + row["ticker"],
            "ko_name": row["kr_name"],
            "en_name": row["en_name"],
            "ctry": row["ctry"],
            "date": row["filing_date"],
            "title": None,
            "url": row["url"],
            "summary": row["kr_summary"],
            "impact_reason": row["kr_impact_reason"],
            "key_points": row["kr_key_points"],
            "en_summary": row["en_summary"],
            "en_impact_reason": row["en_impact_reason"],
            "en_key_points": row["en_key_points"],
            "emotion": row["en_market_impact"],
            "form_type": row["form_type"],
            "category_type": row["category_type"],
            "extra_info": row["extra_info"],
            "that_time_price": 0,
            "is_top_story": False,
            "is_exist": row["is_exist"],
        }
        disclosure_records.append(disclosure_record)

    def batch_insert(records, batch_size=500):
        """
        레코드를 배치 크기로 나누어 삽입하는 함수
        중복된 filing_id는 skip
        """

        def replace_nan(records_batch):
            return [{k: (None if pd.isna(v) else v) for k, v in record.items()} for record in records_batch]

        # 기존 filing_id 조회 (chunk 단위로)
        chunk_size = 5000
        existing_filing_ids = set()
        offset = 0
        while True:
            chunk = pd.DataFrame(
                database._select(
                    table="disclosure_information",
                    columns=["filing_id"],
                    limit=chunk_size,
                    offset=offset,
                    **dict(ctry="KR"),
                )
            )
            if chunk.empty:
                break
            existing_filing_ids.update(chunk["filing_id"].dropna())
            offset += chunk_size

        total = len(records)
        processed = 0
        skipped = 0

        # 배치 단위로 처리
        for i in range(0, total, batch_size):
            batch_records = records[i : i + batch_size]
            batch_df = pd.DataFrame(batch_records)

            # 중복 체크 (filing_id 기준)
            unique_batch = batch_df[~batch_df["filing_id"].isin(existing_filing_ids)]

            if not unique_batch.empty:
                cleaned_batch = replace_nan(unique_batch.to_dict("records"))
                try:
                    database._insert(table="disclosure_information", sets=cleaned_batch)
                    processed += len(cleaned_batch)
                    existing_filing_ids.update(unique_batch["filing_id"])
                except Exception as e:
                    logger.error(f"배치 처리 중 오류: {str(e)}")
                    raise

            skipped += len(batch_records) - len(unique_batch)
            logger.info(f"진행률: {i+len(batch_records)}/{total} (처리: {processed}, 스킵: {skipped})")

        return processed, skipped

    # 기존의 데이터베이스 입력 부분을 아래와 같이 수정
    if disclosure_records:
        logger.info(f"총 입력할 레코드 수: {len(disclosure_records)}")
        logger.info("첫 번째 레코드 샘플:")
        logger.info(disclosure_records[0])

        try:
            batch_insert(disclosure_records)
            logger.info("모든 데이터 입력 완료")
            return len(disclosure_records)
        except Exception as e:
            logger.error(f"미국 공시 데이터 처리 실패: {str(e)}")
            error_msg = f"""
            미국 공시 데이터 처리 실패
            * 처리 날짜: {check_date}
            * 에러 메시지: {str(e)}
            """
            raise ValueError(error_msg)

    return len(disclosure_records)


def kr_run_disclosure_is_top_story(date: str = None):
    """
    공시 주요 소식 선정 배치 함수
    Args:
        date (str): 원하는 날짜(YYYYMMDD)

    Returns:
        bool: 성공 여부
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_utc()

    utc_start_date = pd.to_datetime(check_date) - timedelta(days=1)
    utc_end_date = pd.to_datetime(check_date)

    news_data = pd.DataFrame(
        database._select(
            table="news_analysis",
            columns=["ticker"],
            **dict(
                ctry="KR",
                is_top_story=True,
            ),
        )
    )
    if news_data.empty:
        error_msg = f"""
        `뉴스 데이터 누락: news_analysis 테이블 데이터 체크 필요합니다.`
        * kr_time={check_date} utc_time={utc_start_date}
        """
        raise ValueError(error_msg)

    unique_tickers = news_data["ticker"].unique().tolist()

    top_5_tickers = unique_tickers

    try:
        # 모든 공시 데이터 중 is_top_story가 True인 데이터를 False로 초기화
        database._update(
            table="disclosure_information",
            sets={"is_top_story": False},
            **dict(ctry="KR", is_top_story=True),
        )

        # 거래대금 상위 5개 종목의 is_top_story를 True로 업데이트
        database._update(
            table="disclosure_information",
            sets={"is_top_story": True},
            **dict(
                ctry="KR",
                date__gte=utc_start_date,
                date__lt=utc_end_date,
                ticker__in=top_5_tickers,
            ),
        )
    except Exception:
        error_msg = f"""
        한국 공시 주요 소식 선정 배치 실패
        * 처리 날짜: {check_date}
        """
        raise ValueError(error_msg)


def us_run_disclosure_is_top_story(date: str = None):
    """
    미국 공시 주요 소식 선정 배치 함수
    Args:
        date (str): 원하는 날짜(YYYYMMDD)

    Returns:
        bool: 성공 여부
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_utc()

    utc_start_date = pd.to_datetime(check_date) - timedelta(days=1)
    utc_end_date = pd.to_datetime(check_date)

    # 주요소식 모아보기 11개를 맞추기 위해서 뉴스가 있는 종목을 조회해야 함.
    news_data = pd.DataFrame(
        database._select(
            table="news_analysis",
            columns=["ticker"],
            **dict(
                ctry="US",
                is_top_story=True,
            ),
        )
    )
    if news_data.empty:
        error_msg = f"""
        `뉴스 데이터 누락: news_analysis 테이블 데이터 체크 필요합니다.`
        * kr_time={check_date} utc_time={utc_start_date}
        """
        raise ValueError(error_msg)

    unique_tickers = news_data["ticker"].unique().tolist()

    top_6_tickers = unique_tickers

    try:
        # 해당 날짜의 모든 뉴스 데이터 is_top_story를 False로 초기화
        database._update(
            table="disclosure_information",
            sets={"is_top_story": False},
            **dict(ctry="US", is_top_story=True),
        )

        # 거래대금 상위 6개 종목의 is_top_story를 True로 업데이트
        database._update(
            table="disclosure_information",
            sets={"is_top_story": True},
            **dict(
                ctry="US",
                date__gte=utc_start_date,
                date__lt=utc_end_date,
                ticker__in=top_6_tickers,
            ),
        )
    except Exception:
        error_msg = f"""
        미국 공시 주요 소식 선정 배치 실패
        * 처리 날짜: {check_date}
        """
        raise ValueError(error_msg)


if __name__ == "__main__":
    renewal_kr_run_disclosure_batch()
#     from app.core.logging.config import configure_logging
#     configure_logging()
#     kr_run_disclosure_is_top_story(20250201)
