import argparse  # noqa
from datetime import timedelta
from io import BytesIO
import pandas as pd
from sqlalchemy import text
from Aws.common.configs import s3_client
from app.utils.date_utils import get_business_days, now_kr, now_utc
from app.database.crud import database
from app.common.constants import US_EXCLUDE_DATES, KR_EXCLUDE_DATES


def get_data_from_bucket(bucket, key, dir):
    response = s3_client.get_object(Bucket=bucket, Key=f"{dir}/{key}")
    return response["Body"].read()


def get_news_data(ctry: str, date: str):
    news_data = get_data_from_bucket(bucket="quantus-news", key=f"{date}.parquet", dir=f"merged_data/{ctry}")
    df = pd.read_parquet(BytesIO(news_data))

    return df


def kr_run_news_batch(date: str = None):
    """
    한국 뉴스 배치 함수
    Args:
        date (str): 원하는 s3 파일 날짜(YYYYMMDD)

    Returns:
        int: 입력된 뉴스 데이터 수
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_kr(is_date=True)

    s3_date_str = check_date.strftime("%Y%m%d")

    # 뉴스 데이터 조회 및 전처리
    df_news = get_news_data(ctry="KR", date=s3_date_str)
    if df_news.empty:
        error_msg = f"""
        `뉴스 데이터 누락: s3 뉴스 데이터 체크 필요합니다.`
        * s3_date: {check_date}
        """
        raise ValueError(error_msg)

    df_need_news = (
        df_news[["date", "Code", "titles", "summary", "emotion", "links"]]
        .dropna(subset=["emotion"])
        .assign(Code="A" + df_news["Code"])
    )

    news_tickers = df_need_news["Code"].unique().tolist()

    # 뉴스 데이터의 고유한 날짜 추출
    df_need_news["date"] = pd.to_datetime(df_need_news["date"])
    unique_dates = df_need_news["date"].dt.strftime("%Y-%m-%d").unique()

    # 영업일 목록 조회
    max_date = max(pd.to_datetime(unique_dates)).date()
    min_date = min(pd.to_datetime(unique_dates)).date()

    business_days = get_business_days(country="KR", start_date=min_date - timedelta(days=7), end_date=max_date)
    business_days = sorted(business_days)
    business_days = [bd for bd in business_days if bd.strftime("%Y-%m-%d") not in KR_EXCLUDE_DATES]
    business_days_dict = {bd.strftime("%Y-%m-%d"): bd for bd in business_days}
    # print(f'business_days_dict#####4#: {business_days_dict}')
    # return 0

    # DB에 존재하는 티커 조회
    df_stock_data = pd.DataFrame(
        database._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name"],
            **dict(ticker__in=news_tickers),
        )
    )
    if df_stock_data.empty:
        error_msg = f"""
        `종목 데이터 누락: stock_information 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    existing_tickers = df_stock_data["ticker"].unique().tolist()

    # 날짜 매핑 생성
    price_date_mapping = {}
    current_time = now_kr(is_date=False)
    today_str = current_time.strftime("%Y-%m-%d")

    for news_date in pd.to_datetime(unique_dates):
        news_date_str = news_date.strftime("%Y-%m-%d")

        if news_date_str == today_str:
            if news_date_str not in business_days_dict:
                price_date_mapping[news_date_str] = business_days[-1].strftime("%Y-%m-%d")
            else:
                price_date_mapping[news_date_str] = business_days[-2].strftime("%Y-%m-%d")
        else:
            news_date = news_date.date()
            if news_date_str in business_days_dict:
                price_date_mapping[news_date_str] = news_date_str
            else:
                for bd in reversed(business_days):
                    if bd.date() < news_date:
                        price_date_mapping[news_date_str] = bd.strftime("%Y-%m-%d")
                        break
    # print(f'price_date_mapping#####5#: {price_date_mapping}')

    def get_price_data(date, tickers):
        """주가 데이터 조회 함수"""
        return pd.DataFrame(
            database._select(
                table="stock_kr_1d",
                columns=["Ticker", "Close"],
                **dict(Date=date, Ticker__in=tickers),
            )
        )

    # News 테이블에 입력할 데이터 준비
    news_records = []

    # 날짜별로 처리
    for date_str in unique_dates:
        # 해당 날짜의 뉴스 데이터 필터링
        df_date = df_need_news[df_need_news["date"].dt.strftime("%Y-%m-%d") == date_str].copy()
        check_news_tickers = df_date["Code"].unique().tolist()

        if date_str in price_date_mapping:
            price_df = get_price_data(price_date_mapping[date_str], check_news_tickers)
            if price_df.empty:
                error_msg = f"""
                `주가 데이터 누락: stock_kr_1d 테이블 데이터 체크 필요합니다.`
                * business_day: {date_str}
                """
                raise ValueError(error_msg)

            df_date = pd.merge(df_date, price_df[["Ticker", "Close"]], left_on="Code", right_on="Ticker", how="left")
            df_date = pd.merge(df_date, df_stock_data, left_on="Code", right_on="ticker", how="left")

            # 레코드 생성
            for _, row in df_date.iterrows():
                if pd.isna(row["Code"]) or row["Code"] == "":
                    continue

                news_record = {
                    "ticker": row["Code"],
                    "ko_name": row["kr_name"],
                    "en_name": row["en_name"],
                    "ctry": "KR",
                    "date": row["date"],
                    "title": row["titles"],
                    "summary": row["summary"],
                    "emotion": row["emotion"],
                    "links": row["links"],
                    "that_time_price": row["Close"],
                    "is_top_story": False,
                    "is_exist": row["Code"] in existing_tickers,
                }
                news_records.append(news_record)
    # test = pd.DataFrame(news_records)
    # print(test.head())
    # return 0

    def batch_insert(records, batch_size=500):
        """
        레코드를 배치 크기로 나누어 삽입하는 함수
        중복된 links는 skip
        """

        def replace_nan(records_batch):
            return [{k: (None if pd.isna(v) else v) for k, v in record.items()} for record in records_batch]

        chunk_size = 5000
        existing_combinations = set()
        offset = 0
        while True:
            chunk = pd.DataFrame(
                database._select(
                    table="news_information",
                    columns=["links", "ticker"],
                    limit=chunk_size,
                    offset=offset,
                    **dict(ctry="KR"),
                )
            )
            if chunk.empty:
                break
            chunk["link_ticker"] = chunk["links"] + chunk["ticker"]
            existing_combinations.update(chunk.dropna(subset=["links"])["link_ticker"])
            offset += chunk_size

        total = len(records)
        processed = 0
        skipped = 0

        # 배치 단위로 처리
        for i in range(0, total, batch_size):
            batch_records = records[i : i + batch_size]
            batch_df = pd.DataFrame(batch_records)

            # 중복 체크
            batch_df["link_ticker"] = batch_df["links"] + batch_df["ticker"]
            unique_batch = batch_df[~batch_df["link_ticker"].isin(existing_combinations)]
            unique_batch = unique_batch.drop("link_ticker", axis=1)

            if not unique_batch.empty:
                cleaned_batch = replace_nan(unique_batch.to_dict("records"))
                try:
                    database._insert(table="news_information", sets=cleaned_batch)
                    processed += len(cleaned_batch)
                    existing_combinations.update(batch_df["link_ticker"])
                except Exception as e:
                    print(f"배치 처리 중 오류: {str(e)}")
                    raise

            skipped += len(batch_records) - len(unique_batch)
            print(f"진행률: {i+len(batch_records)}/{total} (처리: {processed}, 스킵: {skipped})")

    # DB에 데이터 입력
    if news_records:
        print(f"총 입력할 레코드 수: {len(news_records)}")

        try:
            batch_insert(news_records)
            print("모든 데이터 입력 완료")
        except Exception as e:
            print(f"데이터베이스 입력 중 오류 발생: {str(e)}")
            error_msg = f"""
            뉴스 데이터 처리 실패
            * 처리 날짜: {check_date}
            * 에러 메시지: {str(e)}
            """
            raise ValueError(error_msg)

    return len(news_records)


def renewal_kr_run_news_batch(date: str = None):
    """
    한국 뉴스 배치 함수
    Args:
        date (str): 원하는 날짜(YYYYMMDD)

    Returns:
        int: 입력된 뉴스 데이터 수
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_utc(is_date=True)

    # 뉴스 데이터 조회 및 전처리
    query = text("""
    SELECT
        n.id, n.ticker, n.title, n.related_tickers, n.url, n.news_date,
        a.ai_summary as en_summary, a.market_impact as emotion, a.impact_reason as en_impact_reason, a.key_points as en_key_points,
        t.ai_title, t.ai_summary as kr_summary, t.impact_reason as kr_impact_reason, t.key_points as kr_key_points
    FROM kor_news_analysis_translation as t
    LEFT JOIN kor_news as n ON t.collect_id = n.id
    LEFT JOIN kor_news_analysis as a ON t.collect_id = a.collect_id
    WHERE DATE(CONVERT_TZ(n.news_date, 'Asia/Seoul', 'UTC')) = :check_date
    AND t.lang = 'ko-KR'
    """)

    df_news = pd.DataFrame(database._execute(query, {"check_date": check_date.strftime("%Y-%m-%d")}))
    if df_news.empty:
        error_msg = f"""
        `뉴스 데이터 누락: kor_news_analysis_translation 테이블 데이터 체크 필요합니다.`
        * check_date: {check_date}
        """
        raise ValueError(error_msg)

    df_news["ticker"] = "A" + df_news["ticker"]
    df_news["related_tickers"] = df_news["related_tickers"].apply(
        lambda x: ",".join(["A" + ticker.strip() for ticker in x.split(",")]) if pd.notna(x) else ""
    )
    news_tickers = df_news["ticker"].unique().tolist()

    # 뉴스 데이터의 고유한 날짜 추출
    df_news["date"] = pd.to_datetime(df_news["news_date"])
    unique_dates = df_news["date"].dt.strftime("%Y-%m-%d").unique()

    # 영업일 목록 조회
    max_date = max(pd.to_datetime(unique_dates)).date()
    min_date = min(pd.to_datetime(unique_dates)).date()

    business_days = get_business_days(country="KR", start_date=min_date - timedelta(days=7), end_date=max_date)
    business_days = sorted(business_days)
    business_days = [bd for bd in business_days if bd.strftime("%Y-%m-%d") not in KR_EXCLUDE_DATES]
    business_days_dict = {bd.strftime("%Y-%m-%d"): bd for bd in business_days}

    # DB에 존재하는 티커 조회
    df_stock_data = pd.DataFrame(
        database._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name"],
            **dict(ticker__in=news_tickers, can_use=True),
        )
    )
    if df_stock_data.empty:
        error_msg = f"""
        `종목 데이터 누락: stock_information 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    existing_tickers = df_stock_data["ticker"].unique().tolist()

    # 날짜 매핑 생성
    price_date_mapping = {}
    current_time = now_kr(is_date=False)
    today_str = current_time.strftime("%Y-%m-%d")

    for news_date in pd.to_datetime(unique_dates):
        news_date_str = news_date.strftime("%Y-%m-%d")

        if news_date_str == today_str:
            if news_date_str not in business_days_dict:
                price_date_mapping[news_date_str] = business_days[-1].strftime("%Y-%m-%d")
            else:
                price_date_mapping[news_date_str] = business_days[-2].strftime("%Y-%m-%d")
        else:
            news_date = news_date.date()
            if news_date_str in business_days_dict:
                price_date_mapping[news_date_str] = news_date_str
            else:
                for bd in reversed(business_days):
                    if bd.date() < news_date:
                        price_date_mapping[news_date_str] = bd.strftime("%Y-%m-%d")
                        break

    def get_price_data(date, tickers):
        """주가 데이터 조회 함수"""
        return pd.DataFrame(
            database._select(
                table="stock_kr_1d",
                columns=["Ticker", "Close"],
                **dict(Date=date, Ticker__in=tickers),
            )
        )

    # News 테이블에 입력할 데이터 준비
    news_records = []

    # 날짜별로 처리
    for date_str in unique_dates:
        # 해당 날짜의 뉴스 데이터 필터링
        df_date = df_news[df_news["news_date"].dt.strftime("%Y-%m-%d") == date_str].copy()
        # check_news_tickers = df_date["ticker"].unique().tolist()

        if date_str in price_date_mapping:
            #     price_df = get_price_data(price_date_mapping[date_str], check_news_tickers)
            #     if price_df.empty:
            #         error_msg = f"""
            #         `주가 데이터 누락: stock_kr_1d 테이블 데이터 체크 필요합니다.`
            #         * business_day: {date_str}
            #         """
            #         raise ValueError(error_msg)

            # df_date = pd.merge(df_date, price_df[["Ticker", "Close"]], left_on="ticker", right_on="Ticker", how="left")
            df_date = pd.merge(df_date, df_stock_data, left_on="ticker", right_on="ticker", how="left")

            # 레코드 생성
            for _, row in df_date.iterrows():
                if pd.isna(row["ticker"]) or row["ticker"] == "":
                    continue

                news_record = {
                    "collect_id": row["id"],
                    "ticker": row["ticker"],
                    "kr_name": row["kr_name"],
                    "en_name": row["en_name"],
                    "ctry": "KR",
                    "date": row["news_date"],
                    "title": row["ai_title"],
                    "emotion": row["emotion"],
                    "summary": row["kr_summary"],
                    "impact_reason": row["kr_impact_reason"],
                    "key_points": row["kr_key_points"],
                    "en_summary": row["en_summary"],
                    "en_impact_reason": row["en_impact_reason"],
                    "en_key_points": row["en_key_points"],
                    "related_tickers": row["related_tickers"],
                    "url": row["url"],
                    "that_time_price": 0,
                    "is_top_story": False,
                    "is_exist": row["ticker"] in existing_tickers,
                }
                news_records.append(news_record)

    def batch_insert(records, batch_size=500):
        """
        레코드를 배치 크기로 나누어 삽입하는 함수
        중복된 collect_id는 skip
        """

        def replace_nan(records_batch):
            return [{k: (None if pd.isna(v) else v) for k, v in record.items()} for record in records_batch]

        # collect_id 기준으로 기존 데이터 조회
        chunk_size = 5000
        existing_ids = set()
        offset = 0
        while True:
            chunk = pd.DataFrame(
                database._select(
                    table="news_analysis",
                    columns=["collect_id"],
                    limit=chunk_size,
                    offset=offset,
                    **dict(ctry="KR"),
                )
            )
            if chunk.empty:
                break
            existing_ids.update(chunk.dropna(subset=["collect_id"])["collect_id"])
            offset += chunk_size

        total = len(records)
        processed = 0
        skipped = 0

        # 배치 단위로 처리
        for i in range(0, total, batch_size):
            batch_records = records[i : i + batch_size]
            batch_df = pd.DataFrame(batch_records)

            # 중복 체크 (collect_id 기준)
            unique_batch = batch_df[~batch_df["collect_id"].isin(existing_ids)]

            if not unique_batch.empty:
                cleaned_batch = replace_nan(unique_batch.to_dict("records"))
                try:
                    database._insert(table="news_analysis", sets=cleaned_batch)
                    processed += len(cleaned_batch)
                    existing_ids.update(unique_batch["collect_id"])
                except Exception as e:
                    print(f"배치 처리 중 오류: {str(e)}")
                    raise

            skipped += len(batch_records) - len(unique_batch)
            print(f"진행률: {i+len(batch_records)}/{total} (처리: {processed}, 스킵: {skipped})")

    # DB에 데이터 입력
    if news_records:
        print(f"총 입력할 레코드 수: {len(news_records)}")

        try:
            batch_insert(news_records)
            print("모든 데이터 입력 완료")
        except Exception as e:
            print(f"데이터베이스 입력 중 오류 발생: {str(e)}")
            error_msg = f"""
            뉴스 데이터 처리 실패
            * 처리 날짜: {check_date}
            * 에러 메시지: {str(e)}
            """
            raise ValueError(error_msg)

    return len(news_records)


def us_run_news_batch(date: str = None):
    """
    미국 뉴스 배치 함수
    Args:
        date (str): 원하는 s3 파일 날짜(YYYYMMDD)

    Returns:
        int: 입력된 뉴스 데이터 수
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_kr(is_date=True)

    s3_date_str = check_date.strftime("%Y%m%d")

    # 뉴스 데이터 조회 및 전처리
    df_news = get_news_data(ctry="US", date=s3_date_str)
    if df_news.empty:
        error_msg = f"""
        `뉴스 데이터 누락: s3 뉴스 데이터 체크 필요합니다.`
        * s3_date: {check_date}
        """
        raise ValueError(error_msg)

    df_need_news = df_news[["Code", "date", "titles", "summary", "emotion", "links"]].dropna(subset=["emotion"])

    news_tickers = df_need_news["Code"].unique().tolist()

    # 종목 데이터 조회
    df_stock_data = pd.DataFrame(
        database._select(
            table="stock_information", columns=["ticker", "kr_name", "en_name"], **dict(ticker__in=news_tickers)
        )
    )
    if df_stock_data.empty:
        error_msg = f"""
        `종목 데이터 누락: stock_information 테이블 데이터 체크 필요합니다.`
        * s3_date: {check_date}
        """
        raise ValueError(error_msg)

    existing_tickers = df_stock_data["ticker"].unique().tolist()

    # 뉴스 데이터의 고유한 날짜 추출
    df_need_news["date"] = pd.to_datetime(df_need_news["date"])
    unique_dates = df_need_news["date"].dt.strftime("%Y-%m-%d").unique()

    # 영업일 목록 조회
    max_date = max(pd.to_datetime(unique_dates)).date()
    min_date = min(pd.to_datetime(unique_dates)).date()

    business_days = get_business_days(country="US", start_date=min_date - timedelta(days=7), end_date=max_date)
    business_days = sorted(business_days)
    business_days = [bd for bd in business_days if bd.strftime("%Y-%m-%d") not in US_EXCLUDE_DATES]
    business_days_dict = {bd.strftime("%Y-%m-%d"): bd for bd in business_days}

    # 날짜 매핑 생성
    price_date_mapping = {}
    current_time = now_kr(is_date=False)
    today_str = current_time.strftime("%Y-%m-%d")

    for news_date in pd.to_datetime(unique_dates):
        news_date_str = news_date.strftime("%Y-%m-%d")

        if news_date_str == today_str:
            if news_date_str not in business_days_dict:
                price_date_mapping[news_date_str] = business_days[-1].strftime("%Y-%m-%d")
            else:
                price_date_mapping[news_date_str] = business_days[-2].strftime("%Y-%m-%d")
        else:
            news_date = news_date.date()
            if news_date_str in business_days_dict:
                price_date_mapping[news_date_str] = news_date_str
            else:
                for bd in reversed(business_days):
                    if bd.date() < news_date:
                        price_date_mapping[news_date_str] = bd.strftime("%Y-%m-%d")
                        break

    def get_price_data(date, tickers):
        """주가 데이터 조회 함수"""
        return pd.DataFrame(
            database._select(
                table="stock_us_1d",
                columns=["Ticker", "Close"],
                **dict(Date=date, Ticker__in=tickers),
            )
        )

    # News 테이블에 입력할 데이터 준비
    news_records = []

    # 날짜별로 처리
    for date_str in unique_dates:
        # 해당 날짜의 뉴스 데이터 필터링
        df_date = df_need_news[df_need_news["date"].dt.strftime("%Y-%m-%d") == date_str].copy()
        check_news_tickers = df_date["Code"].unique().tolist()

        if date_str in price_date_mapping:
            price_df = get_price_data(price_date_mapping[date_str], check_news_tickers)
            if price_df.empty:
                error_msg = f"""
                `주가 데이터 누락: stock_us_1d 테이블 데이터 체크 필요합니다.`
                * business_day: {date_str}
                """
                raise ValueError(error_msg)
            df_date = pd.merge(df_date, price_df[["Ticker", "Close"]], left_on="Code", right_on="Ticker", how="left")

            # 종목 정보 병합
            df_date = pd.merge(df_date, df_stock_data, left_on="Code", right_on="ticker", how="left")

            # 레코드 생성
            for _, row in df_date.iterrows():
                if pd.isna(row["Code"]) or row["Code"] == "":
                    continue

                news_record = {
                    "ticker": row["Code"],
                    "ko_name": row["kr_name"],
                    "en_name": row["en_name"],
                    "ctry": "US",
                    "date": row["date"],
                    "title": row["titles"],
                    "summary": row["summary"],
                    "emotion": row["emotion"],
                    "links": row["links"],
                    "that_time_price": row["Close"],
                    "is_top_story": False,
                    "is_exist": row["Code"] in existing_tickers,
                }
                news_records.append(news_record)

    def batch_insert(records, batch_size=500):
        """
        레코드를 배치 크기로 나누어 삽입하는 함수
        중복된 links는 skip
        """

        def replace_nan(records_batch):
            return [{k: (None if pd.isna(v) else v) for k, v in record.items()} for record in records_batch]

        chunk_size = 5000
        existing_combinations = set()
        offset = 0
        while True:
            chunk = pd.DataFrame(
                database._select(
                    table="news_information",
                    columns=["links", "ticker"],
                    limit=chunk_size,
                    offset=offset,
                    **dict(ctry="US"),
                )
            )
            if chunk.empty:
                break
            chunk["link_ticker"] = chunk["links"] + chunk["ticker"]
            existing_combinations.update(chunk.dropna(subset=["links"])["link_ticker"])
            offset += chunk_size

        total = len(records)
        processed = 0
        skipped = 0

        # 배치 단위로 처리
        for i in range(0, total, batch_size):
            batch_records = records[i : i + batch_size]
            batch_df = pd.DataFrame(batch_records)

            # 중복 체크
            batch_df["link_ticker"] = batch_df["links"] + batch_df["ticker"]
            unique_batch = batch_df[~batch_df["link_ticker"].isin(existing_combinations)]
            unique_batch = unique_batch.drop("link_ticker", axis=1)

            if not unique_batch.empty:
                cleaned_batch = replace_nan(unique_batch.to_dict("records"))
                try:
                    database._insert(table="news_information", sets=cleaned_batch)
                    processed += len(cleaned_batch)
                    existing_combinations.update(batch_df["link_ticker"])
                except Exception as e:
                    print(f"배치 처리 중 오류: {str(e)}")
                    raise

            skipped += len(batch_records) - len(unique_batch)
            print(f"진행률: {i+len(batch_records)}/{total} (처리: {processed}, 스킵: {skipped})")

    # DB에 데이터 입력
    if news_records:
        print(f"총 입력할 레코드 수: {len(news_records)}")

        try:
            batch_insert(news_records)
            print("모든 데이터 입력 완료")
        except Exception as e:
            print(f"데이터베이스 입력 중 오류 발생: {str(e)}")
            error_msg = f"""
            뉴스 데이터 처리 실패
            * 처리 날짜: {check_date}
            """
            raise ValueError(error_msg)

    return len(news_records)


def renewal_us_run_news_batch(date: str = None):
    """
    미국 뉴스 배치 함수
    Args:
        date (str): 원하는 날짜(YYYYMMDD)

    Returns:
        int: 입력된 뉴스 데이터 수
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_utc(is_date=True)

    # 뉴스 데이터 조회 및 전처리
    query = text("""
    SELECT
        n.id, n.ticker, n.title, n.related_tickers, n.url, n.news_date,
        a.ai_summary as en_summary, a.market_impact as emotion, a.impact_reason as en_impact_reason, a.key_points as en_key_points,
        t.ai_title, t.ai_summary as kr_summary, t.impact_reason as kr_impact_reason, t.key_points as kr_key_points
    FROM usa_news_analysis_translation as t
    LEFT JOIN usa_news as n ON t.collect_id = n.id
    LEFT JOIN usa_news_analysis as a ON t.collect_id = a.collect_id
    WHERE DATE(n.news_date) = :check_date
    AND t.lang = 'ko-KR'
    """)

    df_news = pd.DataFrame(database._execute(query, {"check_date": check_date.strftime("%Y-%m-%d")}))
    if df_news.empty:
        error_msg = f"""
        `뉴스 데이터 누락: kor_news_analysis_translation 테이블 데이터 체크 필요합니다.`
        * check_date: {check_date}
        """
        raise ValueError(error_msg)

    news_tickers = df_news["ticker"].unique().tolist()

    # 뉴스 데이터의 고유한 날짜 추출
    df_news["date"] = pd.to_datetime(df_news["news_date"])
    unique_dates = df_news["date"].dt.strftime("%Y-%m-%d").unique()

    # 영업일 목록 조회
    max_date = max(pd.to_datetime(unique_dates)).date()
    min_date = min(pd.to_datetime(unique_dates)).date()

    business_days = get_business_days(country="US", start_date=min_date - timedelta(days=7), end_date=max_date)
    business_days = sorted(business_days)
    business_days = [bd for bd in business_days if bd.strftime("%Y-%m-%d") not in US_EXCLUDE_DATES]
    business_days_dict = {bd.strftime("%Y-%m-%d"): bd for bd in business_days}

    # DB에 존재하는 티커 조회
    df_stock_data = pd.DataFrame(
        database._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name"],
            **dict(ticker__in=news_tickers, can_use=True),
        )
    )
    if df_stock_data.empty:
        error_msg = f"""
        `종목 데이터 누락: stock_information 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    existing_tickers = df_stock_data["ticker"].unique().tolist()

    # 날짜 매핑 생성
    price_date_mapping = {}
    current_time = now_kr(is_date=False)
    today_str = current_time.strftime("%Y-%m-%d")

    for news_date in pd.to_datetime(unique_dates):
        news_date_str = news_date.strftime("%Y-%m-%d")

        if news_date_str == today_str:
            if news_date_str not in business_days_dict:
                price_date_mapping[news_date_str] = business_days[-1].strftime("%Y-%m-%d")
            else:
                price_date_mapping[news_date_str] = business_days[-2].strftime("%Y-%m-%d")
        else:
            news_date = news_date.date()
            if news_date_str in business_days_dict:
                price_date_mapping[news_date_str] = news_date_str
            else:
                for bd in reversed(business_days):
                    if bd.date() < news_date:
                        price_date_mapping[news_date_str] = bd.strftime("%Y-%m-%d")
                        break

    def get_price_data(date, tickers):
        """주가 데이터 조회 함수"""
        return pd.DataFrame(
            database._select(
                table="stock_us_1d",
                columns=["Ticker", "Close"],
                **dict(Date=date, Ticker__in=tickers),
            )
        )

    # News 테이블에 입력할 데이터 준비
    news_records = []

    # 날짜별로 처리
    for date_str in unique_dates:
        # 해당 날짜의 뉴스 데이터 필터링
        df_date = df_news[df_news["news_date"].dt.strftime("%Y-%m-%d") == date_str].copy()
        # check_news_tickers = df_date["ticker"].unique().tolist()

        if date_str in price_date_mapping:
            #     price_df = get_price_data(price_date_mapping[date_str], check_news_tickers)
            #     if price_df.empty:
            #         error_msg = f"""
            #         `주가 데이터 누락: stock_us_1d 테이블 데이터 체크 필요합니다.`
            #         * business_day: {date_str}
            #         """
            #         raise ValueError(error_msg)

            #     df_date = pd.merge(df_date, price_df[["Ticker", "Close"]], left_on="ticker", right_on="Ticker", how="left")
            df_date = pd.merge(df_date, df_stock_data, left_on="ticker", right_on="ticker", how="left")

            # 레코드 생성
            for _, row in df_date.iterrows():
                if pd.isna(row["ticker"]) or row["ticker"] == "":
                    continue

                news_record = {
                    "collect_id": row["id"],
                    "ticker": row["ticker"],
                    "kr_name": row["kr_name"],
                    "en_name": row["en_name"],
                    "ctry": "US",
                    "date": row["news_date"],
                    "title": row["ai_title"],
                    "emotion": row["emotion"],
                    "summary": row["kr_summary"],
                    "impact_reason": row["kr_impact_reason"],
                    "key_points": row["kr_key_points"],
                    "en_summary": row["en_summary"],
                    "en_impact_reason": row["en_impact_reason"],
                    "en_key_points": row["en_key_points"],
                    "related_tickers": row["related_tickers"],
                    "url": row["url"],
                    "that_time_price": 0,
                    "is_top_story": False,
                    "is_exist": row["ticker"] in existing_tickers,
                }
                news_records.append(news_record)

    def batch_insert(records, batch_size=500):
        """
        레코드를 배치 크기로 나누어 삽입하는 함수
        중복된 collect_id는 skip
        """

        def replace_nan(records_batch):
            return [{k: (None if pd.isna(v) else v) for k, v in record.items()} for record in records_batch]

        # collect_id 기준으로 기존 데이터 조회
        chunk_size = 5000
        existing_ids = set()
        offset = 0
        while True:
            chunk = pd.DataFrame(
                database._select(
                    table="news_analysis",
                    columns=["collect_id"],
                    limit=chunk_size,
                    offset=offset,
                    **dict(ctry="US"),
                )
            )
            if chunk.empty:
                break
            existing_ids.update(chunk.dropna(subset=["collect_id"])["collect_id"])
            offset += chunk_size

        total = len(records)
        processed = 0
        skipped = 0

        # 배치 단위로 처리
        for i in range(0, total, batch_size):
            batch_records = records[i : i + batch_size]
            batch_df = pd.DataFrame(batch_records)

            # 중복 체크 (collect_id 기준)
            unique_batch = batch_df[~batch_df["collect_id"].isin(existing_ids)]

            if not unique_batch.empty:
                cleaned_batch = replace_nan(unique_batch.to_dict("records"))
                try:
                    database._insert(table="news_analysis", sets=cleaned_batch)
                    processed += len(cleaned_batch)
                    existing_ids.update(unique_batch["collect_id"])
                except Exception as e:
                    print(f"배치 처리 중 오류: {str(e)}")
                    raise

            skipped += len(batch_records) - len(unique_batch)
            print(f"진행률: {i+len(batch_records)}/{total} (처리: {processed}, 스킵: {skipped})")

    # DB에 데이터 입력
    if news_records:
        print(f"총 입력할 레코드 수: {len(news_records)}")

        try:
            batch_insert(news_records)
            print("모든 데이터 입력 완료")
        except Exception as e:
            print(f"데이터베이스 입력 중 오류 발생: {str(e)}")
            error_msg = f"""
            뉴스 데이터 처리 실패
            * 처리 날짜: {check_date}
            * 에러 메시지: {str(e)}
            """
            raise ValueError(error_msg)

    return len(news_records)


def temp_kr_run_news_is_top_story(date: str = None):
    """
    한국 뉴스 주요 소식 선정 배치 함수
    Args:
        date (str): 원하는 날짜(YYYYMMDD)

    Returns:
        bool: 성공 여부
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_kr(is_date=True)

    business_days = get_business_days(country="KR", start_date=check_date - timedelta(days=14), end_date=check_date)
    business_days = [bd for bd in business_days if bd.strftime("%Y-%m-%d") not in KR_EXCLUDE_DATES]

    if check_date == now_kr(is_date=True):
        business_day = business_days[-2]
    else:
        business_day = business_days[-1]

    start_date = check_date - timedelta(days=1)
    end_date = check_date

    news_data = pd.DataFrame(
        database._select(
            table="news_information",
            columns=["ticker"],
            **dict(
                ctry="KR",
                date__gte=start_date,
                date__lt=end_date,
                is_exist=True,
            ),
        )
    )
    if news_data.empty:
        error_msg = f"""
        `뉴스 데이터 누락: 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    unique_news_tickers = news_data["ticker"].unique().tolist()

    # 오늘 가격 데이터 조회
    df_price = pd.DataFrame(
        database._select(
            table="stock_kr_1d",
            columns=["Ticker", "Open", "Close", "High", "Low", "Volume"],
            **dict(Date=business_day.strftime("%Y-%m-%d"), Ticker__in=unique_news_tickers),
        )
    )
    if df_price.empty:
        error_msg = f"""
        `주가 데이터 누락: 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    df_price["trading_value"] = (
        (df_price["Close"] + df_price["Open"] + df_price["High"] + df_price["Low"]) / 4
    ) * df_price["Volume"]

    # 해당 날짜의 거래대금 상위 5개 종목 선정
    top_5_tickers = df_price.nlargest(5, "trading_value")["Ticker"].tolist()

    try:
        # 해당 날짜의 모든 뉴스 데이터 is_top_story를 False로 초기화
        database._update(
            table="news_information",
            sets={"is_top_story": False},
            **{
                "ctry": "KR",
                "is_top_story": True,
            },
        )

        # 거래대금 상위 5개 종목의 is_top_story를 True로 업데이트
        database._update(
            table="news_information",
            sets={"is_top_story": True},
            **{
                "ctry": "KR",
                "date__gte": start_date,
                "date__lt": end_date,
                "ticker__in": top_5_tickers,
            },
        )

        return True

    except Exception:
        error_msg = f"""
        한국 뉴스 주요 소식 선정 배치 실패
        * 처리 날짜: {check_date}
        """
        raise ValueError(error_msg)


def renewal_kr_run_news_is_top_story(date: str = None):
    """
    한국 뉴스 주요 소식 선정 배치 함수
    Args:
        date (str): 원하는 날짜(YYYYMMDD)

    Returns:
        bool: 성공 여부
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_kr(is_date=True)

    business_days = get_business_days(country="KR", start_date=check_date - timedelta(days=14), end_date=check_date)
    business_days = [bd for bd in business_days if bd.strftime("%Y-%m-%d") not in KR_EXCLUDE_DATES]

    if check_date == now_kr(is_date=True):
        business_day = business_days[-2]
    else:
        business_day = business_days[-1]

    start_date = check_date - timedelta(days=1)
    end_date = check_date

    news_data = pd.DataFrame(
        database._select(
            table="news_analysis",
            columns=["ticker"],
            **dict(
                ctry="KR",
                date__gte=start_date,
                date__lt=end_date,
                is_exist=True,
            ),
        )
    )
    if news_data.empty:
        error_msg = f"""
        `뉴스 데이터 누락: news_analysis 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    unique_news_tickers = news_data["ticker"].unique().tolist()

    # 오늘 가격 데이터 조회
    df_price = pd.DataFrame(
        database._select(
            table="stock_kr_1d",
            columns=["Ticker", "Open", "Close", "High", "Low", "Volume"],
            **dict(Date=business_day.strftime("%Y-%m-%d"), Ticker__in=unique_news_tickers),
        )
    )
    if df_price.empty:
        error_msg = f"""
        `주가 데이터 누락: stock_kr_1d 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    df_price["trading_value"] = (
        (df_price["Close"] + df_price["Open"] + df_price["High"] + df_price["Low"]) / 4
    ) * df_price["Volume"]

    # 해당 날짜의 거래대금 상위 5개 종목 선정
    top_5_tickers = df_price.nlargest(5, "trading_value")["Ticker"].tolist()

    try:
        # 해당 날짜의 모든 뉴스 데이터 is_top_story를 False로 초기화
        database._update(
            table="news_analysis",
            sets={"is_top_story": False},
            **{
                "ctry": "KR",
                "is_top_story": True,
            },
        )

        update_end_datetime = now_kr()
        update_start_datetime = update_end_datetime - timedelta(days=1)

        # 거래대금 상위 5개 종목의 is_top_story를 True로 업데이트
        database._update(
            table="news_analysis",
            sets={"is_top_story": True},
            **{
                "ctry": "KR",
                "date__gte": update_start_datetime,
                "date__lt": update_end_datetime,
                "ticker__in": top_5_tickers,
            },
        )

        return True

    except Exception:
        error_msg = f"""
        한국 뉴스 주요 소식 선정 배치 실패
        * 처리 날짜: {check_date}
        """
        raise ValueError(error_msg)


def temp_us_run_news_is_top_story(date: str = None):
    """
    미국 뉴스 주요 소식 선정 배치 함수
    Args:
        date (str): 원하는 s3 파일 날짜(YYYYMMDD)

    Returns:
        bool: 성공 여부
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_kr(is_date=True)

    business_days = get_business_days(country="US", start_date=check_date - timedelta(days=14), end_date=check_date)
    business_days = [bd for bd in business_days if bd.strftime("%Y-%m-%d") not in US_EXCLUDE_DATES]

    if check_date == now_kr(is_date=True):
        business_day = business_days[-2]
    else:
        business_day = business_days[-1]

    start_date = check_date - timedelta(days=1)
    end_date = check_date

    news_data = pd.DataFrame(
        database._select(
            table="news_information",
            columns=["ticker", "is_exist"],
            **dict(
                ctry="US",
                date__gte=start_date,
                date__lt=end_date,
                is_exist=True,
            ),
        )
    )
    if news_data.empty:
        error_msg = f"""
        `뉴스 데이터 누락: news_information 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    unique_news_tickers = news_data["ticker"].unique().tolist()

    # 오늘 가격 데이터 조회
    df_price = pd.DataFrame(
        database._select(
            table="stock_us_1d",
            columns=["Ticker", "Open", "Close", "High", "Low", "Volume"],
            **dict(Date=business_day.strftime("%Y-%m-%d"), Ticker__in=unique_news_tickers),
        )
    )
    if df_price.empty:
        error_msg = f"""
        `주가 데이터 누락: stock_us_1d 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    df_price["trading_value"] = (
        (df_price["Close"] + df_price["Open"] + df_price["High"] + df_price["Low"]) / 4
    ) * df_price["Volume"]

    # 해당 날짜의 거래대금 상위 6개 종목 선정
    top_6_tickers = df_price.nlargest(6, "trading_value")["Ticker"].tolist()

    try:
        # 해당 날짜의 모든 뉴스 데이터 is_top_story를 False로 초기화
        database._update(
            table="news_information",
            sets={"is_top_story": False},
            **{
                "ctry": "US",
                "is_top_story": True,
            },
        )

        # 거래대금 상위 5개 종목의 is_top_story를 True로 업데이트
        database._update(
            table="news_information",
            sets={"is_top_story": True},
            **{
                "ctry": "US",
                "date__gte": f"{start_date}",
                "date__lt": f"{end_date}",
                "ticker__in": top_6_tickers,
            },
        )
        return True
    except Exception:
        error_msg = f"""
        미국 뉴스 주요 소식 선정 배치 실패
        * 처리 날짜: {check_date}
        """
        raise ValueError(error_msg)


def renewal_us_run_news_is_top_story(date: str = None):
    """
    미국 뉴스 주요 소식 선정 배치 함수
    Args:
        date (str): 원하는 날짜(YYYYMMDD)

    Returns:
        bool: 성공 여부
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_utc(is_date=True)

    business_days = get_business_days(country="US", start_date=check_date - timedelta(days=14), end_date=check_date)
    business_days = [bd for bd in business_days if bd.strftime("%Y-%m-%d") not in US_EXCLUDE_DATES]

    if check_date == now_kr(is_date=True):
        business_day = business_days[-2]
    else:
        business_day = business_days[-1]

    start_date = check_date - timedelta(days=1)
    end_date = check_date

    news_data = pd.DataFrame(
        database._select(
            table="news_analysis",
            columns=["ticker", "is_exist"],
            **dict(
                ctry="US",
                date__gte=start_date,
                date__lt=end_date,
                is_exist=True,
            ),
        )
    )
    if news_data.empty:
        error_msg = f"""
        `뉴스 데이터 누락: news_analysis 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    unique_news_tickers = news_data["ticker"].unique().tolist()

    # 오늘 가격 데이터 조회
    df_price = pd.DataFrame(
        database._select(
            table="stock_us_1d",
            columns=["Ticker", "Open", "Close", "High", "Low", "Volume"],
            **dict(Date=business_day.strftime("%Y-%m-%d"), Ticker__in=unique_news_tickers),
        )
    )
    if df_price.empty:
        error_msg = f"""
        `주가 데이터 누락: stock_us_1d 테이블 데이터 체크 필요합니다.`
        * business_day: {check_date}
        """
        raise ValueError(error_msg)

    df_price["trading_value"] = (
        (df_price["Close"] + df_price["Open"] + df_price["High"] + df_price["Low"]) / 4
    ) * df_price["Volume"]

    # 해당 날짜의 거래대금 상위 6개 종목 선정
    top_6_tickers = df_price.nlargest(6, "trading_value")["Ticker"].tolist()

    try:
        # 해당 날짜의 모든 뉴스 데이터 is_top_story를 False로 초기화
        database._update(
            table="news_analysis",
            sets={"is_top_story": False},
            **{
                "ctry": "US",
                "is_top_story": True,
            },
        )

        update_end_datetime = now_kr()
        update_start_datetime = update_end_datetime - timedelta(days=1)

        # 거래대금 상위 5개 종목의 is_top_story를 True로 업데이트
        database._update(
            table="news_analysis",
            sets={"is_top_story": True},
            **{
                "ctry": "US",
                "date__gte": update_start_datetime,
                "date__lt": update_end_datetime,
                "ticker__in": top_6_tickers,
            },
        )
        return True
    except Exception:
        error_msg = f"""
        미국 뉴스 주요 소식 선정 배치 실패
        * 처리 날짜: {check_date}
        """
        raise ValueError(error_msg)


def kr_run_news_is_top_story(date: str = None):
    """
    한국 뉴스 주요 소식 선정 배치 함수
    Args:
        date (str): 원하는 날짜(YYYYMMDD)

    Returns:
        bool: 성공 여부
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_kr(is_date=True)

    # 오늘 가격 데이터 조회
    df_price = pd.DataFrame(
        database._select(
            table="stock_trend",
            columns=["ticker", "volume_change_1m"],
            **dict(Date=check_date.strftime("%Y-%m-%d"), ctry="KR"),  # TODO :: ctry 바뀔 가능성 존재함.
        )
    )

    # 거래대금 상위 5개 종목 선정
    top_5_tickers = df_price.nlargest(5, "volume_change_1m")["ticker"].tolist()

    # 해당 날짜의 모든 뉴스 데이터 is_top_story를 False로 초기화
    database._update(
        table="news_information",
        sets={"is_top_story": False},
        **dict(ctry="KR", date=check_date.strftime("%Y-%m-%d")),
    )

    # 거래대금 상위 5개 종목의 is_top_story를 True로 업데이트
    database._update(
        table="news_information",
        sets={"is_top_story": True},
        **dict(ctry="KR", date=check_date.strftime("%Y-%m-%d"), ticker__in=top_5_tickers),
    )

    return True


def us_run_news_is_top_story(date: str = None):
    """
    미국 뉴스 주요 소식 선정 배치 함수
    Args:
        date (str): 원하는 s3 파일 날짜(YYYYMMDD)

    Returns:
        bool: 성공 여부
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_utc(is_date=True)

    # 오늘 가격 데이터 조회
    df_price = pd.DataFrame(
        database._select(
            table="stock_trend",
            columns=["ticker", "volume_change_1m"],
            **dict(Date=check_date.strftime("%Y-%m-%d"), ctry="US"),
        )
    )

    # 거래대금 상위 6개 종목 선정
    top_6_tickers = df_price.nlargest(6, "volume_change_1m")["ticker"].tolist()

    # 해당 날짜의 모든 뉴스 데이터 is_top_story를 False로 초기화
    database._update(
        table="news_information",
        sets={"is_top_story": False},
        **dict(ctry="US", date=check_date.strftime("%Y-%m-%d")),
    )

    # 거래대금 상위 6개 종목의 is_top_story를 True로 업데이트
    database._update(
        table="news_information",
        sets={"is_top_story": True},
        **dict(ctry="US", date=check_date.strftime("%Y-%m-%d"), ticker__in=top_6_tickers),
    )

    return True


# if __name__ == "__main__":
#     renewal_kr_run_news_batch()
# renewal_us_run_news_batch()
# renewal_us_run_news_is_top_story()
#     renewal_us_run_news_batch(20250121)
#     renewal_kr_run_news_batch()
#     temp_us_run_news_is_top_story()
# temp_kr_run_news_is_top_story()
# renewal_kr_run_news_batch(20250114)
# for date in range(20241201, 20241211):
#     us_run_news_batch(date=str(date))
########################################
# parser = argparse.ArgumentParser(description="뉴스 데이터 수집 배치")
# parser.add_argument("--country", type=str, choices=["us", "kr"], required=True, help="수집할 국가 선택 (us 또는 kr)")
# parser.add_argument("--date", type=str, help="수집할 날짜 (YYYYMMDD 형식)")

# args = parser.parse_args()

# if args.country == "us":
#     if args.date:
#         us_run_news_batch(date=args.date)
#     else:
#         us_run_news_batch()
# elif args.country == "kr":
#     if args.date:
#         kr_run_news_batch(date=args.date)
#     else:
#         kr_run_news_batch()
