from datetime import timedelta
import pandas as pd
from app.database.crud import database
from app.utils.date_utils import get_business_days, now_kr
from sqlalchemy import text


def us_run_disclosure_batch(date: str = None):
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_kr(is_date=True)

    check_date_str = check_date.strftime("%Y-%m-%d")

    # Raw SQL 쿼리 작성
    query = text("""
    SELECT
        d.form_type, d.ticker, d.filing_date, d.url,
        a.ai_summary as en_summary, a.impact_reason as en_impact_reason, a.key_points as en_key_points, a.market_impact as en_market_impact,
        t.ai_summary as kr_summary, t.impact_reason as kr_impact_reason, t.key_points as kr_key_points
    FROM usa_disclosure d
    LEFT JOIN usa_disclosure_analysis a ON d.filing_id = a.filing_id
    LEFT JOIN usa_disclosure_analysis_translation t ON d.filing_id = t.filing_id
    WHERE d.ai_processed = TRUE
    AND DATE(d.processed_at) = :check_date
    AND t.lang = 'ko-KR'
    """)

    # _execute 메서드로 쿼리 실행
    result = database._execute(query, {"check_date": check_date_str})

    # 결과를 DataFrame으로 변환
    df_disclosure = pd.DataFrame(
        result.fetchall(),
        columns=[
            "form_type",
            "ticker",
            "filing_date",
            "url",
            "en_summary",
            "en_impact_reason",
            "en_key_points",
            "en_market_impact",
            "kr_summary",
            "kr_impact_reason",
            "kr_key_points",
        ],
    )
    ticker_list = df_disclosure["ticker"].unique().tolist()
    filing_dates = df_disclosure["filing_date"].dt.date.unique()

    if len(filing_dates) == 0:
        return 0

    # 영업일 목록 조회
    max_date = max(filing_dates)
    min_date = min(filing_dates)

    business_days = get_business_days(country="US", start_date=min_date - timedelta(days=7), end_date=max_date)
    business_days = sorted(business_days)
    # business_days의 모든 요소를 date 타입으로 변환
    business_days = [bd.date() if isinstance(bd, pd.Timestamp) else bd for bd in business_days]

    # 각 날짜의 가격 데이터 매핑 생성
    price_dates = {}
    current_time = now_kr(is_date=False)
    today_str = current_time.strftime("%Y-%m-%d")

    for filing_date in filing_dates:
        filing_date_str = filing_date.strftime("%Y-%m-%d")
        print(f"Processing filing_date: {filing_date_str}")  # 디버깅용 출력

        if filing_date_str == today_str:
            price_dates[filing_date] = business_days[-2].strftime("%Y-%m-%d")
        else:
            # 해당 날짜가 영업일인지 확인
            if filing_date in business_days:
                print(f"{filing_date_str} is a business day")  # 디버깅용 출력
                price_dates[filing_date] = filing_date_str
            else:
                print(f"{filing_date_str} is not a business day")  # 디버깅용 출력
                # 해당 날짜 이전의 가장 최근 영업일 찾기
                found_previous_day = False
                for bd in reversed(business_days):
                    if bd < filing_date:
                        price_dates[filing_date] = bd.strftime("%Y-%m-%d")
                        print(f"Found previous business day: {bd}")  # 디버깅용 출력
                        found_previous_day = True
                        break

                if not found_previous_day:
                    print(f"No previous business day found for {filing_date_str}")  # 디버깅용 출력

    # 종목 이름 데이터 조회
    df_stock = pd.DataFrame(
        database._select(
            table="stock_us_tickers",
            columns=["ticker", "korean_name", "english_name"],
            **dict(ticker__in=ticker_list),
        )
    )

    df_merge = pd.merge(df_disclosure, df_stock, on="ticker", how="left")

    # 모든 가격 데이터 조회
    unique_price_dates = list(set(price_dates.values()))
    df_price = pd.DataFrame(
        database._select(
            table="stock_us_1d",
            columns=["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"],
            **dict(Date__in=unique_price_dates, Ticker__in=ticker_list),
        )
    )

    # price_dates 매핑을 사용하여 가격 데이터 병합
    df_merge["price_date"] = df_merge["filing_date"].dt.date.map(lambda x: price_dates.get(x))

    # Date 컬럼을 문자열로 변환
    df_price["Date"] = df_price["Date"].dt.strftime("%Y-%m-%d")

    # 가격 데이터 병합
    df_merge = pd.merge(df_merge, df_price, left_on=["ticker", "price_date"], right_on=["Ticker", "Date"], how="left")

    # 필수 컬럼 추가
    df_merge["ctry"] = "US"
    df_merge["that_time_price"] = df_merge["Close"]
    df_merge["that_time_change"] = (df_merge["Close"] - df_merge["Open"]) / df_merge["Open"]
    df_merge["volume_change"] = (
        (df_merge["Open"] + df_merge["High"] + df_merge["Low"] + df_merge["Close"]) / 4
    ) * df_merge["Volume"]
    df_merge["is_top_story"] = False
    df_merge["is_exist"] = df_merge["ticker"].isin(df_price["Ticker"].unique().tolist())

    # insert할 레코드 생성
    disclosure_records = []
    for _, row in df_merge.iterrows():
        disclosure_record = {
            "ticker": row["ticker"],
            "ko_name": row["korean_name"],
            "en_name": row["english_name"],
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
            "that_time_price": row["that_time_price"],
            "is_top_story": False,
            "is_exist": row["is_exist"],
        }
        disclosure_records.append(disclosure_record)

    # disclosure_records = pd.DataFrame(disclosure_records)
    # disclosure_records.to_csv("33333us_disclosure_records.csv", index=False)

    # DB에 데이터 입력
    if disclosure_records:
        try:
            database._insert(table="disclosure_information", sets=disclosure_records)
            return len(disclosure_records)
        except Exception as e:
            raise e

    return len(disclosure_records)


def renewal_us_run_disclosure_batch(batch_min: int = 15, date: str = None):
    run_batch_min = batch_min

    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d%H%M%S").date()
    else:
        check_date = now_kr(is_date=True)

    check_date_str = check_date.strftime("%Y-%m-%d")
    start_datetime = check_date - timedelta(days=run_batch_min)  # noqa
    end_datetime = check_date  # noqa

    # Raw SQL 쿼리 작성
    query = text("""
    SELECT
        a.filing_id, a.ai_summary as en_summary, a.market_impact as en_market_impact, a.impact_reason as en_impact_reason, a.key_points as en_key_points,
        d.form_type, d.ticker, d.url, d.processed_at as filing_date,
        t.ai_summary as kr_summary, t.impact_reason as kr_impact_reason, t.key_points as kr_key_points
    FROM usa_disclosure_analysis_translation t
    LEFT JOIN usa_disclosure d ON t.filing_id = d.filing_id
    LEFT JOIN usa_disclosure_analysis a ON t.filing_id = a.filing_id
    WHERE DATE(t.created_at) = :check_date
    AND t.lang = 'ko-KR'
    """)
    # query = text("""
    # SELECT
    #     d.form_type, d.ticker, d.filing_date, d.url,
    #     a.ai_summary as en_summary, a.impact_reason as en_impact_reason, a.key_points as en_key_points, a.market_impact as en_market_impact,
    #     t.ai_summary as kr_summary, t.impact_reason as kr_impact_reason, t.key_points as kr_key_points
    # FROM usa_disclosure d
    # LEFT JOIN usa_disclosure_analysis a ON d.filing_id = a.filing_id
    # LEFT JOIN usa_disclosure_analysis_translation t ON d.filing_id = t.filing_id
    # WHERE d.ai_processed = TRUE
    # AND DATE(d.processed_at) >= :start_datetime
    # AND DATE(d.processed_at) < :end_datetime
    # AND t.lang = 'ko-KR'
    # """)

    # _execute 메서드로 쿼리 실행
    result = database._execute(query, {"check_date": check_date_str})

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
        return 0

    # 영업일 목록 조회
    max_date = max(filing_dates)
    min_date = min(filing_dates)

    business_days = get_business_days(country="US", start_date=min_date - timedelta(days=7), end_date=max_date)
    business_days = sorted(business_days)
    # business_days의 모든 요소를 date 타입으로 변환
    business_days = [bd.date() if isinstance(bd, pd.Timestamp) else bd for bd in business_days]

    # 각 날짜의 가격 데이터 매핑 생성
    price_dates = {}
    current_time = now_kr(is_date=False)
    today_str = current_time.strftime("%Y-%m-%d")

    for filing_date in filing_dates:
        filing_date_str = filing_date.strftime("%Y-%m-%d")

        if filing_date_str == today_str:
            price_dates[filing_date] = business_days[-2].strftime("%Y-%m-%d")
        else:
            # 해당 날짜가 영업일인지 확인
            if filing_date in business_days:
                price_dates[filing_date] = filing_date_str
            else:
                # 해당 날짜 이전의 가장 최근 영업일 찾기
                found_previous_day = False
                for bd in reversed(business_days):
                    if bd < filing_date:
                        price_dates[filing_date] = bd.strftime("%Y-%m-%d")
                        found_previous_day = True
                        break

                if not found_previous_day:
                    print(f"No previous business day found for {filing_date_str}")  # 디버깅용 출력

    # 종목 이름 데이터 조회
    df_stock = pd.DataFrame(
        database._select(
            table="stock_us_tickers",
            columns=["ticker", "korean_name", "english_name"],
            **dict(ticker__in=ticker_list),
        )
    )

    df_merge = pd.merge(df_disclosure, df_stock, on="ticker", how="left")

    # 모든 가격 데이터 조회
    unique_price_dates = list(set(price_dates.values()))
    df_price = pd.DataFrame(
        database._select(
            table="stock_us_1d",
            columns=["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"],
            **dict(Date__in=unique_price_dates, Ticker__in=ticker_list),
        )
    )

    # price_dates 매핑을 사용하여 가격 데이터 병합
    df_merge["price_date"] = df_merge["filing_date"].dt.date.map(lambda x: price_dates.get(x))

    # Date 컬럼을 문자열로 변환
    df_price["Date"] = df_price["Date"].dt.strftime("%Y-%m-%d")

    # 가격 데이터 병합
    df_merge = pd.merge(df_merge, df_price, left_on=["ticker", "price_date"], right_on=["Ticker", "Date"], how="left")

    # 필수 컬럼 추가
    df_merge["ctry"] = "US"
    df_merge["that_time_price"] = df_merge["Close"]
    df_merge["is_top_story"] = False
    df_merge["is_exist"] = df_merge["ticker"].isin(df_price["Ticker"].unique().tolist())

    # insert할 레코드 생성
    disclosure_records = []
    for _, row in df_merge.iterrows():
        disclosure_record = {
            "filing_id": row["filing_id"],
            "ticker": row["ticker"],
            "ko_name": row["korean_name"],
            "en_name": row["english_name"],
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
            "that_time_price": row["that_time_price"],
            "is_top_story": False,
            "is_exist": row["is_exist"],
        }
        disclosure_records.append(disclosure_record)

    # disclosure_records = pd.DataFrame(disclosure_records)
    # disclosure_records.to_csv("33333us_disclosure_records.csv", index=False)
    # print(f'disclosure_records######1: {len(disclosure_records)}')
    # print(disclosure_records[disclosure_records['filing_id']]['filing_id'].tolist())
    # return 0

    def batch_insert(records, batch_size=1000):
        """레코드를 배치 크기로 나누어 삽입하는 함수"""

        # NaN 값을 None으로 변환하는 함수
        def replace_nan(record):
            return {k: (None if pd.isna(v) else v) for k, v in record.items()}

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            # NaN 값을 None으로 변환
            cleaned_batch = [replace_nan(record) for record in batch]

            print(f"배치 처리 중: {i+1}~{min(i+batch_size, len(records))} / {len(records)}")
            try:
                database._insert(table="disclosure_information", sets=cleaned_batch)
                print(f"배치 {i//batch_size + 1} 성공적으로 입력됨")
            except Exception as e:
                print(f"배치 {i//batch_size + 1} 처리 중 오류 발생: {str(e)}")
                raise

    # 데이터베이스 입력
    if disclosure_records:
        print(f"총 입력할 레코드 수: {len(disclosure_records)}")
        print("첫 번째 레코드 샘플:")
        print(disclosure_records[0])

        try:
            batch_insert(disclosure_records)
            print("모든 데이터 입력 완료")
            return len(disclosure_records)
        except Exception as e:
            print(f"데이터베이스 입력 중 오류 발생: {str(e)}")
            raise

    return len(disclosure_records)


def kr_run_disclosure_batch(date: str = None):
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_kr(is_date=True)

    check_date_str = check_date.strftime("%Y-%m-%d")
    from_datetime = "2024-12-30 08:10:00"

    # Raw SQL 쿼리 작성
    query = text("""
    SELECT
        d.form_type, d.ticker, d.company_name as ko_name, d.processed_at as filing_date, d.url, d.category_type, d.extra_info,
        a.ai_summary as en_summary, a.impact_reason as en_impact_reason, a.key_points as en_key_points, a.market_impact as en_market_impact,
        t.ai_summary as kr_summary, t.impact_reason as kr_impact_reason, t.key_points as kr_key_points
    FROM kor_disclosure d
    LEFT JOIN kor_disclosure_analysis a ON d.filing_id = a.filing_id
    LEFT JOIN kor_disclosure_analysis_translation t ON d.filing_id = t.filing_id
    WHERE d.ai_processed = TRUE
    AND DATE(d.processed_at) = :check_date
    AND d.processed_at >= :from_datetime
    AND t.lang = 'ko-KR'
    """)

    # _execute 메서드로 쿼리 실행
    result = database._execute(query, {"check_date": check_date_str, "from_datetime": from_datetime})

    # 결과를 DataFrame으로 변환
    df_disclosure = pd.DataFrame(
        result.fetchall(),
        columns=[
            "form_type",
            "ticker",
            "ko_name",
            "filing_date",
            "url",
            "category_type",
            "extra_info",
            "en_summary",
            "en_impact_reason",
            "en_key_points",
            "en_market_impact",
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

    # 각 날짜의 가격 데이터 매핑 생성
    price_dates = {}
    current_time = now_kr(is_date=False)
    today_str = current_time.strftime("%Y-%m-%d")

    for filing_date in filing_dates:
        filing_date_str = filing_date.strftime("%Y-%m-%d")

        if filing_date_str == today_str:
            price_dates[filing_date] = business_days[-2].strftime("%Y-%m-%d")
        else:
            # 해당 날짜가 영업일인지 확인
            if filing_date in business_days:
                price_dates[filing_date] = filing_date_str
            else:
                # 해당 날짜 이전의 가장 최근 영업일 찾기
                found_previous_day = False  # noqa
                for bd in reversed(business_days):
                    if bd < filing_date:
                        price_dates[filing_date] = bd.strftime("%Y-%m-%d")
                        found_previous_day = True  # noqa
                        break

    # 모든 가격 데이터 조회
    unique_price_dates = list(set(price_dates.values()))
    df_price = pd.DataFrame(
        database._select(
            table="stock_kr_1d",
            columns=["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"],
            **dict(Date__in=unique_price_dates, Ticker__in=ticker_list),
        )
    )

    # price_dates 매핑을 사용하여 가격 데이터 병합
    df_merge = df_disclosure.copy()
    df_merge["price_date"] = df_merge["filing_date"].dt.date.map(lambda x: price_dates.get(x))

    # Date 컬럼을 문자열로 변환
    df_price["Date"] = df_price["Date"].dt.strftime("%Y-%m-%d")
    df_price["Ticker"] = df_price["Ticker"].str.replace("A", "")

    # 가격 데이터 병합
    df_merge = pd.merge(df_merge, df_price, left_on=["ticker", "price_date"], right_on=["Ticker", "Date"], how="left")

    # 필수 컬럼 추가
    df_merge["ctry"] = "KR"
    df_merge["that_time_price"] = df_merge["Close"]
    df_merge["that_time_change"] = (df_merge["Close"] - df_merge["Open"]) / df_merge["Open"]
    df_merge["volume_change"] = (
        (df_merge["Open"] + df_merge["High"] + df_merge["Low"] + df_merge["Close"]) / 4
    ) * df_merge["Volume"]
    df_merge["is_top_story"] = False
    df_merge["is_exist"] = df_merge["ticker"].isin(df_price["Ticker"].unique().tolist())

    # insert할 레코드 생성
    disclosure_records = []
    for _, row in df_merge.iterrows():
        disclosure_record = {
            "ticker": "A" + row["ticker"],
            "ko_name": row["ko_name"],
            "en_name": None,
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
            "that_time_price": row["that_time_price"],
            "is_top_story": False,
            "is_exist": row["is_exist"],
        }
        disclosure_records.append(disclosure_record)

    # disclosure_records = pd.DataFrame(disclosure_records)
    # disclosure_records.to_csv("44444kr_disclosure_records.csv", index=False)

    def batch_insert(records, batch_size=1000):
        """레코드를 배치 크기로 나누어 삽입하는 함수"""

        # NaN 값을 None으로 변환하는 함수
        def replace_nan(record):
            return {k: (None if pd.isna(v) else v) for k, v in record.items()}

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            # NaN 값을 None으로 변환
            cleaned_batch = [replace_nan(record) for record in batch]

            print(f"배치 처리 중: {i+1}~{min(i+batch_size, len(records))} / {len(records)}")
            try:
                database._insert(table="disclosure_information", sets=cleaned_batch)
                print(f"배치 {i//batch_size + 1} 성공적으로 입력됨")
            except Exception as e:
                print(f"배치 {i//batch_size + 1} 처리 중 오류 발생: {str(e)}")
                raise

    # 기존의 데이터베이스 입력 부분을 아래와 같이 수정
    if disclosure_records:
        print(f"총 입력할 레코드 수: {len(disclosure_records)}")
        print("첫 번째 레코드 샘플:")
        print(disclosure_records[0])

        try:
            batch_insert(disclosure_records)
            print("모든 데이터 입력 완료")
            return len(disclosure_records)
        except Exception as e:
            print(f"데이터베이스 입력 중 오류 발생: {str(e)}")
            raise

    return len(disclosure_records)


def renewal_kr_run_disclosure_batch(batch_min: int = 15, date: str = None):  # TODO :: 12월 30일 스킵하는 것 지우기!!!
    run_batch_min = batch_min  # 15분 단위로 배치 실행

    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d%H%M%S")
    else:
        check_date = now_kr(is_date=False)

    check_date_str = check_date.strftime("%Y-%m-%d")
    start_datetime = check_date - timedelta(minutes=run_batch_min)  # noqa
    end_datetime = check_date  # noqa

    # Raw SQL 쿼리 작성
    # query = text("""
    # SELECT
    #     a.filing_id, a.ai_summary as en_summary, a.market_impact as en_market_impact, a.impact_reason as en_impact_reason, a.key_points as en_key_points,
    #     d.company_name as ko_name, d.form_type, d.category_type, d.extra_info, d.ticker, d.url, d.processed_at as filing_date,
    #     t.ai_summary as kr_summary, t.impact_reason as kr_impact_reason, t.key_points as kr_key_points
    # FROM kor_disclosure_analysis_translation t
    # LEFT JOIN kor_disclosure d ON t.filing_id = d.filing_id
    # LEFT JOIN kor_disclosure_analysis a ON t.filing_id = a.filing_id
    # WHERE DATE(t.created_at) = :check_date
    # AND t.created_at >= :start_datetime
    # AND t.created_at < :end_datetime
    # AND t.lang = 'ko-KR'
    # """)
    query = text("""
    SELECT
        a.filing_id, a.ai_summary as en_summary, a.market_impact as en_market_impact, a.impact_reason as en_impact_reason, a.key_points as en_key_points,
        d.company_name as ko_name, d.form_type, d.category_type, d.extra_info, d.ticker, d.url, d.processed_at as filing_date,
        t.ai_summary as kr_summary, t.impact_reason as kr_impact_reason, t.key_points as kr_key_points
    FROM kor_disclosure_analysis_translation t
    LEFT JOIN kor_disclosure d ON t.filing_id = d.filing_id
    LEFT JOIN kor_disclosure_analysis a ON t.filing_id = a.filing_id
    WHERE DATE(t.created_at) = :check_date
    """)

    # _execute 메서드로 쿼리 실행
    # result = database._execute(query, {"check_date": check_date_str, "start_datetime": start_datetime, "end_datetime": end_datetime})
    result = database._execute(query, {"check_date": check_date_str})
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
    business_days = [bd for bd in business_days if bd.strftime("%Y-%m-%d") != "2024-12-30"]

    # 각 날짜의 가격 데이터 매핑 생성
    price_dates = {}
    current_time = now_kr(is_date=False)
    today_str = current_time.strftime("%Y-%m-%d")

    for filing_date in filing_dates:
        filing_date_str = filing_date.strftime("%Y-%m-%d")

        if filing_date_str == today_str:
            price_dates[filing_date] = business_days[-2].strftime("%Y-%m-%d")
        else:
            # 해당 날짜가 영업일인지 확인
            if filing_date in business_days:
                price_dates[filing_date] = filing_date_str
            else:
                # 해당 날짜 이전의 가장 최근 영업일 찾기
                found_previous_day = False  # noqa
                for bd in reversed(business_days):
                    if bd < filing_date:
                        price_dates[filing_date] = bd.strftime("%Y-%m-%d")
                        found_previous_day = True  # noqa
                        break

    # 모든 가격 데이터 조회
    unique_price_dates = list(set(price_dates.values()))
    df_price = pd.DataFrame(
        database._select(
            table="stock_kr_1d",
            columns=["Ticker", "Date", "Close"],
            **dict(Date__in=unique_price_dates, Ticker__in=ticker_list),
        )
    )

    # price_dates 매핑을 사용하여 가격 데이터 병합
    df_merge = df_disclosure.copy()
    df_merge["price_date"] = df_merge["filing_date"].dt.date.map(lambda x: price_dates.get(x))

    # Date 컬럼을 문자열로 변환
    df_price["Date"] = df_price["Date"].dt.strftime("%Y-%m-%d")
    df_price["Ticker"] = df_price["Ticker"].str.replace("A", "")

    # 가격 데이터 병합
    df_merge = pd.merge(df_merge, df_price, left_on=["ticker", "price_date"], right_on=["Ticker", "Date"], how="left")

    # 필수 컬럼 추가
    df_merge["ctry"] = "KR"
    df_merge["that_time_price"] = df_merge["Close"]
    df_merge["is_top_story"] = False
    df_merge["is_exist"] = df_merge["ticker"].isin(df_price["Ticker"].unique().tolist())

    # insert할 레코드 생성
    disclosure_records = []
    for _, row in df_merge.iterrows():
        disclosure_record = {
            "filing_id": row["filing_id"],
            "ticker": "A" + row["ticker"],
            "ko_name": row["ko_name"],
            "en_name": None,
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
            "that_time_price": row["that_time_price"],
            "is_top_story": False,
            "is_exist": row["is_exist"],
        }
        disclosure_records.append(disclosure_record)

    # disclosure_records = pd.DataFrame(disclosure_records)
    # disclosure_records.to_csv("44444kr_disclosure_records.csv", index=False)
    # print(f'disclosure_records######1: {len(disclosure_records)}')
    # return len(disclosure_records)

    def batch_insert(records, batch_size=1000):
        """레코드를 배치 크기로 나누어 삽입하는 함수"""

        # NaN 값을 None으로 변환하는 함수
        def replace_nan(record):
            return {k: (None if pd.isna(v) else v) for k, v in record.items()}

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            # NaN 값을 None으로 변환
            cleaned_batch = [replace_nan(record) for record in batch]

            print(f"배치 처리 중: {i+1}~{min(i+batch_size, len(records))} / {len(records)}")
            try:
                database._insert(table="disclosure_information", sets=cleaned_batch)
                print(f"배치 {i//batch_size + 1} 성공적으로 입력됨")
            except Exception as e:
                print(f"배치 {i//batch_size + 1} 처리 중 오류 발생: {str(e)}")
                raise

    # 기존의 데이터베이스 입력 부분을 아래와 같이 수정
    if disclosure_records:
        print(f"총 입력할 레코드 수: {len(disclosure_records)}")
        print("첫 번째 레코드 샘플:")
        print(disclosure_records[0])

        try:
            batch_insert(disclosure_records)
            print("모든 데이터 입력 완료")
            return len(disclosure_records)
        except Exception as e:
            print(f"데이터베이스 입력 중 오류 발생: {str(e)}")
            raise

    return len(disclosure_records)


def update_disclosure_that_time_price(date: str = None):
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_kr(is_date=True)

    df_disclosure = pd.DataFrame(
        database._select(
            table="disclosure_information",
            columns=["id", "ticker", "date"],
            **dict(date__like=check_date.strftime("%Y-%m-%d")),
        )
    )
    unique_tickers = df_disclosure["ticker"].unique().tolist()

    df_price = pd.DataFrame(  # noqa
        database._select(
            table="stock_kr_1d",
            columns=["Ticker", "Date", "Close"],
            **dict(Date=check_date.strftime("%Y-%m-%d"), Ticker__in=unique_tickers),
        )
    )


def temp_kr_run_disclosure_is_top_story(date: str = None):
    """
    stock_trend 테이블 생성 전 임시 사용 함수
    한국 공시 주요 소식 선정 배치 함수
    Args:
        date (str): 원하는 날짜(YYYYMMDD)

    Returns:
        bool: 성공 여부
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_kr(is_date=True)

    business_days = get_business_days(country="KR", start_date=check_date - timedelta(days=7), end_date=check_date)
    business_days = sorted(business_days)
    # business_days의 모든 요소를 date 타입으로 변환
    if check_date == now_kr(is_date=True):
        business_day = business_days[-2]
    else:
        business_day = business_days[-1]

    condition = dict(Date=business_day.strftime("%Y-%m-%d"))

    df_price = pd.DataFrame(
        database._select(
            table="stock_kr_1d",
            columns=["Ticker", "Open", "Close", "High", "Low", "Volume"],
            **condition,
        )
    )
    df_price["trading_value"] = (
        (df_price["Close"] + df_price["Open"] + df_price["High"] + df_price["Low"]) / 4 * df_price["Volume"]
    )

    top_5_tickers = df_price.nlargest(5, "trading_value")["Ticker"].tolist()

    start_date = check_date
    end_date = start_date + timedelta(days=1)

    try:
        # 해당 날짜의 모든 뉴스 데이터 is_top_story를 False로 초기화
        database._update(
            table="disclosure_information",
            sets={"is_top_story": False},
            **dict(ctry="KR", date__gte=start_date - timedelta(days=1), date__lt=end_date),
        )

        # 거래대금 상위 5개 종목의 is_top_story를 True로 업데이트
        database._update(
            table="disclosure_information",
            sets={"is_top_story": True},
            **dict(ctry="KR", date__gte=start_date, date__lt=end_date, ticker__in=top_5_tickers),
        )
    except Exception as e:
        raise e

    return True


def temp_us_run_disclosure_is_top_story(date: str = None):
    """
    stock_trend 테이블 생성 전 임시 사용 함수
    미국 공시 주요 소식 선정 배치 함수
    Args:
        date (str): 원하는 날짜(YYYYMMDD)

    Returns:
        bool: 성공 여부
    """
    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_kr(is_date=True)

    business_days = get_business_days(country="US", start_date=check_date - timedelta(days=7), end_date=check_date)
    if check_date == now_kr(is_date=True):
        business_day = business_days[-2]
    else:
        business_day = business_days[-1]

    df_price = pd.DataFrame(
        database._select(
            table="stock_us_1d",
            columns=["Ticker", "Open", "Close", "High", "Low", "Volume"],
            **dict(Date=business_day.strftime("%Y-%m-%d")),
        )
    )
    df_price["trading_value"] = (
        (df_price["Close"] + df_price["Open"] + df_price["High"] + df_price["Low"]) / 4 * df_price["Volume"]
    )

    top_6_tickers = df_price.nlargest(6, "trading_value")["Ticker"].tolist()

    start_date = check_date
    end_date = start_date + timedelta(days=1)

    try:
        # 해당 날짜의 모든 뉴스 데이터 is_top_story를 False로 초기화
        database._update(
            table="disclosure_information",
            sets={"is_top_story": False},
            **dict(ctry="US", date__gte=start_date - timedelta(days=1), date__lt=end_date),
        )

        # 거래대금 상위 6개 종목의 is_top_story를 True로 업데이트
        database._update(
            table="disclosure_information",
            sets={"is_top_story": True},
            **dict(ctry="US", date__gte=start_date, date__lt=end_date, ticker__in=top_6_tickers),
        )
    except Exception as e:
        raise e

    return True


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
        check_date = now_kr(is_date=True)

    df_price = pd.DataFrame(
        database._select(
            table="stock_trend",
            columns=["ticker", "volume_change_1m"],
            **dict(Date=check_date.strftime("%Y-%m-%d"), ctry="KR"),  # TODO :: ctry 바뀔 가능성 존재함.
        )
    )

    top_5_tickers = df_price.nlargest(5, "volume_change_1m")["ticker"].tolist()

    try:
        # 해당 날짜의 모든 뉴스 데이터 is_top_story를 False로 초기화
        database._update(
            table="disclosure_information",
            sets={"is_top_story": False},
            **dict(ctry="KR", date=check_date.strftime("%Y-%m-%d")),
        )

        # 거래대금 상위 5개 종목의 is_top_story를 True로 업데이트
        database._update(
            table="disclosure_information",
            sets={"is_top_story": True},
            **dict(ctry="KR", date=check_date.strftime("%Y-%m-%d"), ticker__in=top_5_tickers),
        )
    except Exception as e:
        raise e


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
        check_date = now_kr(is_date=True)

    df_price = pd.DataFrame(
        database._select(
            table="stock_trend",
            columns=["ticker", "volume_change_1m"],
            **dict(Date=check_date.strftime("%Y-%m-%d"), ctry="US"),
        )
    )
    top_6_tickers = df_price.nlargest(6, "volume_change_1m")["ticker"].tolist()

    try:
        # 해당 날짜의 모든 뉴스 데이터 is_top_story를 False로 초기화
        database._update(
            table="disclosure_information",
            sets={"is_top_story": False},
            **dict(ctry="US", date=check_date.strftime("%Y-%m-%d")),
        )

        # 거래대금 상위 6개 종목의 is_top_story를 True로 업데이트
        database._update(
            table="disclosure_information",
            sets={"is_top_story": True},
            **dict(ctry="US", date=check_date.strftime("%Y-%m-%d"), ticker__in=top_6_tickers),
        )
    except Exception as e:
        raise e


if __name__ == "__main__":
    # us_run_disclosure_batch(20241223)
    # kr_run_disclosure_batch(20241230)
    # temp_us_run_disclosure_is_top_story()
    # renewal_us_run_disclosure_batch(batch_min=15, date="20241218080000")
    renewal_kr_run_disclosure_batch(batch_min=15, date="20250102080000")
