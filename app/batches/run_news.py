from datetime import timedelta
from io import BytesIO
import pandas as pd
from app.modules.common.enum import Country
from quantus_aws.common.configs import s3_client
from app.utils.date_utils import get_business_days, now_kr
from app.database.crud import database


def get_data_from_bucket(bucket, key, dir):
    response = s3_client.get_object(Bucket=bucket, Key=f"{dir}/{key}")
    return response["Body"].read()


def get_news_data(ctry: str, date: str):
    news_data = get_data_from_bucket(bucket="quantus-news", key=f"{date}.parquet", dir=f"merged_data/{ctry}")
    df = pd.read_parquet(BytesIO(news_data))

    return df


def kr_run_news_batch(date: str):
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

    # 뉴스 데이터 조회
    df_news = get_news_data(ctry=Country.KR, date=s3_date_str)
    df_need_news = df_news[["Code", "Name", "titles", "summary", "emotion"]]
    df_need_news = df_need_news.dropna(subset=["emotion"])
    df_need_news["Code"] = "A" + df_need_news["Code"]
    news_tickers = df_need_news["Code"].unique().tolist()

    # DB에 존재하는 티커 조회
    df_stock = pd.DataFrame(
        database._select(
            table="stock_kr_1d",
            columns=["Ticker", "Open", "Close", "Volume"],
            **dict(Date=check_date.strftime("%Y-%m-%d"), Ticker__in=news_tickers),
        )
    )

    # DB에 존재하는 티커 목록
    existing_tickers = df_stock["Ticker"].unique().tolist()

    # 뉴스 데이터의 고유한 날짜 추출
    df_need_news["date"] = pd.to_datetime(df_need_news["date"])
    unique_dates = df_need_news["date"].dt.strftime("%Y-%m-%d").unique()

    # 영업일 목록 조회
    max_date = max(pd.to_datetime(unique_dates)).date()
    min_date = min(pd.to_datetime(unique_dates)).date()

    business_days = get_business_days(country="KR", start_date=min_date - timedelta(days=7), end_date=max_date)
    business_days = sorted(business_days)

    # 각 날짜의 가격 데이터 매핑 생성
    price_date_mapping = {}
    current_time = now_kr(is_date=False)
    today_str = current_time.strftime("%Y-%m-%d")

    for news_date in pd.to_datetime(unique_dates):
        news_date_str = news_date.strftime("%Y-%m-%d")

        if news_date_str == today_str:
            price_date_mapping[news_date_str] = business_days[-2].strftime("%Y-%m-%d")

        # 과거 뉴스의 경우
        else:
            news_date = news_date.date()
            # 해당 날짜가 영업일인지 확인
            if news_date in business_days:
                price_date_mapping[news_date_str] = news_date_str
            else:
                # 해당 날짜 이전의 가장 최근 영업일 찾기
                for bd in reversed(business_days):
                    if bd.date() < news_date:
                        price_date_mapping[news_date_str] = bd.strftime("%Y-%m-%d")
                        break

    # 각 날짜별 가격 데이터 조회
    price_data = {}
    for news_date, price_date in price_date_mapping.items():
        df_price = pd.DataFrame(
            database._select(
                table="stock_kr_1d",
                columns=["Ticker", "Open", "Close", "High", "Low", "Volume"],
                **dict(Date=price_date, Ticker__in=news_tickers),
            )
        )
        if not df_price.empty:
            price_data[news_date] = df_price

    # News 테이블에 입력할 데이터 준비
    news_records = []

    # 날짜별로 처리
    for date_str in unique_dates:
        # 해당 날짜의 뉴스 데이터 필터링
        df_date = df_need_news[df_need_news["date"].dt.strftime("%Y-%m-%d") == date_str].copy()

        if date_str in price_data:
            # 해당 날짜의 가격 데이터와 병합
            df_date = pd.merge(df_date, price_data[date_str], left_on="Code", right_on="Ticker", how="left")

            # 변동률 계산
            df_date["price_change"] = ((df_date["Close"] - df_date["Open"]) / df_date["Open"]) * 100

            # 거래대금 계산
            df_date["trading_value"] = (df_date["Close"] + df_date["Open"]) * df_date["Volume"]

            # 레코드 생성
            for _, row in df_date.iterrows():
                if pd.isna(row["Code"]) or row["Code"] == "":
                    continue

                news_record = {
                    "ticker": row["Code"],
                    "ko_name": row["Name"],
                    "en_name": None,
                    "ctry": "KR",
                    "date": row["date"],
                    "title": row["titles"],
                    "summary": row["summary"],
                    "emotion": row["emotion"],
                    "that_time_price": row["Close"],
                    "that_time_change": row["price_change"],
                    "volume": row["Volume"],
                    "volume_change": row["trading_value"],
                    "is_top_story": False,
                    "is_exist": row["Code"] in existing_tickers,
                }
                news_records.append(news_record)

    # DB에 데이터 입력
    if news_records:
        try:
            database._insert(table="news_information", sets=news_records)
        except Exception:
            raise

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

    # 뉴스 데이터 조회
    df_news = get_news_data(ctry="US", date=s3_date_str)
    df_need_news = df_news[["Code", "date", "titles", "summary", "emotion"]]
    df_need_news = df_need_news.dropna(subset=["emotion"])
    news_tickers = df_need_news["Code"].unique().tolist()

    # 종목 데이터 조회
    df_stock_data = pd.DataFrame(
        database._select(
            table="stock_us_tickers", columns=["ticker", "korean_name", "english_name"], **dict(ticker__in=news_tickers)
        )
    )

    # DB에 존재하는 티커 목록
    existing_tickers = df_stock_data["ticker"].tolist()

    # 뉴스 데이터의 고유한 날짜 추출
    df_need_news["date"] = pd.to_datetime(df_need_news["date"])
    unique_dates = df_need_news["date"].dt.strftime("%Y-%m-%d").unique()

    # 영업일 목록 조회
    max_date = max(pd.to_datetime(unique_dates)).date()
    min_date = min(pd.to_datetime(unique_dates)).date()

    business_days = get_business_days(country="US", start_date=min_date - timedelta(days=7), end_date=max_date)
    business_days = sorted(business_days)

    # 각 날짜의 가격 데이터 매핑 생성
    price_date_mapping = {}
    current_time = now_kr(is_date=False)
    # us_market_close_time = current_time.replace(hour=4, minute=0)  # 한국시간 04:00 (미국 장마감)
    today_str = current_time.strftime("%Y-%m-%d")

    for news_date in pd.to_datetime(unique_dates):
        news_date_str = news_date.strftime("%Y-%m-%d")

        # # 1. 오늘 뉴스인 경우 나중에 쓰기 위해 주석 처리
        # if news_date_str == today_str:
        #     # 장 마감 전이면 전 영업일 데이터 사용
        #     if current_time < us_market_close_time:
        #         # 마지막에서 두 번째 영업일 사용 (마지막은 오늘)
        #         price_date_mapping[news_date_str] = business_days[-2].strftime('%Y-%m-%d')
        #     else:
        #         # 장 마감 후면 오늘 데이터 사용
        #         price_date_mapping[news_date_str] = today_str
        if news_date_str == today_str:
            price_date_mapping[news_date_str] = business_days[-2].strftime("%Y-%m-%d")

        # 2. 과거 뉴스의 경우
        else:
            news_date = news_date.date()
            # 해당 날짜가 영업일인지 확인
            if news_date in business_days:
                price_date_mapping[news_date_str] = news_date_str
            else:
                # 해당 날짜 이전의 가장 최근 영업일 찾기
                for bd in reversed(business_days):
                    if bd.date() < news_date:
                        price_date_mapping[news_date_str] = bd.strftime("%Y-%m-%d")
                        break

    # 각 날짜별 가격 데이터 조회
    price_data = {}
    for news_date, price_date in price_date_mapping.items():
        df_price = pd.DataFrame(
            database._select(
                table="stock_us_1d", columns=["Ticker", "Open", "Close", "High", "Low", "Volume"], **dict(Date=price_date)
            )
        )
        if not df_price.empty:
            price_data[news_date] = df_price

    # 데이터프레임 전처리
    df_merged = pd.merge(df_need_news, df_stock_data, left_on="Code", right_on="ticker", how="left")

    # News 테이블에 입력할 데이터 준비
    news_records = []

    # 날짜별로 처리
    for date_str in unique_dates:
        # 해당 날짜의 뉴스 데이터 필터링
        df_date = df_merged[df_merged["date"].dt.strftime("%Y-%m-%d") == date_str].copy()

        if date_str in price_data:
            # 해당 날짜의 가격 데이터와 병합
            df_date = pd.merge(df_date, price_data[date_str], left_on="Code", right_on="Ticker", how="left")

            # 변동률 계산
            df_date["price_change"] = ((df_date["Close"] - df_date["Open"]) / df_date["Open"]) * 100

            # 거래대금 계산
            df_date["trading_value"] = (
                (df_date["Close"] + df_date["Open"] + df_date["High"] + df_date["Low"]) / 4
            ) * df_date["Volume"]

            # 레코드 생성
            for _, row in df_date.iterrows():
                if pd.isna(row["Code"]) or row["Code"] == "":
                    continue  # Code가 없는 경우 건너뛰기
                news_record = {
                    "ticker": row["Code"],
                    "ko_name": row["korean_name"],
                    "en_name": row["english_name"],
                    "ctry": "US",
                    "date": row["date"],
                    "title": row["titles"],
                    "summary": row["summary"],
                    "emotion": row["emotion"],
                    "that_time_price": row["Close"],
                    "that_time_change": row["price_change"],
                    "volume": row["Volume"],
                    "volume_change": row["trading_value"],
                    "is_top_story": False,
                    "is_exist": row["Code"] in existing_tickers,
                }
                news_records.append(news_record)

    # DB에 데이터 입력
    if news_records:
        try:
            database._insert(table="news_information", sets=news_records)
        except Exception:
            raise

    return len(news_records)


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
            table="stock_kr_1d",
            columns=["Ticker", "Open", "Close", "High", "Low", "Volume"],
            **dict(Date=check_date.strftime("%Y-%m-%d"), ctry="KR"),
        )
    )
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
            **dict(ctry="KR", date=check_date.strftime("%Y-%m-%d")),
        )

        # 거래대금 상위 5개 종목의 is_top_story를 True로 업데이트
        database._update(
            table="news_information",
            sets={"is_top_story": True},
            **dict(ctry="KR", date=check_date.strftime("%Y-%m-%d"), ticker__in=top_5_tickers),
        )

        return True

    except Exception:
        raise


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
        check_date = now_kr(is_date=True)

    # 오늘 가격 데이터 조회
    df_price = pd.DataFrame(
        database._select(
            table="stock_us_1d",
            columns=["Ticker", "Open", "Close", "High", "Low", "Volume"],
            **dict(Date=check_date.strftime("%Y-%m-%d"), ctry="US"),
        )
    )
    df_price["trading_value"] = (
        (df_price["Close"] + df_price["Open"] + df_price["High"] + df_price["Low"]) / 4
    ) * df_price["Volume"]

    # 해당 날짜의 거래대금 상위 5개 종목 선정
    top_6_tickers = df_price.nlargest(6, "trading_value")["Ticker"].tolist()

    try:
        # 해당 날짜의 모든 뉴스 데이터 is_top_story를 False로 초기화
        database._update(
            table="news_information",
            sets={"is_top_story": False},
            **dict(ctry="US", date=check_date.strftime("%Y-%m-%d")),
        )

        # 거래대금 상위 5개 종목의 is_top_story를 True로 업데이트
        database._update(
            table="news_information",
            sets={"is_top_story": True},
            **dict(ctry="US", date=check_date.strftime("%Y-%m-%d"), ticker__in=top_6_tickers),
        )

        return True

    except Exception:
        raise


if __name__ == "__main__":
    us_run_news_batch()
