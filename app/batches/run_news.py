from datetime import timedelta
import pandas as pd
from sqlalchemy import text

from app.utils.date_utils import now_utc
from app.database.crud import database
from app.core.logging.config import get_logger

logger = get_logger(__name__)


def run_news_batch(ctry: str = None, date: str = None):
    """
    뉴스 배치 함수
    Args:
        date (str): 원하는 날짜(YYYYMMDD)
        ctry (str): 국가 코드(KR, US)
    """
    if ctry not in ["KR", "US"]:
        error_msg = f"""
        유효하지 않은 국가 코드: {ctry}
        """
        raise ValueError(error_msg)

    if date:
        check_date = pd.to_datetime(date, format="%Y%m%d").date()
    else:
        check_date = now_utc(is_date=True)

    if ctry == "KR":
        ctry_news = "kor_news"
        ctry_news_analysis = "kor_news_analysis"
        ctry_news_analysis_translation = "kor_news_analysis_translation"
    elif ctry == "US":
        ctry_news = "usa_news"
        ctry_news_analysis = "usa_news_analysis"
        ctry_news_analysis_translation = "usa_news_analysis_translation"

    query = f"""
    SELECT
        n.id, n.ticker, n.title, n.related_tickers, n.url, n.news_date,
        a.market_impact as emotion, a.is_related,
        t.ai_title, t.ai_summary as summary, t.impact_reason, t.key_points, t.lang
    FROM {ctry_news} as n
    LEFT JOIN {ctry_news_analysis} as a ON n.id = a.collect_id
    RIGHT JOIN {ctry_news_analysis_translation} as t ON n.id = t.collect_id
    WHERE DATE(t.created_at) = :check_date
    AND t.lang = 'ko-KR'
    """
    # TODO :: AND t.lang = 'ko-KR' => 제거 해야 함. 필터링 로직 만들기 전까지 임시로 사용
    df_news = pd.DataFrame(
        database._execute(
            text(query),
            {
                "check_date": check_date.strftime("%Y-%m-%d"),
            },
        )
    )
    if df_news.empty:
        error_msg = f"""
        `뉴스 데이터 누락: {ctry_news} 테이블 데이터 체크 필요합니다.`
        * check_date: {check_date}
        """
        raise ValueError(error_msg)
    if ctry == "KR":
        df_news["ticker"] = "A" + df_news["ticker"]

    df_news["related_tickers"] = df_news["related_tickers"].apply(
        lambda x: ",".join(["A" + ticker.strip() for ticker in x.split(",")]) if pd.notna(x) else ""
    )

    news_tickers = df_news["ticker"].unique().tolist()

    df_stock_data = pd.DataFrame(
        database._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name"],
            **dict(ticker__in=news_tickers, is_activate=True),
        )
    )
    if df_stock_data.empty:
        error_msg = f"""
        `종목 데이터 누락: stock_information 테이블 데이터 체크 필요합니다.`
        * check_date: {check_date}
        """
        raise ValueError(error_msg)

    existing_tickers = df_stock_data["ticker"].unique().tolist()

    news_records = []
    total_records = pd.merge(df_news, df_stock_data, left_on="ticker", right_on="ticker", how="left")

    for _, row in total_records.iterrows():
        if pd.isna(row["ticker"]) or row["ticker"] == "":
            continue

        news_record = {
            "collect_id": row["id"],
            "ticker": row["ticker"],
            "kr_name": row["kr_name"],
            "en_name": row["en_name"],
            "ctry": ctry,
            "date": row["news_date"],
            "title": row["ai_title"],
            "emotion": row["emotion"],
            "summary": row["summary"],
            "impact_reason": row["impact_reason"],
            "key_points": row["key_points"],
            "lang": row["lang"],
            "related_tickers": row["related_tickers"],
            "url": row["url"],
            "that_time_price": 0,
            "is_top_story": False,
            "is_exist": row["ticker"] in existing_tickers,
            "is_related": row["is_related"],
        }
        news_records.append(news_record)

    def batch_insert(records, batch_size=500):
        """
        레코드를 배치 크기로 나누어 삽입하는 함수
        collect_id, ctry, lang의 조합으로 중복 체크
        """

        def replace_nan(records_batch):
            return [{k: (None if pd.isna(v) else v) for k, v in record.items()} for record in records_batch]

        # collect_id, ctry, lang 조합으로 기존 데이터 조회
        chunk_size = 5000
        existing_combinations = set()
        offset = 0
        collect_ids = [record["collect_id"] for record in records]

        while True:
            chunk = pd.DataFrame(
                database._select(
                    table="news_analysis",
                    columns=["collect_id", "ctry", "lang"],
                    limit=chunk_size,
                    offset=offset,
                    **dict(collect_id__in=collect_ids),
                )
            )
            if chunk.empty:
                break
            # collect_id, ctry, lang 조합을 튜플로 저장
            existing_combinations.update(
                set(
                    zip(
                        chunk.dropna(subset=["collect_id", "ctry", "lang"])["collect_id"],
                        chunk.dropna(subset=["collect_id", "ctry", "lang"])["ctry"],
                        chunk.dropna(subset=["collect_id", "ctry", "lang"])["lang"],
                    )
                )
            )
            offset += chunk_size

        total = len(records)
        processed = 0
        skipped = 0

        # 배치 단위로 처리
        for i in range(0, total, batch_size):
            batch_records = records[i : i + batch_size]
            batch_df = pd.DataFrame(batch_records)

            # 중복 체크 (collect_id, ctry, lang 조합 기준)
            batch_df["is_duplicate"] = batch_df.apply(
                lambda x: (x["collect_id"], x["ctry"], x["lang"]) in existing_combinations, axis=1
            )
            unique_batch = batch_df[~batch_df["is_duplicate"]].drop(columns=["is_duplicate"])

            if not unique_batch.empty:
                cleaned_batch = replace_nan(unique_batch.to_dict("records"))
                try:
                    database._insert(table="news_analysis", sets=cleaned_batch)
                    processed += len(cleaned_batch)
                    # 새로 추가된 레코드의 collect_id, ctry, lang 조합을 existing_combinations에 추가
                    existing_combinations.update(
                        set(zip(unique_batch["collect_id"], unique_batch["ctry"], unique_batch["lang"]))
                    )
                except Exception as e:
                    logger.info(f"배치 처리 중 오류: {str(e)}")
                    raise

            skipped += len(batch_records) - len(unique_batch)
            logger.info(f"진행률: {i+len(batch_records)}/{total} (처리: {processed}, 스킵: {skipped})")

    # DB에 데이터 입력
    if news_records:
        logger.info(f"총 입력할 레코드 수: {len(news_records)}")

        try:
            batch_insert(news_records)
            logger.info("모든 데이터 입력 완료")
        except Exception as e:
            logger.info(f"데이터베이스 입력 중 오류 발생: {str(e)}")
            error_msg = f"""
            뉴스 데이터 처리 실패
            * 처리 날짜: {check_date}
            * 에러 메시지: {str(e)}
            """
            raise ValueError(error_msg)

    return len(news_records)


##################################################
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
        check_date = now_utc(is_date=True)

    utc_start_date = pd.to_datetime(check_date) - timedelta(days=1)
    utc_end_date = pd.to_datetime(check_date)

    news_data = pd.DataFrame(
        database._select(
            table="news_analysis",
            columns=["ticker"],
            **dict(
                ctry="KR",
                date__gte=utc_start_date,
                date__lt=utc_end_date,
                is_exist=True,
            ),
        )
    )
    if news_data.empty:
        error_msg = f"""
        `뉴스 데이터 누락: news_analysis 테이블 데이터 체크 필요합니다.`
        * kr_time={check_date} utc_time={utc_start_date}
        """
        raise ValueError(error_msg)

    unique_news_tickers = news_data["ticker"].unique().tolist()
    df_price = pd.DataFrame(
        database._select(
            table="stock_trend",
            columns=["ticker", "volume_change_1d"],
            **dict(ticker__in=unique_news_tickers, ctry="kr"),
        )
    )

    # 거래대금 상위 5개 종목 선정
    top_5_tickers = df_price.nlargest(5, "volume_change_1d")["ticker"].tolist()
    try:
        database._update(
            table="news_analysis",
            sets={"is_top_story": False},
            **{
                "ctry": "KR",
                "is_top_story": True,
            },
        )

        database._update(
            table="news_analysis",
            sets={"is_top_story": True},
            **{
                "ctry": "KR",
                "date__gte": utc_start_date,
                "date__lt": utc_end_date,
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

    utc_start_date = pd.to_datetime(check_date) - timedelta(days=1)
    utc_end_date = pd.to_datetime(check_date)

    news_data = pd.DataFrame(
        database._select(
            table="news_analysis",
            columns=["ticker", "is_exist"],
            **dict(
                ctry="US",
                date__gte=utc_start_date,
                date__lt=utc_end_date,
                is_exist=True,
            ),
        )
    )
    if news_data.empty:
        error_msg = f"""
        `뉴스 데이터 누락: news_analysis 테이블 데이터 체크 필요합니다.`
        * kr_time={check_date} utc_time={utc_start_date}
        """
        raise ValueError(error_msg)

    unique_news_tickers = news_data["ticker"].unique().tolist()

    df_price = pd.DataFrame(
        database._select(
            table="stock_trend",
            columns=["ticker", "volume_change_1d"],
            **dict(ticker__in=unique_news_tickers, ctry="us"),
        )
    )

    top_6_tickers = df_price.nlargest(6, "volume_change_1d")["ticker"].tolist()

    try:
        database._update(
            table="news_analysis",
            sets={"is_top_story": False},
            **{
                "ctry": "US",
                "is_top_story": True,
            },
        )

        database._update(
            table="news_analysis",
            sets={"is_top_story": True},
            **{
                "ctry": "US",
                "date__gte": utc_start_date,
                "date__lt": utc_end_date,
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


# if __name__ == "__main__":
#     from app.core.logging.config import configure_logging

#     configure_logging()

#     renewal_kr_run_news_batch()
#     renewal_us_run_news_batch()

# run_news_batch(ctry="US", date="20250217")
