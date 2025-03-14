from typing import Literal
import pandas as pd
from app.database.crud import database
from app.utils.date_utils import now_utc
from sqlalchemy import text
from app.core.logging.config import get_logger

logger = get_logger(__name__)


def run_disclosure_batch(ctry: Literal["KR", "US"], date: str = None):
    """
    공시 데이터 배치 처리
    Args:
        ctry (str): 처리할 나라 (KR 또는 US)
        date (str): 처리할 날짜 (YYYYMMDD)
    Returns:
        int: 처리한 레코드 수
    """
    logger.info(f"Starting disclosure batch processing for {ctry} on {date}(UTC)")
    try:
        if date:
            check_date = pd.to_datetime(date, format="%Y%m%d").date()
        else:
            check_date = now_utc()
    except ValueError as e:
        raise ValueError(f"Invalid date format. Expected YYYYMMDD, got: {date}") from e

    check_date_str = check_date.strftime("%Y-%m-%d")

    # 테이블 이름 및 국가별 특성 설정
    if ctry == "KR":
        ctry_disclosure = "kor_disclosure"
        ctry_disclosure_analysis = "kor_disclosure_analysis"
        ctry_disclosure_analysis_translation = "kor_disclosure_analysis_translation"
        extra_cols = "d.category_type, d.extra_info,"
    elif ctry == "US":
        ctry_disclosure = "usa_disclosure"
        ctry_disclosure_analysis = "usa_disclosure_analysis"
        ctry_disclosure_analysis_translation = "usa_disclosure_analysis_translation"
        extra_cols = ""
    else:
        raise ValueError(f"Invalid country code. Expected KR or US, got: {ctry}")

    # 쿼리 작성 - 언어 조건 없이 모든 번역 데이터 가져오기
    query = f"""
    SELECT
        a.filing_id, a.ai_summary as en_summary, a.market_impact as en_market_impact, a.impact_reason as en_impact_reason, a.key_points as en_key_points,
        d.form_type, d.ticker, d.url, d.filing_date, {extra_cols}
        t.ai_summary as kr_summary, t.impact_reason as kr_impact_reason, t.key_points as kr_key_points, t.lang
    FROM {ctry_disclosure_analysis_translation} t
    LEFT JOIN {ctry_disclosure} d ON t.filing_id = d.filing_id
    LEFT JOIN {ctry_disclosure_analysis} a ON t.filing_id = a.filing_id
    WHERE DATE(t.created_at) = :check_date
    """
    # TODO:: ctry_disclosure_analysis 테이블에서 market_impact, impact_reason, key_points 제거하기 / 제거 후 db에서 컬럼 삭제

    # 쿼리 실행
    result = database._execute(text(query), {"check_date": check_date_str})

    # 결과를 DataFrame으로 변환
    df_disclosure = pd.DataFrame(result.fetchall())

    if df_disclosure.empty:
        error_msg = f"""
        `공시 데이터 누락: {ctry_disclosure_analysis_translation} 테이블 데이터 체크 필요합니다.`
        * check_date: {check_date_str}(UTC)
        """
        raise ValueError(error_msg)

    # 티커에 국가별 접두사 추가 (한국만 해당)
    if ctry == "KR" and not df_disclosure.empty:
        # A가 이미 포함되어 있는지 확인
        if not df_disclosure["ticker"].str.startswith("A").any():
            df_disclosure["ticker"] = "A" + df_disclosure["ticker"]

    # 고유 티커 목록 추출
    ticker_list = df_disclosure["ticker"].unique().tolist()

    # 종목 정보 조회
    df_stock_data = pd.DataFrame(
        database._select(
            table="stock_information",
            columns=["ticker", "kr_name", "en_name"],
            **dict(ticker__in=ticker_list, is_activate=True),
        )
    )

    if df_stock_data.empty:
        error_msg = f"""
        `공시 업데이트 배치: stock_information 테이블 데이터 체크 필요합니다.`
        * check_date: {check_date_str}(UTC)
        * ticker_list: {ticker_list}
        """
        raise ValueError(error_msg)

    # 존재하는 티커 목록
    existing_tickers = df_stock_data["ticker"].unique().tolist()

    # 데이터 병합 후 필수 컬럼 추가
    df_merge = pd.merge(df_disclosure, df_stock_data, on="ticker", how="left")
    df_merge["ctry"] = ctry
    df_merge["is_exist"] = df_merge["ticker"].isin(existing_tickers)

    # 누락된 컬럼 확인 및 추가
    for col in ["category_type", "extra_info"]:
        if col not in df_merge.columns:
            logger.debug(f"{col} 컬럼이 없어 None으로 추가합니다.")
            df_merge[col] = None

    if df_merge["kr_name"].isna().any() or df_merge["en_name"].isna().any():
        logger.warning("일부 종목에 이름 정보가 누락되었습니다.")
        logger.warning(
            df_merge[df_merge["kr_name"].isna() | df_merge["en_name"].isna()][["ticker", "kr_name", "en_name"]]
        )
        df_merge["kr_name"] = df_merge["kr_name"].fillna("")
        df_merge["en_name"] = df_merge["en_name"].fillna("")

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
            "category_type": row["category_type"],
            "extra_info": row["extra_info"],
            "that_time_price": 0,
            "is_top_story": False,
            "is_exist": row["is_exist"],
            "lang": row["lang"],
        }
        disclosure_records.append(disclosure_record)

    if disclosure_records:
        logger.info(f"총 입력할 레코드 수: {len(disclosure_records)}")

        try:
            processed = batch_insert(disclosure_records, ctry)
            logger.info(f"모든 데이터 입력 완료: {processed}개 처리됨")
        except Exception as e:
            logger.error(f"데이터베이스 입력 중 오류 발생: {str(e)}")
            error_msg = f"""
            {ctry} 공시 데이터 처리 실패
            * 처리 날짜: {check_date}
            * 오류: {str(e)}
            """
            raise ValueError(error_msg)

    logger.info(f"Successfully processed {len(disclosure_records)} records for {ctry}")
    return len(disclosure_records)


def batch_insert(records: list, ctry: Literal["KR", "US"], batch_size: int = 500):
    """
    레코드를 배치 크기로 나누어 삽입하는 함수
    모든 국가에서 filing_id와 lang 조합으로 중복 확인

    Args:
        records (list): 삽입할 레코드 목록
        ctry (str): 국가 코드 (KR 또는 US)
        batch_size (int): 배치 크기

    Returns:
        int: 처리된 레코드 수
    """

    def replace_nan(records_batch):
        return [{k: (None if pd.isna(v) else v) for k, v in record.items()} for record in records_batch]

    # 기존 filing_id와 lang 조합 조회 (chunk 단위로)
    chunk_size = 5000
    existing_combinations = set()
    offset = 0

    while True:
        chunk = pd.DataFrame(
            database._select(
                table="disclosure_information",
                columns=["filing_id", "lang"],
                limit=chunk_size,
                offset=offset,
                **dict(ctry=ctry),
            )
        )
        if chunk.empty:
            break

        # filing_id와 lang 조합 저장 (KR, US 모두 동일하게 처리)
        for _, row in chunk.iterrows():
            if not pd.isna(row["filing_id"]) and not pd.isna(row["lang"]):
                existing_combinations.add((row["filing_id"], row["lang"]))

        offset += chunk_size

    total = len(records)
    processed = 0
    skipped = 0

    # 배치 단위로 처리
    for i in range(0, total, batch_size):
        batch_records = records[i : i + batch_size]
        batch_df = pd.DataFrame(batch_records)

        # 중복 체크 (filing_id와 lang 조합 기준)
        unique_records = []
        for _, row in batch_df.iterrows():
            combo = (row["filing_id"], row["lang"])
            if combo not in existing_combinations:
                unique_records.append(row.to_dict())
                existing_combinations.add(combo)
            else:
                skipped += 1

        if unique_records:
            cleaned_batch = replace_nan(unique_records)
            try:
                database._insert(table="disclosure_information", sets=cleaned_batch)
                processed += len(cleaned_batch)
            except Exception as e:
                logger.error(f"배치 처리 중 오류: {str(e)}")
                raise

        logger.info(f"진행률: {i+len(batch_records)}/{total} (처리: {processed}, 스킵: {skipped})")

    logger.info(f"배치 처리 완료: 총 {total}개 중 {processed}개 처리, {skipped}개 스킵")
    return processed


# if __name__ == "__main__":
#     from app.core.logging.config import configure_logging

#     configure_logging()
#     run_disclosure_batch(ctry="KR")
