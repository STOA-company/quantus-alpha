import json
from datetime import datetime, timedelta
from app.core.exception.custom import DataNotFoundException
from app.database.crud import JoinInfo, database
from app.core.logging.config import get_logger
from app.modules.common.enum import TranslateCountry
from app.modules.common.utils import check_ticker_country_len_3
from .mapping import document_type_mapping


logger = get_logger(__name__)


class DisclosureService:
    def __init__(self):
        self.db = database

    async def get_disclosure(
        self, ticker: str, year: str = None, language: TranslateCountry = "ko", page: int = 1, size: int = 6
    ):
        if not year:
            year = datetime.now().strftime("%Y")

        ctry = "usa"
        if ticker:
            ctry = check_ticker_country_len_3(ticker)

        if ctry != "usa":
            raise DataNotFoundException(ticker=ctry, data_type="공시")

        table_name = f"{ctry}_disclosure"

        conditions = {}
        if ticker:
            conditions["ticker"] = ticker
        if year:
            conditions["filing_date__like"] = f"{year}%"
        conditions["ai_processed"] = 1

        columns = ["filing_id", "form_type", "ticker", "sec_url", "filing_date", "company_name", "ai_processed"]
        offset = (page - 1) * size
        total_count = self.db._count(table=table_name, **conditions)

        results = self.db._select(
            table=table_name,
            columns=columns,
            order="filing_date",
            ascending=False,
            limit=size,
            offset=offset,
            **conditions,
        )
        if not results:
            raise DataNotFoundException(ticker=ticker, data_type="공시")

        analysis_table_name = f"{ctry}_disclosure_analysis"
        analysis_conditions = {}
        analysis_conditions["filing_id__in"] = [result.filing_id for result in results]
        analysis_columns = ["filing_id", "ai_summary", "market_impact", "impact_reason", "key_points", "translated"]

        analysis_results = self.db._select(table=analysis_table_name, columns=analysis_columns, **analysis_conditions)

        translated_filing_ids = [result.filing_id for result in analysis_results if result.translated]
        translated_table_name = f"{analysis_table_name}_translation"
        translated_columns = ["filing_id", "ai_summary", "impact_reason", "key_points"]
        translated_conditions = {"filing_id__in": translated_filing_ids}

        translated_results = self.db._select(
            table=translated_table_name, columns=translated_columns, **translated_conditions
        )

        # emotion 카운트 초기화
        emotion_counts = {"positive": 0, "negative": 0, "neutral": 0}

        if not ticker:
            results_ticker = [result.ticker for result in results]
            ctry = "us"

            table_name = f"stock_{ctry}_1d"
            columns = ["Date", "Ticker", "Open", "Close"]
            # join_info 설정
            join_info = JoinInfo(
                primary_table=table_name,  # 메인 테이블 (stock_us_1d)
                secondary_table="stock_us_tickers",  # 조인할 테이블
                primary_column="Ticker",  # 메인 테이블의 조인 컬럼
                secondary_column="ticker",  # stock_us_tickers의 조인 컬럼
                columns=["korean_name" if language == "ko" else "english_name"],  # 조인 테이블에서 가져올 컬럼
            )

            conditions = {"Ticker__in": results_ticker, "Date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")}

            stock_results = self.db._select(table=table_name, columns=columns, join_info=join_info, **conditions)
            if stock_results:
                ticker_dict = {result.Ticker: result for result in stock_results}

        analysis_dict = {result.filing_id: result for result in analysis_results}
        translated_dict = {result.filing_id: result for result in translated_results}

        items = []
        for row in results:
            filing_date = getattr(row, "filing_date", None)
            date_str = filing_date.strftime("%Y-%m-%d %H:%M:%S") if filing_date else None

            analysis_data = analysis_dict.get(row.filing_id)
            translated_data = translated_dict.get(row.filing_id)
            if not ticker:
                ticker_data = ticker_dict.get(row.ticker)
            # emotion 카운트 업데이트
            emotion = analysis_data.market_impact.lower() if analysis_data and analysis_data.market_impact else "neutral"
            emotion_counts[emotion] += 1

            # key_points 파싱
            key_points_list = [None] * 5
            if analysis_data and analysis_data.key_points:
                try:
                    # translated가 True이면 translated_data의 key_points 사용
                    key_points = (
                        translated_data.key_points
                        if analysis_data.translated and translated_data
                        else analysis_data.key_points
                    )

                    # 이미 리스트인 경우와 문자열인 경우를 모두 처리
                    if isinstance(key_points, str):
                        key_points = json.loads(key_points)

                    # 최대 5개의 key points 저장
                    for i, point in enumerate(key_points[:5]):
                        key_points_list[i] = point

                except Exception as e:
                    logger.error(f"Failed to parse key_points for filing_id: {row.filing_id}, error: {str(e)}")

            if not ticker:
                price_change = (
                    round((ticker_data.Close - ticker_data.Open) / ticker_data.Open * 100, 2) if ticker_data else None
                )

            items.append(
                {
                    "title": row.company_name + " " + document_type_mapping.get(row.form_type, row.form_type),
                    "date": date_str,
                    "emotion": analysis_data.market_impact.lower() if analysis_data else None,
                    "impact_reason": translated_data.impact_reason
                    if analysis_data.translated
                    else analysis_data.impact_reason,
                    "key_points_1": key_points_list[0],
                    "key_points_2": key_points_list[1],
                    "key_points_3": key_points_list[2],
                    "key_points_4": key_points_list[3],
                    "key_points_5": key_points_list[4],
                    "summary": translated_data.ai_summary if analysis_data.translated else analysis_data.ai_summary,
                    "document_url": row.sec_url,
                    "name": ticker_data.korean_name if language == "ko" else ticker_data.english_name,
                    "price_change": price_change if not ticker else None,
                }
            )

        return {
            "data": items,
            "total_count": total_count,
            "total_pages": (total_count + size - 1) // size,
            "current_page": page,
            "offset": offset,
            "size": size,
            "positive_count": emotion_counts["positive"],
            "negative_count": emotion_counts["negative"],
            "neutral_count": emotion_counts["neutral"],
            "name": "None",
            "price_change": None,
        }


def get_disclosure_service() -> DisclosureService:
    return DisclosureService()
