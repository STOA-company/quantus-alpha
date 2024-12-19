import json
from datetime import datetime
from app.core.exception.custom import DataNotFoundException
from app.database.crud import database
from app.core.logging.config import get_logger
from app.modules.common.utils import check_ticker_country_len_3
from .mapping import document_type_mapping


logger = get_logger(__name__)


class DisclosureService:
    def __init__(self):
        self.db = database

    async def get_disclosure(self, ticker: str, year: str = None, page: int = 1, size: int = 6):
        if not year:
            year = datetime.now().strftime("%Y")
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
        analysis_columns = ["filing_id", "ai_summary", "market_impact", "impact_reason", "key_points"]
        analysis_results = self.db._select(table=analysis_table_name, columns=analysis_columns, **analysis_conditions)

        analysis_dict = {result.filing_id: result for result in analysis_results}

        # emotion 카운트 초기화
        emotion_counts = {"positive": 0, "negative": 0, "neutral": 0}

        items = []
        for row in results:
            filing_date = getattr(row, "filing_date", None)
            date_str = filing_date.strftime("%Y-%m-%d %H:%M:%S") if filing_date else None

            analysis_data = analysis_dict.get(row.filing_id)

            # emotion 카운트 업데이트
            emotion = analysis_data.market_impact.lower() if analysis_data and analysis_data.market_impact else "neutral"
            emotion_counts[emotion] += 1

            # key_points 파싱
            key_points_list = [None] * 5
            if analysis_data and analysis_data.key_points:
                try:
                    parsed_key_points = json.loads(analysis_data.key_points)
                    for i, point in enumerate(parsed_key_points[:5]):
                        key_points_list[i] = point
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse key_points for filing_id: {row.filing_id}")

            items.append(
                {
                    "title": row.company_name + " " + document_type_mapping.get(row.form_type, row.form_type),
                    "date": date_str,
                    "emotion": analysis_data.market_impact.lower() if analysis_data else None,
                    "impact_reason": analysis_data.impact_reason if analysis_data else None,
                    "key_points_1": key_points_list[0],
                    "key_points_2": key_points_list[1],
                    "key_points_3": key_points_list[2],
                    "key_points_4": key_points_list[3],
                    "key_points_5": key_points_list[4],
                    "summary": analysis_data.ai_summary if analysis_data else "요약 없음",
                    "document_url": row.sec_url,
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
        }


def get_disclosure_service() -> DisclosureService:
    return DisclosureService()
