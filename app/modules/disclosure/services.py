from datetime import datetime
from app.core.exception.custom import DataNotFoundException
from app.modules.common.enum import FinancialCountry
from app.database.crud import database
from app.core.logging.config import get_logger
from .mapping import document_type_mapping


logger = get_logger(__name__)


class DisclosureService:
    def __init__(self):
        self.db = database

    async def get_disclosure(self, ctry: FinancialCountry, ticker: str, year: str = None, page: int = 1, size: int = 6):
        if not year:
            year = datetime.now().strftime("%Y")

        if ctry != FinancialCountry.USA:
            raise DataNotFoundException(ticker=ctry.name, data_type="공시")

        table_name = f"{ctry.value.lower()}_disclosure"

        conditions = {}
        if ticker:
            conditions["ticker"] = ticker
        if year:
            conditions["filing_date__like"] = f"{year}%"

        columns = ["form_type", "ticker", "filing_date", "sec_url", "ai_processed", "company_name"]
        offset = (page - 1) * size

        results = self.db._select(
            table=table_name, columns=columns, order="filing_date", ascending=False, limit=size, **conditions
        )

        if not results:
            raise DataNotFoundException(ticker=ticker, data_type="공시")

        total_count = len(results)

        items = []
        for row in results:
            filing_date = getattr(row, "filing_date", None)
            # datetime 객체를 문자열로 변환
            date_str = filing_date.strftime("%Y-%m-%d %H:%M:%S") if filing_date else None

            items.append(
                {
                    "title": row.company_name + " " + document_type_mapping.get(row.form_type, row.form_type),
                    "date": date_str,
                    "summary": row.summary if row.ai_processed == 1 else "요약 없음",
                    "document_url": row.sec_url,
                    "document_type": row.form_type,
                }
            )

        return {
            "data": items,
            "total_count": total_count,
            "total_pages": (total_count + size - 1) // size,
            "current_page": page,
            "offset": offset,
            "size": size,
        }


def get_disclosure_service() -> DisclosureService:
    return DisclosureService()
