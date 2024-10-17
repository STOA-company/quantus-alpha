from pydantic import BaseModel, Field
from typing import List, Dict, Any

class FinancialDataResponse(BaseModel):
    data: List[Dict[str, Any]] = Field(
        ...,
        example=[
            {"date": "2023-01-01", "revenue": 1000000, "expenses": 800000},
            {"date": "2023-02-01", "revenue": 1100000, "expenses": 850000}
        ]
    )