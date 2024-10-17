from pydantic import BaseModel, Field
from typing import List, Dict, Any

class PriceDataResponse(BaseModel):
    data: List[Dict[str, Any]] = Field(
        ...,
        example=[
            {"date": "2023-01-01", "open": 100, "high": 105, "low": 98, "close": 102, "volume": 1000000},
            {"date": "2023-01-02", "open": 102, "high": 107, "low": 101, "close": 106, "volume": 1200000}
        ]
    )