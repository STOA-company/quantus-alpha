from typing import List
from pydantic import BaseModel, Field


class DividendDetail(BaseModel):
    year: int = Field(description="연도")
    ex_dividend_date: str = Field(description="배당락일")
    dividend_payment_date: str = Field(description="배당지급일")
    dividend_per_share: float = Field(description="1주당 배당금")
    dividend_yield: float = Field(description="수익률")


class DividendItem(BaseModel):
    last_year_dividend_count: int = Field(description="전년도 배당 건수")
    last_dividend_per_share: float = Field(description="직전 1주당 배당금")
    last_dividend_ratio: float = Field(description="직전 1주당 배당비율")
    last_dividend_growth_rate: float = Field(description="직전 배당 성장률")
    detail: List[DividendDetail] = Field(description="배당 상세 정보")
