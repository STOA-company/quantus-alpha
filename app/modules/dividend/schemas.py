from typing import List, Optional
from pydantic import BaseModel, Field


class DividendDetail(BaseModel):
    ex_dividend_date: str = Field(description="배당락일")
    dividend_payment_date: str = Field(description="배당지급일")
    dividend_per_share: float = Field(description="1주당 배당금")
    dividend_yield: float | None = Field(description="수익률")


class DividendYearResponse(BaseModel):
    year: int = Field(description="연도")
    dividend_detail: List[DividendDetail] = Field(description="연도별 배당 상세 정보")


class DividendItem(BaseModel):
    ticker: str = Field(description="종목 코드")
    name: str = Field(description="종목 이름")
    ctry: str = Field(description="국가 코드")
    last_year_dividend_count: Optional[int] = Field(default=None, description="전년도 배당 건수")
    last_year_dividend_date: Optional[List[str]] = Field(default=None, description="전년도 배당 일자")
    last_dividend_per_share: Optional[float] = Field(default=None, description="직전 1주당 배당금")
    last_dividend_ratio: Optional[float] = Field(default=None, description="직전 1주당 배당비율")
    last_dividend_growth_rate: Optional[float] = Field(default=None, description="직전 배당 성장률")
    detail: List[DividendYearResponse] = Field(description="배당 상세 정보")
