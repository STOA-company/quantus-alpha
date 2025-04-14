from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel, Field


class StockInfo(BaseModel):
    introduction: str | None = Field(description="주식 소개")
    homepage_url: str | None = Field(description="홈페이지 주소")
    ceo_name: str | None = Field(description="대표자 이름")
    establishment_date: str | None = Field(description="설립일")
    listing_date: str | None = Field(description="상장일")


class Indicators(BaseModel):
    per: Optional[float] = Field(default=None, description="PER")
    industry_per: Optional[float] = Field(default=None, description="산업 평균 PER")
    pbr: Optional[float] = Field(default=None, description="PBR")
    industry_pbr: Optional[float] = Field(default=None, description="산업 평균 PBR")
    roe: Optional[float] = Field(default=None, description="ROE")
    industry_roe: Optional[float] = Field(default=None, description="산업 평균 ROE")
    financial_data: Optional[str] = Field(default=None, description="재무 현황")
    price_trend: Optional[str] = Field(default=None, description="주가 추세")
    market_situation: Optional[str] = Field(default=None, description="시장 상황")
    industry_situation: Optional[str] = Field(default=None, description="업종 상황")


class SimilarStock(BaseModel):
    ticker: str
    name: str
    ctry: str
    current_price: Optional[float]
    current_price_rate: Optional[float]


class FearGreedIndexItem(BaseModel):
    fear_greed_index: int
    last_close: str
    last_week: str
    last_month: str
    last_year: str


class FearGreedIndexResponse(BaseModel):
    kor_stock: FearGreedIndexItem
    us_stock: FearGreedIndexItem


@dataclass
class StabilityThreshold:
    GOOD: float
    BAD: float


@dataclass
class StabilityTypeInfo:
    db_column: str  # 데이터베이스 컬럼명
    api_field: str  # API 응답 필드명
    description: str  # 지표 설명
    threshold: StabilityThreshold  # 임계값
