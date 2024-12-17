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
