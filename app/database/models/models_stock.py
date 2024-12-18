from datetime import date
from typing import Optional
from pydantic import BaseModel, HttpUrl, Field, model_validator
from pydantic import computed_field

from app.modules.common.enum import FinanceStatus


class StockInformation(BaseModel):
    # Required fields
    ticker: str = Field(description="종목 티커")
    logo_image: HttpUrl = Field(description="로고 이미지 URL")
    name: str = Field(description="종목명")
    ctry: str = Field(description="국가")
    market: str = Field(description="거래소")

    # Index membership flags
    is_kospi_100: bool = Field(default=False, description="코스피 100 편입 여부")
    is_kospi_150: bool = Field(default=False, description="코스피 150 편입 여부")
    is_kospi_200: bool = Field(default=False, description="코스피 200 편입 여부")
    is_kosdaq_150: bool = Field(default=False, description="코스닥 150 편입 여부")
    is_krx_300: bool = Field(default=False, description="KRX 300 편입 여부")
    is_snp_500: bool = Field(default=False, description="S&P 500 편입 여부")
    is_nasdaq_100: bool = Field(default=False, description="나스닥 100 편입 여부")

    # GICS Sector information (nullable)
    sector_1: Optional[str] = Field(None, description="GICS Level 1 (11개 섹터)")
    sector_2: Optional[str] = Field(None, description="GICS Level 2 (25개 섹터)")
    sector_3: Optional[str] = Field(None, description="GICS Level 3 (74개 섹터)")
    sector_4: Optional[str] = Field(None, description="GICS Level 4 (163개 섹터)")

    # Additional company information (nullable)
    homepage_url: Optional[HttpUrl] = Field(None, description="회사 홈페이지 URL")
    ceo: Optional[str] = Field(None, description="대표이사")
    establishment_date: Optional[date] = Field(None, description="설립일")
    listing_date: date = Field(description="상장일")

    class Config:
        json_schema_extra = {
            "example": {
                "ticker": "005930",
                "logo_image": "https://example.com/logo/005930.png",
                "name": "삼성전자",
                "ctry": "KR",
                "market": "KOSPI",
                "listing_date": "1975-06-11",
                "is_kospi_100": True,
                "is_kospi_150": True,
                "is_kospi_200": True,
                "is_kosdaq_150": False,
                "is_krx_300": True,
                "is_snp_500": False,
                "is_nasdaq_100": False,
                "sector_1": "Information Technology",
                "sector_2": "Technology Hardware & Equipment",
                "sector_3": "Technology Hardware, Storage & Peripherals",
                "sector_4": "Electronic Equipment & Components",
                "homepage_url": "https://www.samsung.com",
                "ceo": "경계현",
                "establishment_date": "1969-01-13",
            }
        }


class StockMarketData(BaseModel):
    # Primary Key
    ticker: str = Field(description="종목 코드")

    # Market Data
    market_cap: float = Field(description="시가총액", ge=0)
    shared_outstanding: int = Field(description="상장주식수", gt=0)
    last_close: float = Field(description="전일 종가", ge=0)
    week_52_high: float = Field(description="52주 최고가", ge=0)
    week_52_low: float = Field(description="52주 최저가", ge=0)

    # Financial Ratios
    per: Optional[float] = Field(None, description="주가수익비율(PER)", gt=0)
    pbr: Optional[float] = Field(None, description="주가순자산비율(PBR)", gt=0)
    roe: Optional[float] = Field(None, description="자기자본이익률(ROE)")

    # Status Indicators
    finance_status: FinanceStatus = Field(description="재무현황 (1: 좋음, 2: 보통, 3: 나쁨)")
    stock_trend: int = Field(description="주식 추세")
    market_condition: int = Field(description="시장 상황")
    industry_condition: int = Field(description="업종 상황")

    # Validations
    @model_validator(mode="after")
    def validate_52_week_range(self) -> "StockMarketData":
        if self.week_52_low > self.week_52_high:
            raise ValueError("52주 최저가는 52주 최고가보다 작아야 합니다")
        return self

    # Computed Properties
    @computed_field
    @property
    def is_near_52_week_high(self) -> bool:
        """52주 최고가 대비 10% 이내인지 확인"""
        return self.last_close >= self.week_52_high * 0.9

    @computed_field
    @property
    def is_near_52_week_low(self) -> bool:
        """52주 최저가 대비 10% 이내인지 확인"""
        return self.last_close <= self.week_52_low * 1.1

    @computed_field
    @property
    def market_cap_billions(self) -> float:
        """시가총액을 10억 단위로 변환"""
        return self.market_cap / 1_000_000_000

    model_config = {
        "json_schema_extra": {
            "example": {
                "ticker": "005930",
                "market_cap": 448300000000000,
                "shared_outstanding": 5969782550,
                "last_close": 75100,
                "week_52_high": 77800,
                "week_52_low": 57200,
                "per": 14.2,
                "pbr": 1.35,
                "roe": 9.5,
                "finance_status": 1,
                "stock_trend": 1,
                "market_condition": 2,
                "industry_condition": 2,
            }
        }
    }
