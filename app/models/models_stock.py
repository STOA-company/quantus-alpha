from pydantic import model_validator
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import Column, Date, Float, Integer, String, Boolean

from app.database.crud import Base


class StockInformation(Base):
    __tablename__ = "stock_information"

    # Required Columns
    ticker = Column(String(20), primary_key=True, nullable=False, comment="종목 티커")
    logo_image = Column(String(255), nullable=True, comment="로고 이미지 URL")
    kr_name = Column(String(100), nullable=True, comment="한글 종목명")
    en_name = Column(String(100), nullable=True, comment="영문 종목명")
    ctry = Column(String(20), nullable=True, comment="국가")
    market = Column(String(100), nullable=True, comment="거래소")

    # Index membership flags
    is_kospi_100 = Column(Boolean, default=True, comment="코스피 100 편입 여부")
    is_kospi_150 = Column(Boolean, default=True, comment="코스피 150 편입 여부")
    is_kospi_200 = Column(Boolean, default=True, comment="코스피 200 편입 여부")
    is_kosdaq_150 = Column(Boolean, default=True, comment="코스닥 150 편입 여부")
    is_krx_300 = Column(Boolean, default=True, comment="KRX 300 편입 여부")
    is_snp_500 = Column(Boolean, default=True, comment="S&P 500 편입 여부")
    is_nasdaq_100 = Column(Boolean, default=True, comment="나스닥 100 편입 여부")

    # GICS Sector information (nullable)
    sector_1 = Column(String(100), nullable=True, comment="GICS Level 1 (11개 섹터)")
    sector_2 = Column(String(100), nullable=True, comment="GICS Level 2 (25개 섹터)")
    sector_3 = Column(String(100), nullable=True, comment="GICS Level 3 (74개 섹터)")
    sector_4 = Column(String(100), nullable=True, comment="GICS Level 4 (163개 섹터)")

    # Additional company information (nullable)
    homepage_url = Column(String(255), nullable=True, comment="회사 홈페이지 URL")
    ceo = Column(String(100), nullable=True, comment="대표이사")
    establishment_date = Column(Date, nullable=True, comment="설립일")
    listing_date = Column(Date, nullable=True, comment="상장일")

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


class StockFactor(Base):
    __tablename__ = "stock_factor"

    # Primary Key
    ticker = Column(String(20), primary_key=True, nullable=False, comment="종목 코드")

    # Market Data
    market_cap = Column(Float, nullable=True, comment="시가총액")
    shared_outstanding = Column(Integer, nullable=True, comment="상장주식수")
    last_close = Column(Float, nullable=True, comment="전일 종가")
    week_52_high = Column(Float, nullable=True, comment="52주 최고가")
    week_52_low = Column(Float, nullable=True, comment="52주 최저가")

    # Financial Ratios
    per = Column(Float, nullable=True, comment="주가수익비율(PER)")
    pbr = Column(Float, nullable=True, comment="주가순자산비율(PBR)")
    roe = Column(Float, nullable=True, comment="자기자본이익률(ROE)")

    # Status Indicators
    finance_status = Column(Integer, nullable=True, comment="재무현황 (1: 좋음, 2: 보통, 3: 나쁨)")
    stock_trend = Column(Integer, nullable=True, comment="주식 추세")
    market_condition = Column(Integer, nullable=True, comment="시장 상황")
    industry_condition = Column(Integer, nullable=True, comment="업종 상황")

    # Validations
    @model_validator(mode="after")
    def validate_52_week_range(self) -> "StockFactor":
        if self.week_52_low > self.week_52_high:
            raise ValueError("52주 최저가는 52주 최고가보다 작아야 합니다")
        return self

    @hybrid_property
    def is_near_52_week_high(self) -> bool:
        """52주 최고가 대비 10% 이내인지 확인"""
        return self.last_close >= self.week_52_high * 0.9

    @hybrid_property
    def is_near_52_week_low(self) -> bool:
        """52주 최저가 대비 10% 이내인지 확인"""
        return self.last_close <= self.week_52_low * 1.1

    @hybrid_property
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
