from pydantic import model_validator
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import Column, Date, DateTime, Float, Integer, String, Boolean

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

class StockTrend(Base):
    __tablename__ = "stock_trend"

    ticker = Column(String(20), primary_key=True, nullable=False, comment="종목 코드")
    last_updated = Column(DateTime, nullable=False, index=True, comment="마지막 업데이트 시간")
    ko_name = Column(String(100), nullable=True, comment="종목 한글명")
    en_name = Column(String(100), nullable=True, comment="종목 영문명")
    market = Column(String(10), nullable=False, index=True, comment="시장 구분")
    
    # 현재가
    current_price = Column(Float, nullable=False, comment="현재가")
    prev_close = Column(Float, nullable=False, comment="전일종가")
    
    # 등락률
    change_1m = Column(Float, nullable=True, comment="실시간 등락률")
    change_1d = Column(Float, nullable=True, comment="1일 등락률")
    change_1w = Column(Float, nullable=True, comment="1주 등락률")
    change_1mo = Column(Float, nullable=True, comment="1개월 등락률")
    change_6mo = Column(Float, nullable=True, comment="6개월 등락률")
    change_1y = Column(Float, nullable=True, comment="1년 등락률")
    
    # 거래량
    volume_1m = Column(Float, nullable=True, comment="1분 거래량 비율")
    volume_1d = Column(Float, nullable=True, comment="1일 거래량 비율")
    volume_1w = Column(Float, nullable=True, comment="1주 거래량 비율")
    volume_1mo = Column(Float, nullable=True, comment="1개월 거래량 비율")
    volume_6mo = Column(Float, nullable=True, comment="6개월 거래량 비율")
    volume_1y = Column(Float, nullable=True, comment="1년 거래량 비율")
    
    # 거래대금
    volume_change_1m = Column(Float, nullable=True, comment="1분 거래대금")
    volume_change_1d = Column(Float, nullable=True, comment="1일 거래대금")
    volume_change_1w = Column(Float, nullable=True, comment="1주 거래대금")
    volume_change_1mo = Column(Float, nullable=True, comment="1개월 거래대금")
    volume_change_6mo = Column(Float, nullable=True, comment="6개월 거래대금")
    volume_change_1y = Column(Float, nullable=True, comment="1년 거래대금")