from sqlalchemy import Boolean, Column, DateTime, Float, String
from app.database.crud import Base
from sqlalchemy.schema import Index


class Disclosure(Base):
    __tablename__ = "disclosure_information"

    __table_args__ = (
        # 종목 각 나라별 최신 뉴스 조회
        Index("idx_ticker_ctry", "ticker", "ctry", unique=False),
        # 나라별 최신순 조회
        Index("idx_ctry_date", "ctry", "date", unique=False),
        # Ticker별 최신순 조회
        Index("idx_ticker_date", "ticker", "date", unique=False),
    )

    ticker = Column(String(20), primary_key=True, nullable=False, comment="종목 티커")
    ko_name = Column(String(100), nullable=True, comment="종목 한글명")
    en_name = Column(String(100), nullable=True, comment="종목 영문명")
    ctry = Column(String(20), nullable=True, comment="국가")
    date = Column(DateTime, nullable=True, comment="날짜, 시간")
    title = Column(String(255), nullable=True, comment="제목")
    summary = Column(String(255), nullable=True, comment="요약")
    impact_reason = Column(String(255), nullable=True, comment="영향 요인")
    key_points = Column(String(255), nullable=True, comment="주요 포인트")
    emotion = Column(String(20), nullable=True, comment="감정")
    form_type = Column(String(20), nullable=True, comment="형태")
    that_time_price = Column(Float, nullable=True, comment="해당 시간 종가")
    that_time_change = Column(Float, nullable=True, comment="해당 시간 변동률")
    volume = Column(Float, nullable=True, comment="거래량")
    volume_change = Column(Float, nullable=True, comment="거래대금")
    is_top_story = Column(Boolean, nullable=True, comment="주요 소식 선정 여부")
    is_exist = Column(Boolean, nullable=True, comment="DB 존재 여부")
