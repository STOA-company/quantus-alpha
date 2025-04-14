from sqlalchemy import BigInteger, Boolean, Column, DateTime, Float, String, Text, UniqueConstraint
from sqlalchemy.schema import Index

from app.models.models_base import Base


class News(Base):
    __tablename__ = "news_information"

    __table_args__ = (
        # 종목 각 나라별 최신 뉴스 조회
        Index("idx_ticker_ctry", "ticker", "ctry", unique=False),
        # 나라별 최신순 조회
        Index("idx_ctry_date", "ctry", "date", unique=False),
        # Ticker별 최신순 조회
        Index("idx_ticker_date", "ticker", "date", unique=False),
        # 종목 단일 조회
        Index("idx_ticker", "ticker", unique=False),
    )
    id = Column(BigInteger, nullable=False, primary_key=True, unique=True, autoincrement=True, comment="뉴스 아이디")
    ticker = Column(String(20), nullable=False, comment="종목 티커")
    ko_name = Column(String(100), nullable=True, comment="종목 한글명")
    en_name = Column(String(100), nullable=True, comment="종목 영문명")
    ctry = Column(String(20), nullable=True, comment="국가")
    date = Column(DateTime, nullable=True, comment="날짜, 시간")
    title = Column(String(255), nullable=True, comment="제목")
    summary = Column(Text, nullable=True, comment="요약")
    emotion = Column(String(20), nullable=True, comment="감정")
    links = Column(Text, nullable=True, comment="링크")
    that_time_price = Column(Float, nullable=True, comment="해당 시간 종가")
    is_top_story = Column(Boolean, nullable=True, comment="주요 소식 선정 여부")
    is_exist = Column(Boolean, nullable=True, comment="DB 존재 여부")


class NewsAnalysis(Base):
    __tablename__ = "news_analysis"

    __table_args__ = (
        # collect_id와 ctry를 복합 unique 제약조건으로 설정
        UniqueConstraint("collect_id", "ctry", "lang", name="uix_collect_id_ctry_lang"),
        # 종목 각 나라별 최신 뉴스 조회
        Index("idx_ticker_lang", "ticker", "lang", unique=False),
        # 나라별 최신순 조회
        Index("idx_ctry_lang_date", "ctry", "lang", "date", unique=False),
        # Ticker별 최신순 조회
        Index("idx_ticker_date", "ticker", "date", unique=False),
        # 종목 단일 조회
        Index("idx_ticker", "ticker", unique=False),
    )

    id = Column(BigInteger, nullable=False, primary_key=True, unique=True, autoincrement=True, comment="뉴스 아이디")
    collect_id = Column(BigInteger, nullable=True, comment="수집 아이디")
    ticker = Column(String(20), nullable=True, comment="종목 티커")
    kr_name = Column(String(100), nullable=True, comment="종목 한글명")
    en_name = Column(String(100), nullable=True, comment="종목 영문명")
    ctry = Column(String(20), nullable=True, comment="국가")
    date = Column(DateTime, nullable=True, comment="날짜, 시간")
    title = Column(String(255), nullable=True, comment="제목")
    emotion = Column(String(20), nullable=True, comment="시장 영향")
    summary = Column(Text, nullable=True, comment="요약")
    impact_reason = Column(Text, nullable=True, comment="영향 이유")
    key_points = Column(Text, nullable=True, comment="주요 포인트")
    related_tickers = Column(Text, nullable=True, comment="관련 종목")
    url = Column(Text, nullable=True, comment="URL")
    that_time_price = Column(Float, nullable=True, comment="해당 시간 종가")
    is_top_story = Column(Boolean, nullable=True, comment="주요 소식 선정 여부")
    is_exist = Column(Boolean, nullable=True, comment="DB 존재 여부")
    is_related = Column(Boolean, nullable=True, default=True, comment="관련 종목 여부")
    lang = Column(String(20), nullable=True, comment="언어")
