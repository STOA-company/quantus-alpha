from sqlalchemy import Boolean, Column, DateTime, Float, String
from app.database.crud import Base


class News(Base):
    __tablename__ = "news_information"

    ticker = Column(String(20), primary_key=True, nullable=False, comment="종목 티커")
    ko_name = Column(String(100), nullable=True, comment="종목 한글명")
    en_name = Column(String(100), nullable=True, comment="종목 영문명")
    is_news = Column(Boolean, nullable=True, comment="뉴스 여부 (True: 뉴스, False: 공시)")
    date = Column(DateTime, nullable=True, comment="날짜, 시간")
    title = Column(String(255), nullable=True, comment="제목")
    summary = Column(String(255), nullable=True, comment="요약")
    emotion = Column(String(20), nullable=True, comment="감정")
    that_time_price = Column(Float, nullable=True, comment="해당 시간 종가")
    that_time_change = Column(Float, nullable=True, comment="해당 시간 변동률")
    volume = Column(Float, nullable=True, comment="거래량")
    volume_change = Column(Float, nullable=True, comment="거래대금")
    is_top_story = Column(Boolean, nullable=True, comment="주요 소식 선정 여부")
