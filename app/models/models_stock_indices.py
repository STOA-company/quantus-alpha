from sqlalchemy import Boolean, Column, DateTime, Float, PrimaryKeyConstraint, String
from app.models.models_base import Base


class StockIndices(Base):
    __tablename__ = "stock_indices"

    ticker = Column(String(100), primary_key=True, nullable=False, comment="종목 티커")
    is_open = Column(Boolean, nullable=True, comment="장 오픈 여부")
    date = Column(DateTime, nullable=True, comment="날짜")
    price = Column(Float, nullable=True, comment="현재가")
    change = Column(Float, nullable=True, comment="변동 금액")
    price_rate = Column(Float, nullable=True, comment="현재가 변동률")
    market = Column(String(100), nullable=True, comment="시장")
    included_indices = Column(String(100), nullable=True, comment="포함되어 있는 지수")
    rise_ratio = Column(Float, nullable=True, comment="급상승 비율")
    rise_soft_ratio = Column(Float, nullable=True, comment="약상승 비율")
    fall_ratio = Column(Float, nullable=True, comment="급하락 비율")
    fall_soft_ratio = Column(Float, nullable=True, comment="약하락 비율")
    unchanged_ratio = Column(Float, nullable=True, comment="보합 비율")


class StockIndices1m(Base):
    __tablename__ = "stock_indices_1m"
    __table_args__ = (PrimaryKeyConstraint("ticker", "date"),)

    ticker = Column(String(10), nullable=False, comment="종목 티커")
    date = Column(DateTime, nullable=True, comment="날짜")
    open = Column(Float, nullable=True, comment="시가")
    high = Column(Float, nullable=True, comment="고가")
    low = Column(Float, nullable=True, comment="저가")
    close = Column(Float, nullable=True, comment="종가")
    volume = Column(Float, nullable=True, comment="거래량")
    change = Column(Float, nullable=True, comment="변동 금액")
    change_rate = Column(Float, nullable=True, comment="변동률")


class StockIndices1d(Base):
    __tablename__ = "stock_indices_1d"
    __table_args__ = (PrimaryKeyConstraint("ticker", "date"),)

    ticker = Column(String(10), nullable=False, comment="종목 티커")
    date = Column(DateTime, nullable=False, comment="날짜")
    open = Column(Float, nullable=True, comment="시가")
    high = Column(Float, nullable=True, comment="고가")
    low = Column(Float, nullable=True, comment="저가")
    close = Column(Float, nullable=True, comment="종가")
    volume = Column(Float, nullable=True, comment="거래량")
