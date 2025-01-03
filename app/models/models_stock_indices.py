from sqlalchemy import Boolean, Column, DateTime, Float, String
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
    rise_ratio = Column(Float, nullable=True, comment="상승 비율")
    fall_ratio = Column(Float, nullable=True, comment="하락 비율")
    unchanged_ratio = Column(Float, nullable=True, comment="보합 비율")
