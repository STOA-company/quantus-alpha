from sqlalchemy import Column, DateTime, Float, Integer, String

from app.models.models_base import Base


class StockTrend(Base):
    __tablename__ = "stock_trend"

    ticker = Column(String(20), primary_key=True, nullable=False, comment="종목 코드")
    last_updated = Column(DateTime, nullable=True, index=True, comment="마지막 업데이트 시간")
    ctry = Column(String(10), nullable=True, index=True, comment="국가 구분")
    market = Column(String(10), nullable=True, index=True, comment="시장 구분")
    ko_name = Column(String(100), nullable=True, comment="종목 한글명")
    en_name = Column(String(100), nullable=True, comment="종목 영문명")

    # 현재가
    current_price = Column(Float, nullable=True, comment="현재가")
    prev_close = Column(Float, nullable=True, comment="전일종가")

    change_sign_1m = Column(
        Integer, nullable=True, comment="실시간 대비 등락 부호"
    )  # 1 : 상한, 2 : 상승, 3 : 보합, 4 : 하락, 5 : 하한

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
