from pydantic import model_validator
from sqlalchemy import Column, Date, Float, Index, Integer, String, UniqueConstraint

from app.models.models_base import Base


class Dividend(Base):
    __tablename__ = "dividend_information"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(20), nullable=False, comment="종목 티커")  #
    payment_date = Column(Date, nullable=False, comment="배당 지급일")  #
    ex_date = Column(Date, nullable=False, comment="배당 락일")  #
    per_share = Column(Float, nullable=True, comment="1주당 배당금")  #
    yield_rate = Column(Float, nullable=True, comment="배당 수익률")  # 배당금 / 종가

    __table_args__ = (
        UniqueConstraint("ticker", "ex_date", "payment_date", name="uix_ticker_ex_date_payment_date"),
        Index("idx_ticker_ex_date", "ticker", "ex_date"),
    )

    @model_validator(mode="after")
    def validate_ex_date(self) -> "Dividend":
        if self.ex_date > self.payment_date:
            raise ValueError("배당 락일은 배당 지급일보다 이전이어야 합니다")
        return self
