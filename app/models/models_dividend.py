from pydantic import model_validator
from sqlalchemy import Column, Date, Float, PrimaryKeyConstraint, String
from app.models.models_base import Base


class Dividend(Base):
    __tablename__ = "dividend_information"

    ticker = Column(String(20), nullable=False, comment="종목 티커")
    payment_date = Column(Date, nullable=False, comment="배당 지급일")
    ex_date = Column(Date, nullable=False, comment="배당 락일")  # nullable=False로 변경
    per_share = Column(Float, nullable=True, comment="1주당 배당금")
    per_share_rate = Column(Float, nullable=True, comment="1주당 배당비율")
    growth_rate = Column(Float, nullable=True, comment="배당 성장률")
    yield_rate = Column(Float, nullable=True, comment="배당 수익률")

    __table_args__ = (PrimaryKeyConstraint("ticker", "ex_date"),)

    @model_validator(mode="after")
    def validate_ex_date(self) -> "Dividend":
        if self.ex_date > self.payment_date:  # null 체크 제거 (nullable=False이므로)
            raise ValueError("배당 락일은 배당 지급일보다 이전이어야 합니다")
        return self
