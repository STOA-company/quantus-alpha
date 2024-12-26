from pydantic import model_validator
from sqlalchemy import Column, Date, Float, String
from app.database.crud import Base


class Dividend(Base):
    __tablename__ = "dividend_information"

    ticker = Column(String(20), primary_key=True, nullable=False, comment="종목 티커")
    payment_date = Column(Date, nullable=True, comment="배당 지급일")
    ex_date = Column(Date, nullable=True, comment="배당 락일")
    per_share = Column(Float, nullable=True, comment="1주당 배당금")
    per_share_rate = Column(Float, nullable=True, comment="1주당 배당비율")
    growth_rate = Column(Float, nullable=True, comment="배당 성장률")
    yield_rate = Column(Float, nullable=True, comment="배당 수익률")

    @model_validator(mode="after")
    # 배당 락일은 배당 지급일보다 이전이어야 함
    def validate_ex_date(self) -> "Dividend":
        if self.ex_date < self.payment_date:
            raise ValueError("배당 락일은 배당 지급일보다 이전이어야 합니다")
        return self
