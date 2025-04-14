from pydantic import BaseModel
from sqlalchemy import Float, String
from sqlalchemy.orm import Mapped, mapped_column

from app.modules.common.models import DateMixin


class StockKrFactor(BaseModel, DateMixin):
    __tablename__ = "stock_kr_factors"

    # PK
    ticker: Mapped[str] = mapped_column(String(20), primary_key=True)

    # 기본 정보
    name: Mapped[str] = mapped_column(String(100))
    prev_close: Mapped[float] = mapped_column(Float)

    # 가격 범위
    week_52_high: Mapped[float] = mapped_column(Float)
    week_52_low: Mapped[float] = mapped_column(Float)
    all_time_high: Mapped[float] = mapped_column(Float)
    all_time_low: Mapped[float] = mapped_column(Float)

    # 모멘텀 스코어
    momentum_1m: Mapped[float] = mapped_column(Float)
    momentum_3m: Mapped[float] = mapped_column(Float)
    momentum_6m: Mapped[float] = mapped_column(Float)
    momentum_12m: Mapped[float] = mapped_column(Float)

    # 수익률
    rate_of_change_10d: Mapped[float] = mapped_column(Float)
    rate_of_change_30d: Mapped[float] = mapped_column(Float)
    rate_of_change_60d: Mapped[float] = mapped_column(Float)
