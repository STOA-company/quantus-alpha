from datetime import UTC, datetime

from sqlalchemy import CheckConstraint, DateTime, Float, Index, PrimaryKeyConstraint, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.models_base import Base


class etf_us_1d(Base):
    __tablename__ = "etf_us_1d"
    __table_args__ = (PrimaryKeyConstraint("Ticker", "Date"),)

    Ticker: Mapped[String] = mapped_column(String(length=20), nullable=False)
    Date: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    Open: Mapped[Float] = mapped_column(Float, nullable=True)
    High: Mapped[Float] = mapped_column(Float, nullable=True)
    Low: Mapped[Float] = mapped_column(Float, nullable=True)
    Close: Mapped[Float] = mapped_column(Float, nullable=True)
    Volume: Mapped[Float] = mapped_column(Float, nullable=True)
    Bid: Mapped[Float] = mapped_column(Float, nullable=True)
    Ask: Mapped[Float] = mapped_column(Float, nullable=True)
    Market: Mapped[String] = mapped_column(String(length=20), nullable=True)
    MarketCap: Mapped[Float] = mapped_column(Float, nullable=True)
    NumShrs: Mapped[Float] = mapped_column(Float, nullable=True)

    def __repr__(self) -> str:
        return f"etf_kr_1d(Ticker={self.Ticker!r}, Date={self.Date!r})"

    def __str__(self) -> str:
        return self.__repr__()


class etf_kr_1d(Base):
    __tablename__ = "etf_kr_1d"
    __table_args__ = (PrimaryKeyConstraint("Ticker", "Date"),)

    Ticker: Mapped[String] = mapped_column(String(length=20), nullable=False)
    Date: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    Open: Mapped[Float] = mapped_column(Float, nullable=True)
    High: Mapped[Float] = mapped_column(Float, nullable=True)
    Low: Mapped[Float] = mapped_column(Float, nullable=True)
    Close: Mapped[Float] = mapped_column(Float, nullable=True)
    Volume: Mapped[Float] = mapped_column(Float, nullable=True)
    Bid: Mapped[Float] = mapped_column(Float, nullable=True)
    Ask: Mapped[Float] = mapped_column(Float, nullable=True)
    Market: Mapped[String] = mapped_column(String(length=20), nullable=True)
    MarketCap: Mapped[Float] = mapped_column(Float, nullable=True)
    NumShrs: Mapped[Float] = mapped_column(Float, nullable=True)

    def __repr__(self) -> str:
        return f"etf_kr_1d(Ticker={self.Ticker!r}, Date={self.Date!r})"

    def __str__(self) -> str:
        return self.__repr__()


class etf_top_holdings(Base):
    __tablename__ = "etf_top_holdings"
    __table_args__ = (PrimaryKeyConstraint("ticker", "top_holdings"), Index("idx_ticker", "ticker"))

    ticker: Mapped[String] = mapped_column(String(length=20), nullable=False)
    top_holdings: Mapped[String] = mapped_column(String(length=20), nullable=False)
    weight: Mapped[Float] = mapped_column(Float, CheckConstraint("weight >= 0 AND weight <= 100"), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
