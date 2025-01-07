from sqlalchemy import Column, ForeignKey, Integer, String, Text, Boolean, BigInteger, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from app.models.models_base import Base, BaseMixin


class AlphafinderUser(BaseMixin, Base):
    __tablename__ = "alphafinder_user"
    __table_args__ = (UniqueConstraint("nickname"), {"extend_existing": True})

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    sns_id: Mapped[String] = mapped_column(String(length=1000), nullable=False)
    sns_type: Mapped[String] = mapped_column(String(length=100), nullable=False)
    email: Mapped[String] = mapped_column(String(length=100), nullable=False)
    nickname: Mapped[String] = mapped_column(String(length=100), nullable=False)
    push_token: Mapped[Text] = mapped_column(Text, nullable=True)
    is_competing: Mapped[Boolean] = mapped_column(Boolean, server_default="0")
    magic_split_issued: Mapped[Boolean] = mapped_column(Boolean, server_default="0")
    is_opt_out: Mapped[Boolean] = mapped_column(Boolean, nullable=True)
    signup_platform: Mapped[String] = mapped_column(String(length=100), nullable=False)
    is_relationship: Mapped[Boolean] = mapped_column(Boolean, server_default="0")

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, nickname={self.nickname!r})"

    def __str__(self) -> str:
        return f"User(id={self.id!r}, nickname={self.nickname!r})"


class AlphafinderWatchlist(BaseMixin, Base):
    __tablename__ = "alphafinder_watchlist"
    __table_args__ = {"extend_existing": True}

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("alphafinder_user.id", ondelete="CASCADE"), nullable=True, index=True
    )
    ticker: Mapped[String] = mapped_column(
        String(100), ForeignKey("stock_information.ticker", ondelete="CASCADE"), nullable=True, index=True
    )

    def __repr__(self) -> str:
        return f"Watchlist(id={self.id!r}, user_id={self.user_id!r}, ticker={self.ticker!r})"

    def __str__(self) -> str:
        return f"Watchlist(id={self.id!r}, user_id={self.user_id!r}, ticker={self.ticker!r})"


class AlphaUserRefreshToken(BaseMixin, Base):
    __tablename__ = "alphafinder_user_refresh_token"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    access_token_hash = Column(String(length=64), index=True, nullable=False)
    refresh_token = Column(String(length=1000), nullable=False)
    access_token = Column(String(length=1000), nullable=False)
    token_data = Column(Text, nullable=False)
    is_app = Column(Boolean, server_default="0")
