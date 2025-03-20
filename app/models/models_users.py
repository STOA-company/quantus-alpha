from sqlalchemy import ForeignKey, String, BigInteger, Boolean, Date
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.models_base import ServiceBase, BaseMixin


class AlphafinderUser(BaseMixin, ServiceBase):
    __tablename__ = "alphafinder_user"

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    provider: Mapped[String] = mapped_column(String(length=100), nullable=False)
    email: Mapped[String] = mapped_column(String(length=100), nullable=False, unique=True)
    nickname: Mapped[String] = mapped_column(String(length=100), nullable=False, unique=True)
    profile_image: Mapped[String] = mapped_column(LONGTEXT, nullable=True)
    image_format: Mapped[String] = mapped_column(String(length=20), nullable=True)
    is_subscribed: Mapped[Boolean] = mapped_column(Boolean, nullable=False, default=False)
    subscription_end: Mapped[Date] = mapped_column(Date, nullable=True)

    groups = relationship("ScreenerGroup", back_populates="user", cascade="all, delete-orphan")
    toss_payment_history = relationship("TossPaymentHistory", back_populates="user")

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, nickname={self.nickname!r}, email={self.email!r})"

    def __str__(self) -> str:
        return f"User(id={self.id!r}, nickname={self.nickname!r}, email={self.email!r})"


class UserStockInterest(BaseMixin, ServiceBase):
    __tablename__ = "user_stock_interest"
    __table_args__ = {"extend_existing": True}

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("alphafinder_user.id", ondelete="CASCADE"), nullable=True, index=True
    )
    ticker: Mapped[String] = mapped_column(String(length=20), nullable=False)


class AlphaFinderOAuthToken(BaseMixin, ServiceBase):
    __tablename__ = "alphafinder_oauth_token"
    __table_args__ = {"extend_existing": True}

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    access_token_hash: Mapped[String] = mapped_column(String(length=64), index=True, nullable=False)
    refresh_token: Mapped[String] = mapped_column(String(length=1000), nullable=False)
    access_token: Mapped[String] = mapped_column(String(length=1000), nullable=False)


class TossPaymentHistory(BaseMixin, ServiceBase):
    __tablename__ = "toss_payment_history"
    __table_args__ = {"extend_existing": True}

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[BigInteger] = mapped_column(BigInteger, ForeignKey("alphafinder_user.id"), nullable=False)
    email: Mapped[String] = mapped_column(String(length=100), nullable=False)
    payment_key: Mapped[String] = mapped_column(String(length=100), nullable=False)
    order_id: Mapped[String] = mapped_column(String(length=100), nullable=False)
    amount: Mapped[BigInteger] = mapped_column(BigInteger, nullable=False)
    payment_method: Mapped[String] = mapped_column(String(length=100), nullable=True)

    user = relationship("AlphafinderUser", back_populates="toss_payment_history")
