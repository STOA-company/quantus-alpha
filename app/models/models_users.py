from sqlalchemy import BigInteger, Boolean, Date, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.models_base import BaseMixin, ServiceBase


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
    subscription_start: Mapped[Date] = mapped_column(Date, nullable=True)
    recent_payment_date: Mapped[Date] = mapped_column(Date, nullable=True)  # TODO:: 삭제 예정
    subscription_level: Mapped[int] = mapped_column(
        Integer, ForeignKey("alphafinder_level.level", name="fk_user_subscription_level"), nullable=True, default=1
    )
    subscription_name: Mapped[String] = mapped_column(String(length=100), nullable=True)
    using_history_id: Mapped[int] = mapped_column(Integer, nullable=True)

    groups = relationship("ScreenerGroup", back_populates="user", cascade="all, delete-orphan")
    toss_payment_history = relationship("TossPaymentHistory", back_populates="user")
    conversations = relationship("ChatConversation", back_populates="user")
    interest_groups = relationship("InterestGroup", back_populates="user")

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, nickname={self.nickname!r}, email={self.email!r})"

    def __str__(self) -> str:
        return f"User(id={self.id!r}, nickname={self.nickname!r}, email={self.email!r})"


class InterestGroup(BaseMixin, ServiceBase):
    __tablename__ = "interest_group"
    __table_args__ = {"extend_existing": True}

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[String] = mapped_column(String(length=100), nullable=False)
    user_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("alphafinder_user.id", ondelete="CASCADE"), nullable=False
    )

    user = relationship("AlphafinderUser", back_populates="interest_groups")
    user_stock_interests = relationship("UserStockInterest", back_populates="group")

    __table_args__ = (UniqueConstraint("user_id", "name", name="uix_user_id_name"),)


class UserStockInterest(BaseMixin, ServiceBase):
    __tablename__ = "user_stock_interest"
    __table_args__ = {"extend_existing": True}

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    group_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("interest_group.id", ondelete="CASCADE"), nullable=True, index=True
    )
    ticker: Mapped[String] = mapped_column(String(length=20), nullable=False)

    group = relationship("InterestGroup", back_populates="user_stock_interests")

    __table_args__ = (UniqueConstraint("group_id", "ticker", name="uix_group_id_ticker"),)


class AlphafinderInterestGroup(BaseMixin, ServiceBase):
    __tablename__ = "alphafinder_interest_group"
    __table_args__ = (
        Index("idx_user_id_order", "user_id", "order"),
        {"extend_existing": True},
    )

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[String] = mapped_column(String(length=100), nullable=False)
    user_id: Mapped[BigInteger] = mapped_column(BigInteger, nullable=False)
    order: Mapped[int] = mapped_column(Integer, nullable=False)
    is_editable: Mapped[Boolean] = mapped_column(Boolean, nullable=False, default=True)

    interest_stocks = relationship("AlphafinderUserStockInterest", back_populates="interest_group")

    __table_args__ = (UniqueConstraint("user_id", "name", name="uix_user_id_name"),)


class AlphafinderUserStockInterest(BaseMixin, ServiceBase):
    __tablename__ = "alphafinder_interest_stock"
    __table_args__ = (
        Index("idx_group_id_order", "group_id", "order"),
        {"extend_existing": True},
    )

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    group_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("alphafinder_interest_group.id", ondelete="CASCADE"), nullable=True, index=True
    )
    ticker: Mapped[String] = mapped_column(String(length=20), nullable=False)
    order: Mapped[int] = mapped_column(Integer, nullable=False)

    interest_group = relationship("AlphafinderInterestGroup", back_populates="interest_stocks")

    __table_args__ = (UniqueConstraint("group_id", "ticker", name="uix_group_id_ticker"),)


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


class DataDownloadHistory(ServiceBase):
    __tablename__ = "data_download_history"
    __table_args__ = (
        Index("idx_user_id", "user_id"),
        Index("idx_user_id_download_datetime", "user_id", "download_datetime"),
        {"extend_existing": True},
    )

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[BigInteger] = mapped_column(BigInteger, nullable=False)
    data_type: Mapped[String] = mapped_column(String(length=100), nullable=False)
    data_detail: Mapped[String] = mapped_column(String(length=100), nullable=True)
    download_datetime: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
