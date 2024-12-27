from sqlalchemy import BigInteger, Boolean, DateTime, Float, String, Text
from app.models.models_base import BaseMixin
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import ForeignKey
from app.models.models_base import Base


class AlphafinderLicense(BaseMixin, Base):
    __tablename__ = "alphafinder_license"
    __table_args__ = ({"extend_existing": True},)

    serial_number: Mapped[String] = mapped_column(String(length=100), primary_key=True, nullable=False)
    is_used: Mapped[Boolean] = mapped_column(Boolean, default=False)
    plan: Mapped[Text] = mapped_column(Text)
    expired_at: Mapped[DateTime] = mapped_column(DateTime)
    user_id: Mapped[BigInteger] = mapped_column(BigInteger)
    kinds: Mapped[String] = mapped_column(String(length=100))
    used_at: Mapped[DateTime] = mapped_column(DateTime)
    country_type: Mapped[String] = mapped_column(String(length=100))
    is_expired: Mapped[Boolean] = mapped_column(Boolean, default=False)
    is_reserved: Mapped[Boolean] = mapped_column(Boolean, default=False)
    reserve_date: Mapped[DateTime] = mapped_column(DateTime)
    start_date: Mapped[DateTime] = mapped_column(DateTime)
    recommender_id: Mapped[String] = mapped_column(String(length=100))
    serial_number_related: Mapped[String] = mapped_column(String(length=100))
    purchase_type: Mapped[String] = mapped_column(String(length=50))

    def __repr__(self) -> str:
        return f"License(serial_number={self.serial_number!r})"

    def __str__(self) -> str:
        return f"License(serial_number={self.serial_number!r})"


class AlphafinderMembership(BaseMixin, Base):
    __tablename__ = "alphafinder_membership"
    __table_args__ = {"extend_existing": True}

    user_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("alphafinder_user.id", ondelete="CASCADE"), primary_key=True, nullable=False
    )
    license_serial_number: Mapped[String] = mapped_column(String(length=100))
    used_at: Mapped[DateTime] = mapped_column(DateTime)
    expired_at: Mapped[DateTime] = mapped_column(DateTime)

    def __repr__(self) -> str:
        return f"Membership(user_id={self.user_id!r}, license_serial_number={self.license_serial_number!r})"

    def __str__(self) -> str:
        return f"Membership(user_id={self.user_id!r}, license_serial_number={self.license_serial_number!r})"


class AlphafinderPaymentHistory(BaseMixin, Base):
    __tablename__ = "alphafinder_payment_history"
    __table_args__ = {"extend_existing": True}

    payment_key: Mapped[String] = mapped_column(String(length=100), nullable=False)
    order_id: Mapped[String] = mapped_column(String(length=100), nullable=False, primary_key=True)
    plan: Mapped[String] = mapped_column(String(length=100))
    requested_at: Mapped[DateTime] = mapped_column(DateTime)
    approved_at: Mapped[DateTime] = mapped_column(DateTime)
    paid_amount: Mapped[Float] = mapped_column(Float)
    payment_method: Mapped[String] = mapped_column(String(length=100))
    is_refunded: Mapped[Boolean] = mapped_column(Boolean, default=False)
    recommender_id: Mapped[String] = mapped_column(String(length=100))
    is_rewarded: Mapped[Boolean] = mapped_column(Boolean, default=False)
    reward_date: Mapped[DateTime] = mapped_column(DateTime)

    def __repr__(self) -> str:
        return f"PaymentHistory(payment_key={self.payment_key!r}, order_id={self.order_id!r})"

    def __str__(self) -> str:
        return f"PaymentHistory(payment_key={self.payment_key!r}, order_id={self.order_id!r})"
