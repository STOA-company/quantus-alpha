from sqlalchemy import JSON, Boolean, DateTime, Float, Integer, String, ForeignKey, BigInteger
from app.models.models_base import BaseMixin, ServiceBase
from sqlalchemy.orm import Mapped, mapped_column, relationship


class AlphafinderLevel(ServiceBase):
    __tablename__ = "alphafinder_level"
    __table_args__ = {"extend_existing": True}

    level: Mapped[int] = mapped_column(Integer, nullable=False, primary_key=True)
    name: Mapped[String] = mapped_column(String(length=100), nullable=False)

    # 관계
    prices = relationship("AlphafinderPrice", back_populates="level_info")

    def __repr__(self) -> str:
        return f"AlphafinderLevel(level={self.level!r}, name={self.name!r})"

    def __str__(self) -> str:
        return f"AlphafinderLevel(level={self.level!r}, name={self.name!r})"


class AlphafinderPrice(ServiceBase, BaseMixin):
    __tablename__ = "alphafinder_price"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[String] = mapped_column(String(length=100), nullable=False)
    level: Mapped[int] = mapped_column(Integer, ForeignKey("alphafinder_level.level"), nullable=False)
    price: Mapped[Float] = mapped_column(Float, nullable=False)
    period_days: Mapped[int] = mapped_column(Integer, nullable=False)
    event_price: Mapped[Float] = mapped_column(Float, nullable=True)
    is_active: Mapped[Boolean] = mapped_column(Boolean, nullable=False, default=True)
    price_type: Mapped[String] = mapped_column(String(length=100), nullable=False)
    event_type: Mapped[String] = mapped_column(String(length=100), nullable=True)
    event_end_date: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    is_banner: Mapped[Boolean] = mapped_column(Boolean, nullable=False, default=False)
    is_event: Mapped[Boolean] = mapped_column(Boolean, nullable=False, default=False)

    # 관계
    level_info = relationship("AlphafinderLevel", back_populates="prices")

    def __repr__(self) -> str:
        return f"AlphafinderPrice(id={self.id!r}, name={self.name!r}, level={self.level!r}, price={self.price!r}, event_price={self.event_price!r})"

    def __str__(self) -> str:
        return f"AlphafinderPrice(id={self.id!r}, name={self.name!r}, level={self.level!r}, price={self.price!r}, event_price={self.event_price!r})"


class AlphafinderPaymentHistory(ServiceBase, BaseMixin):
    __tablename__ = "alphafinder_payment_history"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    receipt_id: Mapped[int] = mapped_column(Integer, nullable=False)
    user_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("alphafinder_user.id", ondelete="SET NULL"), nullable=True
    )
    level: Mapped[int] = mapped_column(Integer, ForeignKey("alphafinder_level.level"), nullable=False)
    period_days: Mapped[int] = mapped_column(Integer, nullable=False)
    paid_amount: Mapped[Float] = mapped_column(Float)
    payment_method: Mapped[String] = mapped_column(String(length=100))
    payment_company: Mapped[String] = mapped_column(String(length=100))
    refund_at: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    is_extended: Mapped[Boolean] = mapped_column(Boolean, nullable=False, default=False)

    # 관계
    user = relationship("AlphafinderUser", back_populates="payment_history")
    level_info = relationship("AlphafinderLevel")

    def __repr__(self) -> str:
        return f"PaymentHistory(id={self.id!r}, receipt_id={self.receipt_id!r}, user_id={self.user_id!r}, level={self.level!r}, period_days={self.period_days!r}, paid_amount={self.paid_amount!r}, payment_method={self.payment_method!r}, payment_company={self.payment_company!r}, refund_at={self.refund_at!r}, is_extended={self.is_extended!r})"

    def __str__(self) -> str:
        return f"PaymentHistory(id={self.id!r}, receipt_id={self.receipt_id!r}, user_id={self.user_id!r}, level={self.level!r}, period_days={self.period_days!r}, paid_amount={self.paid_amount!r}, payment_method={self.payment_method!r}, payment_company={self.payment_company!r}, refund_at={self.refund_at!r}, is_extended={self.is_extended!r})"


class TossReceipt(ServiceBase, BaseMixin):
    __tablename__ = "toss_receipt"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    payment_key: Mapped[String] = mapped_column(String(length=100), nullable=False, unique=True)
    order_id: Mapped[String] = mapped_column(String(length=100), nullable=False)
    receipt: Mapped[JSON] = mapped_column(JSON)

    def __repr__(self) -> str:
        return f"TossReceipt(id={self.id!r}, user_id={self.user_id!r}, payment_key={self.payment_key!r}, order_id={self.order_id!r})"

    def __str__(self) -> str:
        return f"TossReceipt(id={self.id!r}, user_id={self.user_id!r}, payment_key={self.payment_key!r}, order_id={self.order_id!r})"


class AlphafinderCouponBox(ServiceBase, BaseMixin):
    __tablename__ = "alphafinder_coupon_box"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("alphafinder_user.id", ondelete="SET NULL"), nullable=True
    )
    coupon_name: Mapped[String] = mapped_column(String(length=100), nullable=True)
    coupon_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("alphafinder_coupon.id", ondelete="SET NULL"), nullable=True
    )
    issued_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False, comment="지급일")
    expired_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False, comment="만료일")
    coupon_status: Mapped[String] = mapped_column(
        String(length=10), nullable=False, default="inactive", comment="쿠폰 상태"
    )

    # 관계
    user = relationship("AlphafinderUser")
    coupon = relationship("AlphafinderCoupon")

    def __repr__(self) -> str:
        return f"AlphafinderCouponBox(id={self.id!r}, user_id={self.user_id!r}, coupon_id={self.coupon_id!r}, issued_at={self.issued_at!r}, expired_at={self.expired_at!r})"

    def __str__(self) -> str:
        return f"AlphafinderCouponBox(id={self.id!r}, user_id={self.user_id!r}, coupon_id={self.coupon_id!r}, issued_at={self.issued_at!r}, expired_at={self.expired_at!r})"


class AlphafinderCoupon(ServiceBase, BaseMixin):
    __tablename__ = "alphafinder_coupon"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    coupon_num: Mapped[String] = mapped_column(String(length=100), nullable=False, unique=True)
    coupon_name: Mapped[String] = mapped_column(String(length=100), nullable=False)
    coupon_period_days: Mapped[int] = mapped_column(Integer, nullable=False)
    is_active: Mapped[Boolean] = mapped_column(Boolean, nullable=False, default=True)
    expired_at: Mapped[DateTime] = mapped_column(DateTime, nullable=True)

    # 관계
    coupon_box = relationship("AlphafinderCouponBox")

    def __repr__(self) -> str:
        return f"AlphafinderCoupon(id={self.id!r}, coupon_num={self.coupon_num!r}, coupon_name={self.coupon_name!r}, coupon_period_days={self.coupon_period_days!r})"

    def __str__(self) -> str:
        return f"AlphafinderCoupon(id={self.id!r}, coupon_num={self.coupon_num!r}, coupon_name={self.coupon_name!r}, coupon_period_days={self.coupon_period_days!r})"


class UserUsingHistory(ServiceBase, BaseMixin):
    __tablename__ = "user_using_history"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[BigInteger] = mapped_column(BigInteger, nullable=True)
    start_date: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    end_date: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    product_name: Mapped[String] = mapped_column(String(length=100), nullable=False)
    product_type: Mapped[String] = mapped_column(String(length=100), nullable=False)
    product_relation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    refund_at: Mapped[DateTime] = mapped_column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return f"UserUsingHistory(id={self.id!r}, user_id={self.user_id!r}, start_date={self.start_date!r}, end_date={self.end_date!r}, product_name={self.product_name!r}, product_type={self.product_type!r}, product_relation_id={self.product_relation_id!r})"

    def __str__(self) -> str:
        return f"UserUsingHistory(id={self.id!r}, user_id={self.user_id!r}, start_date={self.start_date!r}, end_date={self.end_date!r}, product_name={self.product_name!r}, product_type={self.product_type!r}, product_relation_id={self.product_relation_id!r})"
