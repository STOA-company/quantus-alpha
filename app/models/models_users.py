from sqlalchemy import ForeignKey, String, BigInteger, UniqueConstraint
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import Mapped, mapped_column
from app.models.models_base import Base, BaseMixin


class AlphafinderUser(BaseMixin, Base):
    __tablename__ = "alphafinder_user"
    __table_args__ = (UniqueConstraint("nickname"), {"extend_existing": True})

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    provider: Mapped[String] = mapped_column(String(length=100), nullable=False)
    email: Mapped[String] = mapped_column(String(length=100), nullable=False)
    nickname: Mapped[String] = mapped_column(String(length=100), nullable=False)
    profile_image: Mapped[String] = mapped_column(LONGTEXT, nullable=True)
    image_format: Mapped[String] = mapped_column(String(length=20), nullable=True)

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, nickname={self.nickname!r}, email={self.email!r})"

    def __str__(self) -> str:
        return f"User(id={self.id!r}, nickname={self.nickname!r}, email={self.email!r})"


class UserStockInterest(BaseMixin, Base):
    __tablename__ = "user_stock_interest"
    __table_args__ = {"extend_existing": True}

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("alphafinder_user.id", ondelete="CASCADE"), nullable=True, index=True
    )
    ticker: Mapped[String] = mapped_column(String(length=20), nullable=False)


class AlphaFinderOAuthToken(BaseMixin, Base):
    __tablename__ = "alphafinder_oauth_token"
    __table_args__ = {"extend_existing": True}

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    access_token_hash: Mapped[String] = mapped_column(String(length=64), index=True, nullable=False)
    refresh_token: Mapped[String] = mapped_column(String(length=1000), nullable=False)
    access_token: Mapped[String] = mapped_column(String(length=1000), nullable=False)
