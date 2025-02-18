from app.models.models_base import Base
from sqlalchemy import Column, Integer, String, Numeric, DateTime, ForeignKey, UniqueConstraint, CheckConstraint
from sqlalchemy.schema import Index
from sqlalchemy.orm import relationship
from datetime import datetime


class ScreenerFilter(Base):
    __tablename__ = "screener_filters"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String(50), nullable=True, index=True)  # 추천 필터의 경우 None
    name = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    conditions = relationship("ScreenerFilterCondition", back_populates="filter", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint("user_id", "name", name="uix_user_filter_name"),)

    def __repr__(self):
        return f"<ScreenerFilter {self.name}>"


class ScreenerFilterCondition(Base):
    __tablename__ = "screener_filter_conditions"

    id = Column(Integer, primary_key=True, index=True)
    filter_id = Column(Integer, ForeignKey("screener_filters.id", ondelete="CASCADE"), nullable=False)
    factor = Column(String(50), nullable=False)
    above = Column(Numeric)
    below = Column(Numeric)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    filter = relationship("ScreenerFilter", back_populates="conditions")
    factor_info = relationship("Factors")

    __table_args__ = (
        CheckConstraint("above IS NOT NULL OR below IS NOT NULL", name="check_at_least_one_condition"),
        Index("idx_filter_factor", "filter_id", "factor"),
    )

    def __repr__(self):
        return f"<ScreenerFilterCondition {self.factor}>"
