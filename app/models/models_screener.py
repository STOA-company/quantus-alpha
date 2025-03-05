from app.models.models_base import ServiceBase, BaseMixin
from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint, Enum
from sqlalchemy.schema import Index
from sqlalchemy.orm import relationship
from app.enum.type import StockType


class ScreenerGroup(ServiceBase, BaseMixin):
    __tablename__ = "screener_groups"

    id = Column(Integer, primary_key=True, index=True)
    order = Column(Integer, nullable=False)
    user_id = Column(String(50), nullable=True, index=True)  # 추천 필터의 경우 None
    name = Column(String(100), nullable=False)
    type = Column(Enum(StockType), nullable=False, default=StockType.STOCK)

    stock_filters = relationship("ScreenerStockFilter", back_populates="group", cascade="all, delete-orphan")
    factor_filters = relationship("ScreenerFactorFilter", back_populates="group", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint("user_id", "name", "type", name="uix_user_group_name_type"),)

    def __repr__(self):
        return f"<ScreenerGroup {self.name}>"


class ScreenerStockFilter(ServiceBase, BaseMixin):
    __tablename__ = "screener_stock_filters"

    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(Integer, ForeignKey("screener_groups.id", ondelete="CASCADE"), nullable=False)
    factor = Column(String(50), nullable=False)
    above = Column(Integer, nullable=True)
    below = Column(Integer, nullable=True)
    value = Column(String(50), nullable=True)

    __table_args__ = (Index("idx_group_factor", "group_id", "factor"),)

    def __repr__(self):
        return f"<ScreenerStockFilter {self.factor}>"


class ScreenerFactorFilter(ServiceBase, BaseMixin):
    __tablename__ = "screener_factor_filters"

    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(Integer, ForeignKey("screener_groups.id", ondelete="CASCADE"), nullable=False)
    factor = Column(String(50), nullable=False)
    order = Column(Integer, nullable=False)

    group = relationship("ScreenerGroup", back_populates="factor_filters")
