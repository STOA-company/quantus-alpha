from sqlalchemy import Boolean, Column, Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Index

from app.enum.type import StockType
from app.models.models_base import BaseMixin, ServiceBase
from app.models.models_factors import CategoryEnum, FactorTypeEnum


class ScreenerGroup(ServiceBase, BaseMixin):
    __tablename__ = "screener_groups"

    id = Column(Integer, primary_key=True, index=True)
    order = Column(Integer, nullable=False)
    user_id = Column(
        ForeignKey("alphafinder_user.id", ondelete="CASCADE"), nullable=True, index=True
    )  # 추천 필터의 경우 None
    name = Column(String(100), nullable=False)
    type = Column(Enum(StockType), nullable=False, default=StockType.STOCK)

    stock_filters = relationship("ScreenerStockFilter", back_populates="group", cascade="all, delete-orphan")
    factor_filters = relationship("ScreenerFactorFilter", back_populates="group", cascade="all, delete-orphan")
    sort_info = relationship("ScreenerSortInfo", back_populates="group", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint("user_id", "name", "type", name="uix_user_group_name_type"),)

    def __repr__(self):
        return f"<ScreenerGroup {self.name}>"


class ScreenerStockFilter(ServiceBase, BaseMixin):
    __tablename__ = "screener_stock_filters"

    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(Integer, ForeignKey("screener_groups.id", ondelete="CASCADE"), nullable=False)
    factor = Column(String(50), nullable=False)
    type = Column(Enum(FactorTypeEnum), nullable=False, default=FactorTypeEnum.SLIDER)
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
    category = Column(Enum(CategoryEnum), default=CategoryEnum.CUSTOM, nullable=False)

    group = relationship("ScreenerGroup", back_populates="factor_filters")


class ScreenerSortInfo(ServiceBase, BaseMixin):
    __tablename__ = "screener_sort_infos"

    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(Integer, ForeignKey("screener_groups.id", ondelete="CASCADE"), nullable=False)
    category = Column(Enum(CategoryEnum), nullable=False)
    type = Column(Enum(StockType), nullable=False, default=StockType.STOCK)
    sort_by = Column(String(50), nullable=False, default="score")
    ascending = Column(Boolean, nullable=True, default=False)

    group = relationship("ScreenerGroup", back_populates="sort_info")
