from app.models.models_base import ServiceBase, BaseMixin
from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.schema import Index
from sqlalchemy.orm import relationship


class ScreenerFilterGroups(ServiceBase, BaseMixin):
    __tablename__ = "screener_filter_groups"

    id = Column(Integer, primary_key=True, index=True)
    order = Column(Integer, nullable=False)
    user_id = Column(String(50), nullable=True, index=True)  # 추천 필터의 경우 None
    name = Column(String(100), nullable=False)

    conditions = relationship("ScreenerFilterCondition", back_populates="filter", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uix_user_filter_name"),
        UniqueConstraint("user_id", "order", name="uix_user_filter_order"),
    )

    def __repr__(self):
        return f"<ScreenerFilterGroup {self.name}>"


class ScreenerFilterCondition(ServiceBase, BaseMixin):
    __tablename__ = "screener_filter_conditions"

    id = Column(Integer, primary_key=True, index=True)
    filter_group_id = Column(Integer, ForeignKey("screener_filter_groups.id", ondelete="CASCADE"), nullable=False)
    factor = Column(String(50), nullable=False)
    above = Column(Integer, nullable=True)
    below = Column(Integer, nullable=True)
    value = Column(String(50), nullable=True)

    __table_args__ = (Index("idx_filter_factor", "filter_group_id", "factor"),)

    def __repr__(self):
        return f"<ScreenerFilterCondition {self.factor}>"


class ScreenerColumnSet(ServiceBase, BaseMixin):
    __tablename__ = "screener_column_sets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)


class ScreenerColumn(ServiceBase, BaseMixin):
    __tablename__ = "screener_columns"

    id = Column(Integer, primary_key=True, index=True)
    column_set_id = Column(Integer, ForeignKey("screener_column_sets.id", ondelete="CASCADE"), nullable=False)
    factor = Column(String(50), nullable=False)

    column_set = relationship("ScreenerColumnSet", back_populates="columns")
