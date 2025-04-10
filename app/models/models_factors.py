from app.models.models_base import Base
from sqlalchemy.schema import Index
from enum import Enum
from sqlalchemy import Column, String, Text, Integer, Enum as SQLAlchemyEnum, Boolean, ForeignKey
from sqlalchemy.orm import relationship


class CategoryEnum(str, Enum):
    TECHNICAL = "technical"
    FUNDAMENTAL = "fundamental"
    VALUATION = "valuation"
    DIVIDEND = "dividend"
    GROWTH = "growth"
    CUSTOM = "custom"


class SortDirectionEnum(str, Enum):
    ASC = "asc"
    DESC = "desc"


class UnitEnum(str, Enum):
    BIG_PRICE = "big_price"  # 억원/천달러
    SMALL_PRICE = "small_price"  # 원/달러
    PERCENTAGE = "percentage"  # %
    RATIO = "ratio"  # 비율
    SCORE = "score"  # 점
    TIMES = "times"  # 회
    MULTIPLE = "multiple"  # 배


class FactorTypeEnum(str, Enum):
    SLIDER = "slider"
    MULTI = "multi"
    SINGLE = "single"


class Factors(Base):
    __tablename__ = "factors"

    __table_args__ = (
        Index("idx_category", "category"),
        Index("idx_sort_direction", "sort_direction"),
        Index("idx_category_sort_direction", "category", "sort_direction"),
    )

    factor = Column(String(50), primary_key=True, nullable=False, unique=True)
    description = Column(Text, nullable=False)
    unit = Column(SQLAlchemyEnum(UnitEnum), nullable=False)
    sort_direction = Column(SQLAlchemyEnum(SortDirectionEnum), nullable=False)
    category = Column(SQLAlchemyEnum(CategoryEnum), nullable=False)
    type = Column(SQLAlchemyEnum(FactorTypeEnum), nullable=False, default=FactorTypeEnum.SLIDER)
    min_value = Column(Integer, nullable=True)
    max_value = Column(Integer, nullable=True)
    is_stock = Column(Boolean, nullable=False, default=True)
    is_etf = Column(Boolean, nullable=False, default=False)
    is_active = Column(Boolean, nullable=False, default=True)
    order = Column(Integer, nullable=False)

    presets = relationship("FactorsPreset", back_populates="factors")

    def __repr__(self):
        return f"<Factor {self.factor}>"


class FactorsPreset(Base):
    __tablename__ = "factors_preset"

    id = Column(Integer, primary_key=True)
    factor = Column(String(50), ForeignKey("factors.factor", ondelete="CASCADE"), nullable=False)
    value = Column(String(50), nullable=True)
    above = Column(Integer, nullable=True)
    below = Column(Integer, nullable=True)
    display = Column(String(50), nullable=True)
    order = Column(Integer, nullable=False)

    factors = relationship("Factors", back_populates="presets")

    def __repr__(self):
        return f"<FactorPreset {self.factor}>"
