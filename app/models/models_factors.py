from app.models.models_base import Base
from sqlalchemy.schema import Index
from enum import Enum
from sqlalchemy import Column, String, Text, Integer, Enum as SQLAlchemyEnum, Boolean


class CategoryEnum(str, Enum):
    TECHNICAL = "technical"
    FUNDAMENTAL = "fundamental"
    VALUATION = "valuation"


class SortDirectionEnum(str, Enum):
    ASC = "asc"
    DESC = "desc"


class UnitEnum(str, Enum):
    PRICE = "price"  # 가격
    PERCENTAGE = "percentage"  # %
    RATIO = "ratio"  # 비율
    SCORE = "score"  # 점
    TIMES = "times"  # 회
    MULTIPLE = "multiple"  # 배


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
    min_value = Column(Integer, nullable=True)
    max_value = Column(Integer, nullable=True)
    is_stock = Column(Boolean, nullable=False, default=True)
    is_etf = Column(Boolean, nullable=False, default=False)

    def __repr__(self):
        return f"<Factor {self.factor}>"
