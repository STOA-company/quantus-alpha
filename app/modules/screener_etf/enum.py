from enum import Enum


class ETFMarketEnum(str, Enum):
    KR = "kr"
    US = "us"
    NYSE = "nyse"
    NASDAQ = "nasdaq"
    BATS = "bats"


class ETFCategoryEnum(str, Enum):
    TECHNICAL = "technical"
    FUNDAMENTAL = "fundamental"
    VALUATION = "valuation"
    CUSTOM = "custom"
