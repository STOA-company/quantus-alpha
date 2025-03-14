from enum import Enum


class ETFMarketEnum(str, Enum):
    KR = "kr"
    US = "us"
    NYSE = "nyse"
    NASDAQ = "nas"
    BATS = "bats"


class ETFCategoryEnum(str, Enum):
    TECHNICAL = "technical"
    DIVIDEND = "dividend"
    CUSTOM = "custom"
