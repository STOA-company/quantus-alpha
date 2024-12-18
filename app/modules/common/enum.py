from enum import Enum, IntEnum


class Country(Enum):
    KR = "kr"
    US = "us"
    JP = "jp"
    HK = "hk"


class FinancialCountry(Enum):
    KOR = "KOR"
    USA = "USA"
    JPN = "JPN"
    HKG = "HKG"


class Frequency(Enum):
    DAILY = "daily"
    MINUTE = "minute"


class GraphPeriod(Enum):
    ONE_DAY = "oneday"
    ONE_WEEK = "oneweek"
    ONE_MONTH = "onemonth"
    THREE_MONTH = "threemonth"
    ONE_YEAR = "oneyear"
    ALL = "all"


class Market(Enum):
    STOCK = "stock"
    CRYPTO = "crypto"
    FOREX = "forex"


class AssetClass(Enum):
    EQUITY = "equity"
    BOND = "bond"
    COMMODITY = "commodity"


class CacheType(Enum):
    PERMANENT = "permanent"  # 과거 데이터
    TEMPORARY = "temporary"  # 최근 데이터
    NO_CACHE = "no_cache"  # 실시간 데이터


class FinanceStatus(IntEnum):
    GOOD = 1
    NEUTRAL = 2
    BAD = 3
