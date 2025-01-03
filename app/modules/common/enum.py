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


class TranslateCountry(Enum):
    KO = "ko"
    EN = "en"


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


class FearAndGreedIndex(Enum):
    EXTREME_FEAR = 0  # 0-20 // 매우 공포
    FEAR = 25  # 21-40 // 공포
    NEUTRAL = 50  # 41-60 // 중립
    GREED = 75  # 61-80 // 탐욕
    EXTREME_GREED = 100  # 81-100 // 매우 탐욕


class PrdyVrssSign(IntEnum):
    UP_LIMIT = 1  # 상한
    UP = 2  # 상승
    FLAT = 3  # 보합
    DOWN = 4  # 하락
    DOWN_LIMIT = 5  # 하한


class TrendingType(Enum):
    UP = "up"  # 상승
    DOWN = "down"  # 하락
    VOL = "vol"  # 거래량
    AMT = "amt"  # 거래대금


class TrendingPeriod(Enum):
    REALTIME = "rt"  # 실시간
    DAY = "1d"  # 일
    WEEK = "1w"  # 주
    MONTH = "1m"  # 월
    SIX_MONTH = "6m"  # 6개월
    YEAR = "1y"  # 년
