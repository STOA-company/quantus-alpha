from enum import Enum


class Country(Enum):
    KR = "kr"
    US = "us"
    JPN = "jpn"
    HKG = "hkg"


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
