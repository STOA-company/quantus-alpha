from datetime import datetime
from typing import Literal
import exchange_calendars as ecals
from app.core.config import korea_tz, utc_tz, us_eastern_tz
from app.core.logger import setup_logger

logger = setup_logger(__name__)


def now_kr(is_date: bool = False):
    now = datetime.now(korea_tz)
    if is_date:
        return datetime(now.year, now.month, now.day)
    else:
        return datetime(now.year, now.month, now.day, hour=now.hour, minute=now.minute, second=now.second)


def now_utc(is_date: bool = False):
    now = datetime.now(utc_tz)
    if is_date:
        return datetime(now.year, now.month, now.day)
    else:
        return datetime(now.year, now.month, now.day, hour=now.hour, minute=now.minute, second=now.second)


def now_us(is_date: bool = False):
    now = datetime.now(us_eastern_tz)
    if is_date:
        return datetime(now.year, now.month, now.day)
    else:
        return datetime(now.year, now.month, now.day, hour=now.hour, minute=now.minute, second=now.second)


def get_session_checker(country: Literal["KR", "US"], start_date: datetime | str):
    if country == "KR":
        calender = "XKRX"
    elif country == "US":
        calender = "XNYS"

    if isinstance(start_date, datetime):
        start_date = start_date.strftime("%Y-%m-%d")
    return ecals.get_calendar(calender, start=start_date)


def get_business_days(
    country: Literal["KR", "US", "JP", "HK"], start_date: datetime, end_date: datetime
) -> list[datetime]:
    """
    주어진 국가와 기간에 대한 영업일 목록을 반환합니다.

    Args:
        country (Literal["KR", "US", "JP", "HK"]): 국가 코드
        start_date (datetime): 시작 날짜
        end_date (datetime): 종료 날짜

    Returns:
        list[datetime]: 영업일 목록
    """
    calendar_map = {
        "KR": "XKRX",  # 한국 거래소
        "US": "XNYS",  # 뉴욕 증권거래소
        "JP": "XTKS",  # 도쿄 증권거래소
        "HK": "XHKG",  # 홍콩 증권거래소
    }

    calendar = ecals.get_calendar(calendar_map[country])
    schedule = calendar.sessions_in_range(start_date, end_date)

    return schedule.tolist()


def get_time_checker(country: Literal["KR", "US"]) -> bool:
    """
    현재 시간이 장 운영 시간 안에 있는지 확인
    KR: 09:00 ~ 15:30 (KST)
    US: 09:30 ~ 16:00 (ET) - 서머타임 자동 반영
    """
    try:
        if country == "KR":
            calendar = ecals.get_calendar("XKRX")
            current_time = datetime.now(korea_tz)

            return calendar.is_open_on_minute(current_time)

        elif country == "US":
            calendar = ecals.get_calendar("XNYS")
            current_time = datetime.now(us_eastern_tz)

            return calendar.is_open_on_minute(current_time)

    except Exception as e:
        logger.error(f"Error in get_time_checker: {str(e)}")
        return False


def is_business_day(country: Literal["KR", "US"]) -> bool:
    calendar_map = {
        "KR": "XKRX",  # 한국 거래소
        "US": "XNYS",  # 뉴욕 증권거래소
    }
    calendar = ecals.get_calendar(calendar_map[country])
    return calendar.is_session(now_utc(is_date=True))


def check_market_status(country: Literal["KR", "US"]) -> bool:
    """
    시장 상태 확인
    1. 거래일 여부 확인
    2. 거래 시간 확인
    """
    try:
        # 휴장 여부 확인
        if not is_business_day(country):
            logger.info(f"{country} market is not a business day")
            return False

        # 개장 여부 확인
        if not get_time_checker(country):
            logger.info(f"{country} market is not open")
            return False

        return True

    except Exception as e:
        logger.error(f"Error checking market status: {str(e)}")
        return False


def is_holiday(country: Literal["KR", "US"], date_str: str) -> bool:
    """
    주어진 날짜가 휴장일인지 확인합니다.

    Args:
        country (Literal["KR", "US"]): 국가 코드
        date_str (str): "YYYYMMDD" 형식의 날짜 문자열

    Returns:
        bool: 휴장일이면 True, 거래일이면 False 반환
    """
    try:
        calendar_map = {
            "KR": "XKRX",  # 한국 거래소
            "US": "XNYS",  # 뉴욕 증권거래소
        }

        # 날짜 문자열 파싱
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])

        check_date = datetime(year, month, day)

        calendar = ecals.get_calendar(calendar_map[country])

        is_trading_day = calendar.is_session(check_date)

        return not is_trading_day

    except Exception as e:
        logger.error(f"Error in is_holiday: {str(e)}")
        return False


def is_us_market_open_or_recently_closed(extra_hours=1):
    """
    미국 시장이 열려있거나 마감 후 지정된 시간(기본 1시간) 이내인지 확인합니다.

    Args:
        extra_hours (int): 마감 후 추가 시간(시간 단위)

    Returns:
        bool: 시장이 열려있거나 마감 후 지정된 시간 이내면 True
    """
    try:
        calendar = ecals.get_calendar("XNYS")  # 뉴욕 증권거래소
        current_time = now_utc()
        current_time_aware = current_time.replace(tzinfo=utc_tz)

        # 오늘이 거래일인지 확인
        today_date = current_time.date()
        if not calendar.is_session(today_date):
            return False

        # 현재 시장이 열려있는지 확인
        if calendar.is_open_on_minute(current_time_aware):
            return True

        # 시장 마감 시간 확인
        today_schedule = calendar.schedule.loc[today_date.strftime("%Y-%m-%d")]
        close_time = today_schedule["close"].to_pydatetime()  # Pandas Timestamp를 datetime 객체로 변환

        # close_time을 UTC로 변환
        close_time_utc = close_time.astimezone(utc_tz)

        # 현재 시간과 마감 시간의 차이를 계산 (모두 UTC 기준)
        time_diff = (current_time_aware - close_time_utc).total_seconds()

        # 현재 시간이 마감 시간 이후이고, 그 차이가 extra_hours 이내인지 확인
        return 0 <= time_diff <= extra_hours * 3600

    except Exception as e:
        logger.error(f"Error in is_us_market_open_or_recently_closed: {str(e)}")
        return False
