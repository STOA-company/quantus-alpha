from datetime import datetime
from typing import Literal
import exchange_calendars as ecals
from app.core.config import korea_tz, utc_tz
import logging


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


# 장 오픈 시간 조회
def get_time_checker(country: Literal["KR", "US"]) -> bool:
    """
    현재 시간이 장 운영 시간 안에 있는지 확인
    KR: 09:00 ~ 15:30 (KST)
    US: 13:30 ~ 20:00 (UTC) = 09:30 ~ 16:00 (ET)
    """
    try:
        if country == "KR":
            date = datetime.now(korea_tz)
            current_time = date.hour * 60 + date.minute
            logging.info(f"Current time: {current_time}")
            market_open = 9 * 60  # 09:00 KST
            market_close = 15 * 60 + 30  # 15:30 KST

            return market_open <= current_time <= market_close

        elif country == "US":
            date = datetime.now(utc_tz)
            current_time = date.hour * 60 + date.minute
            logging.info(f"Current time: {current_time}")
            market_open = 13 * 60 + 30  # 13:30 UTC = 09:30 ET
            market_close = 20 * 60  # 20:00 UTC = 16:00 ET

            return market_open <= current_time <= market_close

    except Exception as e:
        logging.error(f"Error in get_time_checker: {str(e)}")
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
            logging.info(f"{country} market is not a business day")
            return False

        # 개장 여부 확인
        if not get_time_checker(country):
            logging.info(f"{country} market is not open")
            return False

        return True

    except Exception as e:
        logging.error(f"Error checking market status: {str(e)}")
        return False
