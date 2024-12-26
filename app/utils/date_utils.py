from datetime import datetime
from typing import Literal
from exchange_calendars import ecals
from app.core.config import korea_tz, utc_tz


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
