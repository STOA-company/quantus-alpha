import re
from datetime import datetime

import pytz

# from app.core.dependencies import s3_client
# from io import BytesIO
# import pandas as pd

# def read_s3_file(bucket: str, file_path: str):
#     response = s3_client.get_object(Bucket=bucket, Key=file_path)
#     parquet_content = response['Body'].read()
#     df = pd.read_parquet(BytesIO(parquet_content))
#     return df


def check_ticker_country_len_2(ticker: str):
    # 한국 주식 패턴 체크 (A + 6자리 숫자)
    try:
        if re.match(r"^A\d{6}$", ticker):
            return "kr"
        raise ValueError("한국 주식 티커 형식이 아닙니다")
    except ValueError:
        # 다음 패턴 확인으로 진행
        pass

    # 홍콩 주식 패턴 체크 (HK + 숫자)
    try:
        if re.match(r"^HK\d+$", ticker):
            return "hk"
        raise ValueError("홍콩 주식 티커 형식이 아닙니다")
    except ValueError:
        # 다음 패턴 확인으로 진행
        pass

    # 일본 주식 패턴 체크 (J + 숫자)
    try:
        if re.match(r"^J\d+$", ticker):
            return "jp"
        raise ValueError("일본 주식 티커 형식이 아닙니다")
    except ValueError:
        # 다음 패턴 확인으로 진행
        pass

    # 미국 주식은 위의 패턴에 해당하지 않는 모든 경우
    return "us"


def check_ticker_country_len_3(ticker: str):
    # 한국 주식 패턴 체크 (A + 6자리 숫자)
    try:
        if re.match(r"^A\d{6}$", ticker):
            return "kor"
        raise ValueError("한국 주식 티커 형식이 아닙니다")
    except ValueError:
        # 다음 패턴 확인으로 진행
        pass

    # 홍콩 주식 패턴 체크 (HK + 숫자)
    try:
        if re.match(r"^HK\d+$", ticker):
            return "hkg"
        raise ValueError("홍콩 주식 티커 형식이 아닙니다")
    except ValueError:
        # 다음 패턴 확인으로 진행
        pass

    # 일본 주식 패턴 체크 (J + 숫자)
    try:
        if re.match(r"^J\d+$", ticker):
            return "jpn"
        raise ValueError("일본 주식 티커 형식이 아닙니다")
    except ValueError:
        # 다음 패턴 확인으로 진행
        pass

    # 미국 주식은 위의 패턴에 해당하지 않는 모든 경우
    return "usa"


async def async_check_ticker_country_len_3(ticker: str):
    # 한국 주식 패턴 체크 (A + 6자리 숫자)
    try:
        if re.match(r"^A\d{6}$", ticker):
            return "kor"
        raise ValueError("한국 주식 티커 형식이 아닙니다")
    except ValueError:
        # 다음 패턴 확인으로 진행
        pass

    # 홍콩 주식 패턴 체크 (HK + 숫자)
    try:
        if re.match(r"^HK\d+$", ticker):
            return "hkg"
        raise ValueError("홍콩 주식 티커 형식이 아닙니다")
    except ValueError:
        # 다음 패턴 확인으로 진행
        pass

    # 일본 주식 패턴 체크 (J + 숫자)
    try:
        if re.match(r"^J\d+$", ticker):
            return "jpn"
        raise ValueError("일본 주식 티커 형식이 아닙니다")
    except ValueError:
        # 다음 패턴 확인으로 진행
        pass

    # 미국 주식은 위의 패턴에 해당하지 않는 모든 경우
    return "usa"


contry_mapping = {
    "KOR": "kr",
    "USA": "us",
    "JPN": "jp",
    "HKG": "hk",
    "us": "USA",
    "kr": "KOR",
    "jp": "JPN",
    "hk": "HKG",
}
KST_TIMEZONE = pytz.timezone("Asia/Seoul")


def get_current_market_country() -> str:
    """
    현재 시간 기준으로 활성화된 시장 국가 반환
    한국 시간 07:00-19:00 -> 한국 시장
    한국 시간 19:00-07:00 -> 미국 시장
    """
    current_time = datetime.now(KST_TIMEZONE)
    current_hour = current_time.hour

    if 7 <= current_hour < 19:
        return "kr"
    else:
        return "us"
