import re
# from app.core.dependencies import s3_client
# from io import BytesIO
# import pandas as pd

# def read_s3_file(bucket: str, file_path: str):
#     response = s3_client.get_object(Bucket=bucket, Key=file_path)
#     parquet_content = response['Body'].read()
#     df = pd.read_parquet(BytesIO(parquet_content))
#     return df


def check_ticker_contry_len_2(ticker: str):
    # 한국 주식 패턴 체크 (A + 6자리 숫자)
    if re.match(r"^A\d{6}$", ticker):
        return "kr"

    # 홍콩 주식 패턴 체크 (HK + 숫자)
    if re.match(r"^HK\d+$", ticker):
        return "hk"

    # 일본 주식 패턴 체크 (J + 숫자)
    if re.match(r"^J\d+$", ticker):
        return "jp"

    # 미국 주식은 위의 패턴에 해당하지 않는 모든 경우
    return "us"


def check_ticker_contry_len_3(ticker: str):
    # 한국 주식 패턴 체크 (A + 6자리 숫자)
    if re.match(r"^A\d{6}$", ticker):
        return "kor"

    # 홍콩 주식 패턴 체크 (HK + 숫자)
    if re.match(r"^HK\d+$", ticker):
        return "hkg"

    # 일본 주식 패턴 체크 (J + 숫자)
    if re.match(r"^J\d+$", ticker):
        return "jpn"

    # 미국 주식은 위의 패턴에 해당하지 않는 모든 경우
    return "usa"


contry_mapping = {
    "KOR": "kr",
    "USA": "us",
    "JPN": "jp",
    "HKG": "hk",
}
