import os
from typing import Literal

from app.common.constants import ETF_DATA_DIR, PARQUET_DIR
from app.modules.screener.etf.utils import (
    ETFDataDownloader,
    ETFDataLoader,
    ETFDataMerger,
    ETFDividendFactorExtractor,
    ETFFactorExtractor,
)
from app.utils.date_utils import now_kr, now_us
from Aws.logic.s3 import upload_file_to_bucket

# 데이터 타입에 따른 다운로드 함수 매핑
DATA_MAPPING = {
    "배당_한국": {
        "class": "ETFDataDownloader",
        "method": "download_etf_dividend",
        "params": {"ctry": "KR", "download": True},
    },
    "배당_미국": {
        "class": "ETFDataDownloader",
        "method": "download_etf_dividend",
        "params": {"ctry": "US", "download": True},
    },
    "가격_한국": {
        "class": "ETFDataDownloader",
        "method": "dwonload_etf_price",
        "params": {"ctry": "KR", "download": True},
    },
    "가격_미국": {
        "class": "ETFDataDownloader",
        "method": "dwonload_etf_price",
        "params": {"ctry": "US", "download": True},
    },
    "가격_팩터_추출_한국": {"class": "ETFFactorExtractor", "method": "calculate_all_factors", "params": {"ctry": "KR"}},
    "가격_팩터_추출_미국": {"class": "ETFFactorExtractor", "method": "calculate_all_factors", "params": {"ctry": "US"}},
    "배당_팩터_추출_한국": {
        "class": "ETFDividendFactorExtractor",
        "method": "extract_dividend_factors",
        "params": {"ctry": "KR"},
    },
    "배당_팩터_추출_미국": {
        "class": "ETFDividendFactorExtractor",
        "method": "extract_dividend_factors",
        "params": {"ctry": "US"},
    },
    "데이터_합치_한국": {
        "class": "ETFDataMerger",
        "method": "merge_data",
        "params": {"ctry": "KR", "factor": True, "dividend_factor": True, "info": True, "krx": True},
    },
    "데이터_합치_미국": {
        "class": "ETFDataMerger",
        "method": "merge_data",
        "params": {"ctry": "US", "factor": True, "dividend_factor": True, "info": True, "morningstar": True},
    },
}


def get_available_data_types():
    """현재 사용 가능한 모든 데이터 타입을 반환합니다."""
    return list(DATA_MAPPING.keys())


def execute_selected_method(data_types, class_instances):
    """
    선택한 데이터 타입들을 다운로드합니다.

    Args:
        data_types (list): 다운로드할 데이터 타입의 리스트 (예: ["배당_한국", "배당_미국"])
        class_instances (dict): 클래스 이름을 키로, 클래스 인스턴스를 값으로 하는 딕셔너리
            예: {"ETFDataDownloader": downloader_instance}

    Returns:
        dict: 데이터 타입을 키로, 데이터프레임을 값으로 하는 딕셔너리
    """
    result = {}

    # 유효한 데이터 타입인지 먼저 확인
    invalid_types = [dt for dt in data_types if dt not in DATA_MAPPING]
    if invalid_types:
        supported_types = ", ".join(DATA_MAPPING.keys())
        raise ValueError(
            f"지원하지 않는 데이터 타입이 있습니다: {', '.join(invalid_types)}. 지원 타입: {supported_types}"
        )

    # 선택된 데이터 타입만 다운로드
    for data_type in data_types:
        print(f"{data_type} 다운로드 중...")

        # 맵핑에서 클래스, 메서드 이름과 인자 가져오기
        class_name = DATA_MAPPING[data_type]["class"]
        method_name = DATA_MAPPING[data_type]["method"]
        kwargs = DATA_MAPPING[data_type]["params"]

        # 클래스 인스턴스 가져오기
        if class_name not in class_instances:
            raise ValueError(f"'{class_name}' 클래스 인스턴스가 제공되지 않았습니다.")

        instance = class_instances[class_name]

        # 메서드 참조 가져오기
        method = getattr(instance, method_name)

        # 메서드 호출하고 결과 저장
        result[data_type] = method(**kwargs)
        print(f"{data_type} 다운로드 완료")

    return result


def execute_all_methods(class_instances):
    """
    모든 데이터 타입을 다운로드합니다.

    Args:
        class_instances (dict): 클래스 이름을 키로, 클래스 인스턴스를 값으로 하는 딕셔너리

    Returns:
        dict: 데이터 타입을 키로, 데이터프레임을 값으로 하는 딕셔너리
    """
    return execute_selected_method(get_available_data_types(), class_instances)


def run_etf_screener_data(ctry: Literal["KR", "US", "ALL"] = "ALL"):
    """
    메인 함수
    1. 가격, 배당 데이터 다운로드
    2. 가격, 배당 데이터로부터 팩터 데이터 추출
    3. 팩터 데이터와 다른 데이터를 합쳐 하나의 팩터 데이터로 추출
    """
    print(f"{ctry} ETF 데이터 처리 스크립트를 시작합니다.")
    from app.core.extra.SlackNotifier import SlackNotifier

    slack_notifier = SlackNotifier(
        webhook_url="https://hooks.slack.com/services/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"
    )

    # 클래스 인스턴스 생성
    downloader = ETFDataDownloader()
    factor_extractor = ETFFactorExtractor()
    loader = ETFDataLoader()
    merger = ETFDataMerger()
    dividend_factor_extractor = ETFDividendFactorExtractor()

    # 클래스 인스턴스 매핑
    class_instances = {
        "ETFDataDownloader": downloader,
        "ETFFactorExtractor": factor_extractor,
        "ETFDataLoader": loader,
        "ETFDataMerger": merger,
        "ETFDividendFactorExtractor": dividend_factor_extractor,
    }

    if ctry == "KR":
        date = now_kr(is_date=True)
    elif ctry == "US":
        date = now_us(is_date=True)

    # 1. 데이터 다운로드
    print(f"\n1. {ctry} 데이터 다운로드 시작")
    slack_notifier.notify_info(f"1. {ctry} 데이터 다운로드 시작")

    if ctry == "ALL":
        execute_selected_method(["가격_한국", "가격_미국", "배당_한국", "배당_미국"], class_instances)
    elif ctry == "KR":
        execute_selected_method(["가격_한국", "배당_한국"], class_instances)
    elif ctry == "US":
        execute_selected_method(["가격_미국", "배당_미국"], class_instances)

    print("데이터 다운로드 완료")
    slack_notifier.notify_info("데이터 다운로드 완료")
    # 2. 팩터 데이터 추출
    print(f"\n2. {ctry} 팩터 데이터 추출 시작")

    # 한국 ETF 팩터 추출
    print(f"{ctry} ETF 팩터 추출 중...")
    slack_notifier.notify_info(f"{ctry} ETF 팩터 추출 중...")
    if ctry == "ALL":
        factors = execute_selected_method(
            ["가격_팩터_추출_한국", "배당_팩터_추출_한국", "가격_팩터_추출_미국", "배당_팩터_추출_미국"], class_instances
        )
    elif ctry == "KR":
        factors = execute_selected_method(["가격_팩터_추출_한국", "배당_팩터_추출_한국"], class_instances)
    elif ctry == "US":
        factors = execute_selected_method(["가격_팩터_추출_미국", "배당_팩터_추출_미국"], class_instances)

    # 한국 ETF 팩터 저장
    for key, df in factors.items():
        if key == "가격_팩터_추출_한국":
            df.to_parquet(os.path.join(ETF_DATA_DIR, "kr_etf_factor.parquet"), index=False)
        elif key == "배당_팩터_추출_한국":
            df.to_parquet(os.path.join(ETF_DATA_DIR, "kr_etf_dividend_factor.parquet"), index=False)
        elif key == "가격_팩터_추출_미국":
            df.to_parquet(os.path.join(ETF_DATA_DIR, "us_etf_factor.parquet"), index=False)
        elif key == "배당_팩터_추출_미국":
            df.to_parquet(os.path.join(ETF_DATA_DIR, "us_etf_dividend_factor.parquet"), index=False)
    print(f"{ctry} ETF 팩터 추출 완료")
    slack_notifier.notify_info(f"{ctry} ETF 팩터 추출 완료")

    # 3. 데이터 합치기
    print(f"\n3. {ctry} 데이터 합치기 시작")
    slack_notifier.notify_info(f"{ctry} 데이터 합치기 시작")
    kr_merged = None
    us_merged = None

    if ctry == "ALL":
        # 한국 ETF 데이터 합치기
        print("한국 ETF 데이터 합치는 중...")
        kr_merged = execute_selected_method(["데이터_합치_한국"], class_instances)
        kr_merged["데이터_합치_한국"].to_parquet(os.path.join(PARQUET_DIR, "kr_etf_factors.parquet"), index=False)
        print("한국 ETF 데이터 합치기 완료")

        # 미국 ETF 데이터 합치기
        print("미국 ETF 데이터 합치는 중...")
        us_merged = execute_selected_method(["데이터_합치_미국"], class_instances)
        us_merged["데이터_합치_미국"].to_parquet(os.path.join(PARQUET_DIR, "us_etf_factors.parquet"), index=False)
        print("미국 ETF 데이터 합치기 완료")
    elif ctry == "KR":
        # 한국 ETF 데이터 합치기
        print("한국 ETF 데이터 합치는 중...")
        kr_merged = execute_selected_method(["데이터_합치_한국"], class_instances)
        kr_merged["데이터_합치_한국"].to_parquet(os.path.join(PARQUET_DIR, "kr_etf_factors.parquet"), index=False)
        print("한국 ETF 데이터 합치기 완료")
    elif ctry == "US":
        # 미국 ETF 데이터 합치기
        print("미국 ETF 데이터 합치는 중...")
        us_merged = execute_selected_method(["데이터_합치_미국"], class_instances)
        us_merged["데이터_합치_미국"].to_parquet(os.path.join(PARQUET_DIR, "us_etf_factors.parquet"), index=False)
        print("미국 ETF 데이터 합치기 완료")
        slack_notifier.notify_info("미국 ETF 데이터 합치기 완료")

    str_date = date.strftime("%Y-%m-%d")
    if kr_merged is not None:
        message = f'\n{str_date} 한국 ETF 팩터 추출 완료 \n*SUCCESS:{kr_merged["데이터_합치_한국"].shape[0]}ETF \n kr_merged: {kr_merged["데이터_합치_한국"]}\n\n```'
        slack_notifier.notify_info(message)
    if us_merged is not None:
        message = f'\n{str_date} 미국 ETF 팩터 추출 완료 \n*SUCCESS:{us_merged["데이터_합치_미국"].shape[0]}ETF \n us_merged: {us_merged["데이터_합치_미국"]}\n\n```'
        slack_notifier.notify_info(message)

    # 데이터 s3에 업로드
    try:
        print("데이터 s3에 업로드 중...")
        slack_notifier.notify_info("데이터 s3에 업로드 중...")
        date_YYYYMMDD = date.strftime("%Y%m%d")

        if ctry == "KR":
            file_path = "parquet/kr_etf_factors.parquet"
            obj_path = f"etf/kr/kr_etf_factors_{date_YYYYMMDD}.parquet"
        elif ctry == "US":
            file_path = "parquet/us_etf_factors.parquet"
            obj_path = f"etf/us/us_etf_factors_{date_YYYYMMDD}.parquet"

        upload_file_to_bucket(file_path, "alpha-finder-factors", obj_path)
        print("데이터 s3에 업로드 완료")
        slack_notifier.notify_info("데이터 s3에 업로드 완료")
    except Exception as e:
        slack_notifier.notify_error(error=e, user_name="고경민")

    print("\n모든 작업이 완료되었습니다.")


if __name__ == "__main__":
    run_etf_screener_data("US")
    run_etf_screener_data("KR")
