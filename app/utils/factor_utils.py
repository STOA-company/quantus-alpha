import pandas as pd
from app.database.crud import database
from typing import Dict, List
from Aws.logic.s3 import get_data_from_bucket
import io


sector_map = {
    # 헬스케어
    "헬스케어": "건강관리",
    # 산업재
    "자동차": "자동차",
    "운송": "운송",
    "기계": "기계",
    "조선": "조선",
    # 소비재
    "소비재": "화장품,의류,완구",
    "필수소비재": "필수소비재",
    "유통": "소매(유통)",
    # IT/가전
    "IT하드웨어": "IT하드웨어",
    "IT가전": "IT가전",
    # 소재
    "화학": "화학",
    "금속/광업": "비철,목재등",
    "철강": "철강",
    # 건설/부동산
    "건설": "건설,건축관련",
    "부동산": "real_estate",
    "건축": "construction",
    # 금융
    "보험": "보험",
    "증권": "증권",
    "은행": "은행",
    "금융": "finance",
    # 정보기술
    "반도체": "반도체",
    "디스플레이": "디스플레이",
    "소프트웨어": "소프트웨어",
    # 에너지/유틸리티
    "에너지": "에너지",
    "유틸리티": "유틸리티",
    # 통신/미디어
    "통신": "통신서비스",
    "미디어/엔터": "미디어,교육",
    # 서비스
    "호텔/레저": "호텔,레저서비스",
    # 종합
    "종합상사": "상사,자본재",
}


def get_factors_from_db() -> Dict[str, Dict]:
    factors = database._select("factors", columns=["factor", "description", "unit", "category", "sort_direction"])

    factors_mapping = {}
    for factor in factors:
        factors_mapping[factor.factor] = {
            "description": factor.description,
            "unit": factor.unit,
            "category": factor.category,
            "sort_direction": factor.sort_direction,
        }

    return factors_mapping


def process_factor_data():
    output_file = "parquet/stock_factors.parquet"
    factors_mapping = get_factors_from_db()

    us_result = get_data_from_bucket(bucket="quantus-ticker-prices", key="factor_us_active.parquet", dir="port/")
    us_df = pd.read_parquet(io.BytesIO(us_result))

    kr_result = get_data_from_bucket(bucket="quantus-ticker-prices", key="factor_ko_active.parquet", dir="port/")
    kr_df = pd.read_parquet(io.BytesIO(kr_result))

    kr_df["country"] = "kr"
    us_df["country"] = "us"

    df = pd.concat([kr_df, us_df])

    market_mapping = {"NAS": "NASDAQ", "NYS": "NYSE", "KRX": "KOSDAQ", "KOS": "KOSPI"}

    selected_columns = ["Code", "ExchMnem", "country", "WI26업종명(대)", "Name", "거래대금", "수정주가수익률"] + list(
        factors_mapping.keys()
    )
    df_selected = df[selected_columns]
    df_filtered = df_selected[df_selected["ExchMnem"].isin(market_mapping.keys())]

    df_result = df_filtered.rename(
        columns={
            "ExchMnem": "market",
            "WI26업종명(대)": "sector",
            "Name": "name",
            "거래대금": "trade_volume",
            "수정주가수익률": "price_change_rate",
        }
    )
    df_result["market"] = df_result["market"].map(market_mapping)

    df_result.to_parquet(output_file)


def filter_stocks(market_filter: List[str], sector_filter: List[str], custom_filters: List[Dict]) -> List[str]:
    df = pd.read_parquet("parquet/stock_factors.parquet")
    filtered_df = df.copy()

    if market_filter:
        filtered_df = filtered_df[filtered_df["market"].isin(market_filter)]

    if sector_filter:
        filtered_df = filtered_df[filtered_df["sector"].isin(sector_filter)]

    for filter in custom_filters:
        factor = filter["factor"]

        if factor not in filtered_df.columns:
            raise ValueError(f"팩터 '{factor}'가 데이터에 존재하지 않습니다.")

        if filter["above"] is not None:
            filtered_df = filtered_df[filtered_df[factor] >= filter["above"]]

        if filter["below"] is not None:
            filtered_df = filtered_df[filtered_df[factor] <= filter["below"]]

    stock_codes = filtered_df["Code"].tolist()

    return stock_codes


DEFAULT_COLUMNS = ["Code", "name", "market", "sector", "close", "price_change_rate", "trade_volume"]


def get_filtered_stocks_data(codes: List[str], columns: List[str]) -> List[Dict]:
    required_columns = DEFAULT_COLUMNS + [col for col in columns if col not in DEFAULT_COLUMNS]

    df = pd.read_parquet("parquet/stock_factors.parquet")
    filtered_df = df[df["Code"].isin(codes)][required_columns]
    stocks_data = filtered_df.to_dict(orient="records")

    return stocks_data
