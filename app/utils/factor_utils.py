import pandas as pd
from app.database.crud import database
from typing import Dict, List, Optional
from Aws.logic.s3 import get_data_from_bucket
import io
from app.modules.screener.schemas import MarketEnum

sector_map = {
    "건강관리": "건강관리",
    "자동차": "자동차",
    "화장품,의류,완구": "화장품,의류,완구",
    "화학": "화학",
    "필수소비재": "필수소비재",
    "운송": "운송",
    "상사,자본재": "상사,자본재",
    "비철,목재등": "비철,목재등",
    "건설,건축관련": "건설,건축관련",
    "보험": "보험",
    "에너지": "에너지",
    "기계": "기계",
    "철강": "철강",
    "반도체": "반도체",
    "IT하드웨어": "IT하드웨어",
    "증권": "증권",
    "디스플레이": "디스플레이",
    "IT가전": "IT가전",
    "소매(유통)": "소매(유통)",
    "유틸리티": "유틸리티",
    "은행": "은행",
    "통신서비스": "통신서비스",
    "호텔,레저서비스": "호텔,레저서비스",
    "소프트웨어": "소프트웨어",
    "조선": "조선",
    "미디어,교육": "미디어,교육",
    "real_estate": "부동산",
    "finance": "금융",
    "construction": "건설, 건축",
    "consumer_goods": "필수소비재",
    "hospitality": "호텔, 레저",
    "software": "소프트웨어",
    "professional": "전문 서비스",
    "energy": "에너지",
    "electronics": "전자",
    "aerospace": "항공",
    "industrial": "기계",
    "media": "미디어,교육",
    "other": "기타",
    None: "기타",
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


def process_kr_factor_data():
    output_file = "parquet/kr_stock_factors.parquet"
    factors_mapping = get_factors_from_db()

    result = get_data_from_bucket(bucket="quantus-ticker-prices", key="factor_ko_active.parquet", dir="port/")
    df = pd.read_parquet(io.BytesIO(result))

    df["country"] = "kr"

    market_mapping = {"KRX": "KOSDAQ", "KOS": "KOSPI"}

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
    df_result["sector"] = df_result["sector"].map(sector_map)

    df_result.to_parquet(output_file)


def process_us_factor_data():
    output_file = "parquet/us_stock_factors.parquet"
    factors_mapping = get_factors_from_db()

    result = get_data_from_bucket(bucket="quantus-ticker-prices", key="factor_us_active.parquet", dir="port/")
    df = pd.read_parquet(io.BytesIO(result))

    df["merge_code"] = df["Code"].str.replace("-US", "")

    stock_information = database._select("stock_information", columns=["ticker", "is_snp_500"], ctry="us")
    stock_info_df = pd.DataFrame(stock_information)
    stock_info_df = stock_info_df.rename(columns={"ticker": "merge_code"})
    stock_info_df["is_snp_500"] = stock_info_df["is_snp_500"].fillna(0).astype(int)

    df = pd.merge(df, stock_info_df, on="merge_code", how="left")

    df = df.drop("merge_code", axis=1)

    df["country"] = "us"
    df["is_snp_500"] = df["is_snp_500"].fillna(0).astype(int)

    market_mapping = {"NAS": "NASDAQ", "NYS": "NYSE"}

    selected_columns = [
        "Code",
        "ExchMnem",
        "country",
        "WI26업종명(대)",
        "Name",
        "거래대금",
        "수정주가수익률",
        "is_snp_500",
    ] + list(factors_mapping.keys())
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
    df_result["sector"] = df_result["sector"].map(sector_map)

    df_result.to_parquet(output_file)


def get_df_from_parquet(market_filter: MarketEnum) -> pd.DataFrame:
    df = None
    if market_filter:
        if market_filter in [MarketEnum.US, MarketEnum.NASDAQ, MarketEnum.SNP500]:
            df = pd.read_parquet("parquet/us_stock_factors.parquet")
        elif market_filter in [MarketEnum.KR, MarketEnum.KOSPI, MarketEnum.KOSDAQ]:
            df = pd.read_parquet("parquet/kr_stock_factors.parquet")
    else:
        kr_df = pd.read_parquet("parquet/kr_stock_factors.parquet")
        us_df = pd.read_parquet("parquet/us_stock_factors.parquet")
        kr_df["is_snp_500"] = 0
        df = pd.concat([kr_df, us_df])

    return df


def filter_stocks(
    market_filter: Optional[MarketEnum] = None,
    sector_filter: Optional[List[str]] = None,
    custom_filters: Optional[List[Dict]] = None,
) -> List[str]:
    df = get_df_from_parquet(market_filter)

    filtered_df = df.copy()

    if market_filter:
        if market_filter == MarketEnum.US:
            filtered_df = filtered_df[filtered_df["country"] == "us"]
        elif market_filter == MarketEnum.KR:
            filtered_df = filtered_df[filtered_df["country"] == "kr"]
        elif market_filter == MarketEnum.SNP500:
            filtered_df = filtered_df[filtered_df["is_snp_500"] == 1]
        elif market_filter in [MarketEnum.NASDAQ, MarketEnum.KOSPI, MarketEnum.KOSDAQ]:
            filtered_df = filtered_df[filtered_df["market"] == market_filter]

    if sector_filter:
        filtered_df = filtered_df[filtered_df["sector"].isin(sector_filter)]

    if custom_filters:
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


def get_filtered_stocks_data(
    market_filter: MarketEnum, codes: List[str], columns: Optional[List[str]] = None
) -> List[Dict]:
    if columns is None:
        columns = []
    required_columns = DEFAULT_COLUMNS + [col for col in columns if col not in DEFAULT_COLUMNS]

    df = get_df_from_parquet(market_filter)
    filtered_df = df[df["Code"].isin(codes)][required_columns]
    stocks_data = filtered_df.to_dict(orient="records")

    return stocks_data
