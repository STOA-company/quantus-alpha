import pandas as pd
from app.database.crud import database
from typing import Dict, List
from app.modules.screener.schemas import FilterCondition
from Aws.logic.s3 import get_data_from_bucket
import io


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


def filter_stocks(filters: List[FilterCondition]) -> List[str]:
    df = pd.read_parquet("parquet/stock_factors.parquet")
    filtered_df = df.copy()

    for filter in filters:
        factor = filter["factor"]

        if factor not in filtered_df.columns:
            raise ValueError(f"팩터 '{factor}'가 데이터에 존재하지 않습니다.")

        if filter["above"] is not None:
            filtered_df = filtered_df[filtered_df[factor] >= filter["above"]]

        if filter["below"] is not None:
            filtered_df = filtered_df[filtered_df[factor] <= filter["below"]]

    stock_codes = filtered_df["Code"].tolist()

    return stock_codes


def get_stocks_data(codes: List[str]) -> List[Dict]:
    df = pd.read_parquet("parquet/stock_factors.parquet")
    filtered_df = df[df["Code"].isin(codes)]
    stocks_data = filtered_df.to_dict(orient="records")

    return stocks_data
