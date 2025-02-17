import pandas as pd
from app.database.crud import database
from typing import Dict, List
from app.modules.screener.schemas import FilterRequest


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
    # TODO: S3로부터 한국, 미국 parquet 파일 모두 불러온 후 하나의 parquet 파일로 저장 (나라 컬럼 추가)
    # TODO: 미국, 한국 화폐 단위
    output_file = "stock_factors.parquet"

    factors_mapping = get_factors_from_db()
    kr_df = pd.read_parquet("factor_ko_active.parquet")
    us_df = pd.read_parquet("factor_us_active.parquet")

    kr_df["country"] = "kr"
    us_df["country"] = "us"

    df = pd.concat([kr_df, us_df])

    selected_columns = ["Code"] + list(factors_mapping.keys())
    df_selected = df[selected_columns]
    df_selected.to_parquet(output_file)


def filter_stocks(filters: List[FilterRequest]) -> List[str]:
    # TODO: 나라 필터링 추가
    df = pd.read_parquet("stock_factors.parquet")
    filtered_df = df.copy()

    for filter in filters:
        factor = filter.factor

        if factor not in filtered_df.columns:
            raise ValueError(f"팩터 '{factor}'가 데이터에 존재하지 않습니다.")

        if filter.above is not None:
            filtered_df = filtered_df[filtered_df[factor] >= filter.above]

        if filter.below is not None:
            filtered_df = filtered_df[filtered_df[factor] <= filter.below]

    stock_codes = filtered_df["Code"].tolist()

    return stock_codes


def get_stocks_data(df: pd.DataFrame, codes: List[str]) -> List[Dict]:
    filtered_df = df[df["Code"].isin(codes)]
    stocks_data = filtered_df.to_dict(orient="records")

    return stocks_data
