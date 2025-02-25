import pandas as pd
from app.database.crud import database
from typing import Dict, List, Optional
from Aws.logic.s3 import get_data_from_bucket
import io
from app.modules.screener.schemas import MarketEnum
from app.common.constants import DEFAULT_SCREENER_COLUMNS, NEED_TO_MULTIPLY_100, FACTOR_MAP
import numpy as np
from app.cache.factors import factors_cache
from app.core.extra.SlackNotifier import SlackNotifier
from app.models.models_factors import CategoryEnum

notifier = SlackNotifier()


class FactorUtils:
    def __init__(self):
        self.db = database

    def get_columns(self, category: Optional[CategoryEnum] = None) -> List[str]:
        db_columns = self.db._select(table="factors", columns=["factor"], category=category)

        result = []
        for column_tuple in db_columns:
            column_name = column_tuple[0]

            print(f"Extracted column name: {column_name}")

            if column_name in FACTOR_MAP:
                mapped_name = FACTOR_MAP[column_name]
                result.append(mapped_name)
            else:
                result.append(column_name)

        return result

    def process_kr_factor_data(self):
        output_file = "parquet/kr_stock_factors.parquet"
        factors_mapping = factors_cache.get_configs()

        result = get_data_from_bucket(bucket="quantus-ticker-prices", key="factor_ko_active.parquet", dir="port/")
        df = pd.read_parquet(io.BytesIO(result))

        df["merge_code"] = df["Code"]

        stock_information = self.db._select("stock_information", columns=["ticker", "sector_ko"], ctry="kr")
        stock_info_df = pd.DataFrame(stock_information)
        stock_info_df = stock_info_df.rename(columns={"ticker": "merge_code"})
        stock_info_df["sector"] = stock_info_df["sector_ko"].fillna("기타")

        df = pd.merge(df, stock_info_df, on="merge_code", how="left")
        df = df.drop("merge_code", axis=1)
        df["country"] = "kr"

        market_mapping = {"KRX": "KOSDAQ", "KOS": "KOSPI"}

        selected_columns = [
            "Code",
            "ExchMnem",
            "country",
            "sector",
            "Name",
        ] + list(factors_mapping.keys())
        df_selected = df[selected_columns]
        df_result = df_selected[df_selected["ExchMnem"].isin(market_mapping.keys())].copy()

        self.validate_integer_parts(df, df_result)

        df_result["ExchMnem"] = df_result["ExchMnem"].map(market_mapping)

        for column in NEED_TO_MULTIPLY_100:
            df_result[column] = df_result[column] * 100

        for column in df_result.columns:
            if np.issubdtype(df_result[column].dtype, np.number):
                df_result[column] = np.round(df_result[column].astype(np.float64), 2)

        df_result.to_parquet(output_file)

    def process_us_factor_data(self):
        output_file = "parquet/us_stock_factors.parquet"
        factors_mapping = factors_cache.get_configs()

        result = get_data_from_bucket(bucket="quantus-ticker-prices", key="factor_us_active.parquet", dir="port/")
        df = pd.read_parquet(io.BytesIO(result))

        df["merge_code"] = df["Code"].str.replace("-US", "")

        stock_information = self.db._select("stock_information", columns=["ticker", "is_snp_500", "sector_ko"], ctry="us")
        stock_info_df = pd.DataFrame(stock_information)
        stock_info_df = stock_info_df.rename(columns={"ticker": "merge_code"})
        stock_info_df["is_snp_500"] = stock_info_df["is_snp_500"].fillna(0).astype(int)
        stock_info_df["sector"] = stock_info_df["sector_ko"].fillna("기타")

        df = pd.merge(df, stock_info_df, on="merge_code", how="left")
        df = df.drop("merge_code", axis=1)
        df["country"] = "us"
        df["is_snp_500"] = df["is_snp_500"].fillna(0).astype(int)

        market_mapping = {"NAS": "NASDAQ", "NYS": "NYSE"}

        selected_columns = [
            "Code",
            "ExchMnem",
            "country",
            "sector",
            "Name",
            "is_snp_500",
        ] + list(factors_mapping.keys())

        df_selected = df[selected_columns]
        df_result = df_selected[df_selected["ExchMnem"].isin(market_mapping.keys())].copy()

        self.validate_integer_parts(df, df_result)

        df_result["ExchMnem"] = df_result["ExchMnem"].map(market_mapping)

        for column in NEED_TO_MULTIPLY_100:
            df_result[column] = df_result[column] * 100

        for column in df_result.columns:
            if np.issubdtype(df_result[column].dtype, np.number):
                df_result[column] = np.round(df_result[column].astype(np.float64), 2)

        df_result.to_parquet(output_file)

    def get_df_from_parquet(self, market_filter: MarketEnum) -> pd.DataFrame:
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
        self,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
    ) -> List[str]:
        df = self.get_df_from_parquet(market_filter)
        filtered_df = df.copy()

        # 종목 필터링
        if market_filter:
            if market_filter == MarketEnum.US:
                filtered_df = filtered_df[filtered_df["country"] == "us"]
            elif market_filter == MarketEnum.KR:
                filtered_df = filtered_df[filtered_df["country"] == "kr"]
            elif market_filter == MarketEnum.SNP500:
                filtered_df = filtered_df[filtered_df["is_snp_500"] == 1]
            elif market_filter in [MarketEnum.NASDAQ, MarketEnum.KOSDAQ, MarketEnum.KOSPI]:
                filtered_df = filtered_df[filtered_df["ExchMnem"] == market_filter.value]

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

    def get_filtered_stocks_df(
        self, market_filter: MarketEnum, codes: List[str], columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        if columns is None:
            columns = []
        required_columns = DEFAULT_SCREENER_COLUMNS + [col for col in columns if col not in DEFAULT_SCREENER_COLUMNS]

        df = self.get_df_from_parquet(market_filter)
        filtered_df = df[df["Code"].isin(codes)][required_columns]

        return filtered_df

    def validate_integer_parts(self, original_df: pd.DataFrame, processed_df: pd.DataFrame):
        notifier.notify_info("팩터 정수 부분 불일치 검증 시작")

        def safe_int(x):
            if pd.isna(x) or np.isinf(x):
                return 0
            return int(x)

        original_df = original_df.sort_values("Code")
        processed_df = processed_df.sort_values("Code")

        common_codes = set(original_df["Code"]) & set(processed_df["Code"])
        original_df = original_df[original_df["Code"].isin(common_codes)]
        processed_df = processed_df[processed_df["Code"].isin(common_codes)]

        for col in processed_df.columns:
            if np.issubdtype(processed_df[col].dtype, np.number):
                if col in ["Code", "ExchMnem", "sector", "Name", "country"]:
                    continue

                original_ints = original_df[col].fillna(0).astype(float).apply(safe_int)
                processed_ints = processed_df[col].fillna(0).astype(float).apply(safe_int)

                mismatch_mask = original_ints.values != processed_ints.values
                mismatches = mismatch_mask.sum()
                print(f"{col} - 불일치 건수: {mismatches}")

                if mismatches > 0:
                    mismatch_indices = np.where(mismatch_mask)[0]
                    print("불일치 상세:")
                    for idx in mismatch_indices:
                        code = original_df.iloc[idx]["Code"]
                        original_val = original_ints.iloc[idx]
                        processed_val = processed_ints.iloc[idx]
                        print(f"  Code: {code}, 원본: {original_val}, 처리후: {processed_val}")
                        notifier.notify_error(
                            f"팩터 정수 부분 불일치 발생\n원본: {original_val}, 처리후: {processed_val}", "김광윤"
                        )

        notifier.notify_info("팩터 정수 부분 불일치 검증 완료")

    def convert_unit_and_value(
        self, market_filter: MarketEnum, value: float, unit: str, small_price: bool = False
    ) -> tuple[float, str]:
        if unit.lower() == "price":
            nation = "kr" if market_filter in [MarketEnum.KR, MarketEnum.KOSPI, MarketEnum.KOSDAQ] else "us"

            if nation == "kr":
                if small_price:
                    return value, "원"
                if value >= 10000:  # 1조원 이상
                    return value / 10000, "조원"
                return value, "억원"
            else:  # US
                if small_price:
                    return value, "$"
                # 1T = 1000B = 1000조원
                if value >= 1000000000:  # 1000조원 이상
                    return value / 1000000000, "$T"
                # 1B = 1조원
                elif value >= 1000000:  # 1조원 이상
                    return value / 1000000, "$B"
                # 1M = 10억원
                elif value >= 1000:  # 10억원 이상
                    return value / 1000, "$M"
                # 1K = 100만원
                return value, "$K"

        unit_mapping = {"percentage": "%", "times": "회", "score": "점", "multiple": "배"}
        return value, unit_mapping.get(unit.lower(), "")


factor_utils = FactorUtils()

if __name__ == "__main__":
    factor_utils.process_kr_factor_data()
    factor_utils.process_us_factor_data()
