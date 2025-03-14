import pandas as pd
from app.database.crud import database
from typing import Dict, List, Optional
from Aws.logic.s3 import get_data_from_bucket
import io
from app.modules.screener.stock.schemas import MarketEnum
from app.common.constants import NEED_TO_MULTIPLY_100, FACTOR_MAP, MARKET_MAP, UNIT_MAP, UNIT_MAP_EN
import numpy as np
from app.cache.factors import factors_cache
from app.modules.screener.etf.enum import ETFMarketEnum
from app.core.extra.SlackNotifier import SlackNotifier
from app.models.models_factors import CategoryEnum
import logging
from app.utils.data_utils import ceil_to_integer, floor_to_integer
from app.utils.date_utils import is_holiday
from datetime import datetime, timedelta
from Aws.logic.s3 import upload_file_to_bucket
from app.modules.screener.etf.utils import ETFDataLoader
from pandas.api.types import is_numeric_dtype
from app.modules.screener.stock.schemas import StockType

logger = logging.getLogger(__name__)

notifier = SlackNotifier()


class ScreenerUtils:
    def __init__(self):
        self.db = database
        self.lang = "kr"
        self.etf_factor_loader = ETFDataLoader()

    def get_factors(self, market: MarketEnum) -> List[dict]:
        factors = self.db._select(table="factors", is_stock=True)
        # 시장별 팩터 최소/최대값 계산
        market_data = self.get_df_from_parquet(market)

        result = []
        for factor in factors:
            factor_name = factor.factor

            if factor_name in market_data.columns:
                min_value = market_data[factor_name].min()
                max_value = market_data[factor_name].max()

                result.append(
                    {
                        "factor": FACTOR_MAP[factor_name],
                        "description": factor.description,
                        "unit": str(factor.unit).lower(),
                        "category": str(factor.category).lower(),
                        "direction": factor.sort_direction,
                        "min_value": floor_to_integer(min_value),
                        "max_value": ceil_to_integer(max_value),
                    }
                )

            else:
                raise ValueError(f"팩터 '{factor_name}'가 데이터에 존재하지 않습니다.")

        return result

    def get_etf_factors(self, market: ETFMarketEnum) -> List[dict]:
        factors = self.db._select(table="factors", is_etf=True)
        # 시장별 팩터 최소/최대값 계산
        market_data = self.etf_factor_loader.load_etf_factors(market.value)

        result = []
        for factor in factors:
            factor_name = factor.factor

            if factor_name in market_data.columns:
                min_value = None
                max_value = None
                if is_numeric_dtype(market_data[factor_name]):
                    min_value = floor_to_integer(market_data[factor_name].min())
                    max_value = ceil_to_integer(market_data[factor_name].max())

                result.append(
                    {
                        "factor": FACTOR_MAP[factor_name],
                        "description": factor.description,
                        "unit": str(factor.unit).lower(),
                        "category": str(factor.category).lower(),
                        "direction": factor.sort_direction,
                        "min_value": min_value,
                        "max_value": max_value,
                    }
                )

            else:
                raise ValueError(f"팩터 '{factor_name}'가 데이터에 존재하지 않습니다.")

        return result

    def get_default_columns(
        self,
        category: Optional[CategoryEnum] = None,
        type: Optional[StockType] = None,
    ) -> List[str]:
        base_columns = ["score", "sector", "market"]

        if type == StockType.ETF:
            base_columns.remove("sector")

        if not category:
            return base_columns

        additional_columns = {
            CategoryEnum.TECHNICAL: ["beta", "rsi_14", "sharpe", "momentum_6", "vol"]
            if type == StockType.STOCK
            else ["median_trade", "rsi_14", "sharpe", "momentum_6", "vol"],
            CategoryEnum.FUNDAMENTAL: ["roe", "fscore", "deptRatio", "operating_income", "z_score"],
            CategoryEnum.VALUATION: ["pbr", "pcr", "per", "por", "psr"],
            CategoryEnum.DIVIDEND: [
                "dividend_count",
                "total_fee",
                "last_dividend_per_share",
                "dividend_growth_rate_5y",
                "risk_rating",
            ],
        }

        return [*base_columns, *additional_columns.get(category, [])]

    def process_kr_factor_data(self):
        output_file = "parquet/kr_stock_factors.parquet"
        factors_mapping = factors_cache.get_configs()

        result = get_data_from_bucket(bucket="quantus-ticker-prices", key="factor_ko_active.parquet", dir="port/")
        df = pd.read_parquet(io.BytesIO(result))

        df["merge_code"] = df["Code"]

        stock_information = self.db._select(
            "stock_information",
            columns=["ticker", "kr_name", "en_name", "market", "sector_ko", "sector_2", "is_activate", "is_delisted"],
            ctry="kr",
        )
        stock_info_df = pd.DataFrame(stock_information)
        stock_info_df = stock_info_df.rename(columns={"ticker": "merge_code"})
        stock_info_df["sector"] = stock_info_df["sector_ko"].fillna("기타")
        stock_info_df["sector_en"] = stock_info_df["sector_2"].fillna("Other")

        # INNER JOIN
        df = pd.merge(df, stock_info_df, on="merge_code", how="inner")
        df["Name"] = df["kr_name"]
        df["Name_en"] = df["en_name"]
        df = df.drop(["merge_code", "kr_name", "en_name"], axis=1)

        df["country"] = "kr"

        # NAN 값 처리
        df["is_activate"] = df["is_activate"].fillna(1).astype(int)
        df["is_delisted"] = df["is_delisted"].fillna(0).astype(int)

        df = df[(df["is_activate"] == 1) & (df["is_delisted"] == 0)]

        selected_columns = [
            "Code",
            "market",
            "country",
            "sector",
            "sector_en",
            "Name",
            "Name_en",
            "is_activate",
            "is_delisted",
        ] + list(factors_mapping.keys())

        df_selected = df[selected_columns]
        df_result = df_selected[df_selected["market"].isin(["KOSPI", "KOSDAQ"])].copy()

        for column in df_result.columns:
            if np.issubdtype(df_result[column].dtype, np.number):
                df_result[column] = df_result[column].replace([np.inf, -np.inf], np.nan)

        self.validate_integer_parts(df, df_result)

        df_result["market"] = df_result["market"].map(MARKET_MAP)

        for column in NEED_TO_MULTIPLY_100:
            df_result[column] = df_result[column] * 100

        for column in df_result.columns:
            if np.issubdtype(df_result[column].dtype, np.number):
                df_result[column] = df_result[column].astype(np.float64)

        df_result.to_parquet(output_file)

    def process_us_factor_data(self):
        output_file = "parquet/us_stock_factors.parquet"
        factors_mapping = factors_cache.get_configs()

        result = get_data_from_bucket(bucket="quantus-ticker-prices", key="factor_us_active.parquet", dir="port/")
        df = pd.read_parquet(io.BytesIO(result))

        df["merge_code"] = df["Code"].str.replace("-US", "")

        stock_information = self.db._select(
            "stock_information",
            columns=[
                "ticker",
                "kr_name",
                "en_name",
                "market",
                "is_snp_500",
                "sector_ko",
                "sector_2",
                "is_activate",
                "is_delisted",
            ],
            ctry="us",
        )
        stock_info_df = pd.DataFrame(stock_information)
        stock_info_df = stock_info_df.rename(columns={"ticker": "merge_code"})
        stock_info_df["sector"] = stock_info_df["sector_ko"].fillna("기타")
        stock_info_df["sector_en"] = stock_info_df["sector_2"].fillna("Other")

        # INNER JOIN
        df = pd.merge(df, stock_info_df, on="merge_code", how="inner")
        df["Name"] = df["kr_name"]
        df["Name_en"] = df["en_name"]
        df = df.drop(["merge_code", "kr_name", "en_name"], axis=1)

        df["country"] = "us"

        # NAN 값 처리
        df["is_snp_500"] = df["is_snp_500"].fillna(0).astype(int)
        df["is_activate"] = df["is_activate"].fillna(1).astype(int)
        df["is_delisted"] = df["is_delisted"].fillna(0).astype(int)

        df = df[(df["is_activate"] == 1) & (df["is_delisted"] == 0)]

        selected_columns = [
            "Code",
            "market",
            "country",
            "sector",
            "sector_en",
            "Name",
            "Name_en",
            "is_snp_500",
            "is_activate",
            "is_delisted",
        ] + list(factors_mapping.keys())

        df_selected = df[selected_columns]
        df_result = df_selected[df_selected["market"].isin(["NAS", "NYS"])].copy()

        for column in df_result.columns:
            if np.issubdtype(df_result[column].dtype, np.number):
                df_result[column] = df_result[column].replace([np.inf, -np.inf], np.nan)

        self.validate_integer_parts(df, df_result)

        df_result["market"] = df_result["market"].map(MARKET_MAP)

        for column in NEED_TO_MULTIPLY_100:
            df_result[column] = df_result[column] * 100

        for column in df_result.columns:
            if np.issubdtype(df_result[column].dtype, np.number):
                df_result[column] = df_result[column].astype(np.float64)

        df_result.to_parquet(output_file)

    def get_df_from_parquet(self, market_filter: MarketEnum) -> pd.DataFrame:
        df = None
        if market_filter:
            if market_filter in [MarketEnum.US, MarketEnum.NASDAQ, MarketEnum.SNP500]:
                df = pd.read_parquet("parquet/us_stock_factors.parquet")
            elif market_filter in [MarketEnum.KR, MarketEnum.KOSPI, MarketEnum.KOSDAQ]:
                df = pd.read_parquet("parquet/kr_stock_factors.parquet")

            if market_filter in [MarketEnum.KOSPI, MarketEnum.KOSDAQ, MarketEnum.NASDAQ]:
                df = df[df["market"] == market_filter.value]

            if market_filter == MarketEnum.SNP500:
                df = df[df["is_snp_500"] == 1]
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
                filtered_df = filtered_df[filtered_df["market"] == market_filter.value]

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

    def filter_etfs(
        self,
        market_filter: Optional[ETFMarketEnum] = None,
        custom_filters: Optional[List[Dict]] = None,
    ) -> List[str]:
        df = self.etf_factor_loader.load_etf_factors(market_filter)
        filtered_df = df.copy()

        # 종목 필터링
        if market_filter:
            if market_filter == ETFMarketEnum.US:
                filtered_df = filtered_df[filtered_df["country"] == "us"]
            elif market_filter == ETFMarketEnum.KR:
                filtered_df = filtered_df[filtered_df["country"] == "kr"]
            elif market_filter in [ETFMarketEnum.NASDAQ, ETFMarketEnum.NYSE, ETFMarketEnum.BATS]:
                filtered_df = filtered_df[filtered_df["market"] == market_filter.value.upper()]

        if custom_filters:
            for filter in custom_filters:
                factor = filter["factor"]
                if factor not in filtered_df.columns:
                    raise ValueError(f"팩터 '{factor}'가 데이터에 존재하지 않습니다.")
                if filter["above"] is not None:
                    filtered_df = filtered_df[filtered_df[factor] >= filter["above"]]
                if filter["below"] is not None:
                    filtered_df = filtered_df[filtered_df[factor] <= filter["below"]]

        etf_tickers = filtered_df["Code"].tolist()
        return etf_tickers

    def get_filtered_stocks_df(
        self, market_filter: MarketEnum, codes: List[str], columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        if columns is None:
            columns = []
        required_columns = columns.copy()
        if "score" in required_columns:
            required_columns.remove("score")

        df = self.get_df_from_parquet(market_filter)
        filtered_df = df[df["Code"].isin(codes)][required_columns]

        return filtered_df

    def get_filtered_etfs_df(
        self, market_filter: ETFMarketEnum, codes: List[str], columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        if columns is None:
            columns = []
        required_columns = columns.copy()
        if "score" in required_columns:
            required_columns.remove("score")

        df = self.etf_factor_loader.load_etf_factors(market_filter)
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
                if col in ["Code", "market", "sector", "Name", "country"]:
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
        self, market_filter: MarketEnum, value: float, unit: str, lang: str = "kr"
    ) -> tuple[float, str]:
        nation = "kr" if market_filter in [MarketEnum.KR, MarketEnum.KOSPI, MarketEnum.KOSDAQ] else "us"

        if unit.lower() == "big_price":
            if nation == "kr":
                if value >= 10000:  # 1조원 이상
                    return round(value / 10000, 2), "조원"
                return int(value), "억원"
            else:  # US
                # 1T = 1000B = 1000조원
                if value >= 1000000000:  # 1000조원 이상
                    return round(value / 1000000000, 2), "T$"
                # 1B = 1조원
                elif value >= 1000000:  # 1조원 이상
                    return round(value / 1000000, 2), "B$"
                # 1M = 10억원
                elif value >= 1000:  # 10억원 이상
                    return round(value / 1000, 2), "M$"
                # 1K = 100만원
                return round(value, 2), "K$"

        if unit.lower() == "small_price":
            if nation == "kr":
                return int(value), "원"
            else:
                return round(value, 2), "$"

        unit_map = UNIT_MAP
        if lang == "en":
            unit_map = UNIT_MAP_EN

        value = np.round(value, 2)
        return value, unit_map.get(unit.lower(), "")

    def archive_parquet(self, nation: str, type: str = "stock"):
        date = (
            datetime.now().strftime("%Y%m%d")
            if nation == "kr"
            else (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        )
        if is_holiday(nation.upper(), date):
            logger.info(f"Not archiving {date} stock factors")
            print(f"Not archiving {date} stock factors")
            return

        if nation == "kr":
            file_path = f"parquet/kr_{type}_factors.parquet"
            obj_path = f"{type}/kr/kr_{type}_factors_{date}.parquet"
        else:
            file_path = f"parquet/us_{type}_factors.parquet"
            obj_path = f"{type}/us/us_{type}_factors_{date}.parquet"

        upload_file_to_bucket(file_path, "alpha-finder-factors", obj_path)


screener_utils = ScreenerUtils()


if __name__ == "__main__":
    df = screener_utils.get_df_from_parquet(MarketEnum.US)
    print(df["market"].unique())
