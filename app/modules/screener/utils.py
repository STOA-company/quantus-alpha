import pandas as pd
from app.database.crud import database
from typing import Dict, List, Optional
from Aws.logic.s3 import get_data_from_bucket
import io
from app.modules.screener.stock.schemas import MarketEnum, ExcludeEnum
from app.common.constants import NEED_TO_MULTIPLY_100, MARKET_MAP, UNIT_MAP, UNIT_MAP_EN
import numpy as np
from app.modules.screener.etf.enum import ETFMarketEnum
from app.core.extra.SlackNotifier import SlackNotifier
from app.models.models_factors import CategoryEnum, FactorTypeEnum
import logging
from app.utils.data_utils import ceil_to_integer, floor_to_integer
from app.utils.date_utils import is_holiday
from datetime import datetime, timedelta
from Aws.logic.s3 import upload_file_to_bucket
from app.modules.screener.etf.utils import ETFDataLoader
from app.modules.screener.stock.schemas import StockType
from app.utils.test_utils import time_it
from app.kispy.manager import KISAPIManager
from app.utils.dividend_utils import DividendUtils

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
            factor_presets = self.db._select(table="factors_preset", factor=factor_name, order="order", ascending=True)
            classified_presets = self.classify_factors_preset(factor_presets)
            factor_type = factor.type.lower()
            min_value = None
            max_value = None
            if factor_type == FactorTypeEnum.SLIDER.value:
                min_value = market_data[factor_name].min()
                max_value = market_data[factor_name].max()

            result.append(
                {
                    "factor": factor_name,
                    "description": factor.description,
                    "unit": str(factor.unit).lower(),
                    "category": str(factor.category).lower(),
                    "direction": factor.sort_direction,
                    "min_value": floor_to_integer(min_value),
                    "max_value": ceil_to_integer(max_value),
                    "presets": classified_presets,
                    "type": factor_type,
                }
            )

        return result

    def get_etf_factors(self, market: ETFMarketEnum) -> List[dict]:
        factors = self.db._select(table="factors", is_etf=True)
        market_data = self.etf_factor_loader.load_etf_factors(market.value)

        result = []
        for factor in factors:
            factor_name = factor.factor
            factor_presets = self.db._select(table="factors_preset", factor=factor_name, order="order", ascending=True)
            classified_presets = self.classify_factors_preset(factor_presets)
            factor_type = factor.type.lower()

            if factor_name in market_data.columns:
                min_value = None
                max_value = None
                if factor_type == FactorTypeEnum.SLIDER.value:
                    market_data[factor_name] = pd.to_numeric(market_data[factor_name], errors="coerce")
                    valid_data = market_data[factor_name].dropna()
                    if not valid_data.empty:
                        min_value = valid_data.min()
                        max_value = valid_data.max()

                result.append(
                    {
                        "factor": factor_name,
                        "description": factor.description,
                        "unit": str(factor.unit).lower(),
                        "category": str(factor.category).lower(),
                        "direction": factor.sort_direction,
                        "min_value": floor_to_integer(min_value),
                        "max_value": ceil_to_integer(max_value),
                        "presets": classified_presets,
                        "type": factor_type,
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

        # technical_columns = ["beta", "rsi_14", "sharpe", "momentum_6", "vol"]
        technical_columns = ["close", "marketCap", "median_trade", "abs_beta", "Log_RS_100", "sharpe"]
        dividend_columns = [
            "dividend_count",
            "ttm_dividend_yield",
            "consecutive_dividend_growth_count",
            "div_yield_growth_qoq",
            "div_yield_growth_yoy",
        ]
        if type == StockType.ETF:
            # technical_columns = ["median_trade", "rsi_14", "sharpe", "momentum_6", "vol"]
            technical_columns = ["close", "marketCap", "median_trade", "momentum_6", "sharpe", "sortino"]

            dividend_columns = [
                "ttm_dividend_yield",
                "dividend_count",
                "last_dividend_per_share",
                "dividend_growth_rate_5y",
                "risk_rating",
            ]

        additional_columns = {
            CategoryEnum.TECHNICAL: technical_columns,
            CategoryEnum.FUNDAMENTAL: ["roe", "fscore", "deptRatio", "operating_income", "z_score"],
            CategoryEnum.VALUATION: ["pbr", "pcr", "per", "por", "psr"],
            CategoryEnum.DIVIDEND: dividend_columns,
            CategoryEnum.GROWTH: [
                "rv_growth_yoy",
                "op_growth_yoy",
                "net_profit_growth_yoy",
                "operating_cashflow_growth_yoy",
                "rev_acceleration_yoy",
            ],
        }

        return [*base_columns, *additional_columns.get(category, [])]

    def process_kr_factor_data(self):
        output_file = "parquet/kr_stock_factors.parquet"

        result = get_data_from_bucket(bucket="quantus-ticker-prices", key="factor_ko_active.parquet", dir="port/")
        df = pd.read_parquet(io.BytesIO(result))

        df["merge_code"] = df["Code"]

        stock_information = self.db._select(
            "stock_information",
            columns=[
                "ticker",
                "kr_name",
                "en_name",
                "market",
                "sector_ko",
                "sector_2",
                "is_activate",
                "is_delisted",
                "is_warned",
            ],
            ctry="kr",
        )
        stock_info_df = pd.DataFrame(stock_information)
        stock_info_df = stock_info_df.rename(columns={"ticker": "merge_code"})
        stock_info_df["sector"] = stock_info_df["sector_ko"].fillna("기타")
        stock_info_df["sector_en"] = stock_info_df["sector_2"].fillna("Other")

        df = pd.merge(df, stock_info_df, on="merge_code", how="inner")
        df["Name"] = df["kr_name"]
        df["Name_en"] = df["en_name"]
        df = df.drop(["merge_code", "kr_name", "en_name"], axis=1)

        df["country"] = "kr"

        df["is_activate"] = df["is_activate"].fillna(1).astype(int)
        df["is_delisted"] = df["is_delisted"].fillna(0).astype(int)
        df["is_warned"] = df["is_warned"].fillna(0).astype(int)

        df = df[(df["is_activate"] == 1) & (df["is_delisted"] == 0)]

        unique_tickers = df["Code"].unique().tolist()

        dividend_data = self._get_dividend_data_for_tickers(unique_tickers)
        dividend_utils = DividendUtils()
        dividend_frequencies = dividend_utils.get_dividend_frequency(unique_tickers)

        df["ttm_dividend_yield"] = np.nan
        df["consecutive_dividend_growth_count"] = np.nan
        df["consecutive_dividend_payment_count"] = np.nan
        df["dividend_count"] = np.nan
        df["dividend_frequency"] = ""

        for index, row in df.iterrows():
            ticker = row["Code"]

            if ticker in dividend_data["ttm_yield"]:
                df.at[index, "ttm_dividend_yield"] = round(dividend_data["ttm_yield"][ticker], 2)

            if ticker in dividend_data["consecutive_dividend_growth_count"]:
                df.at[index, "consecutive_dividend_growth_count"] = dividend_data["consecutive_dividend_growth_count"][
                    ticker
                ]

            if ticker in dividend_data["consecutive_dividend_payment_count"]:
                df.at[index, "consecutive_dividend_payment_count"] = dividend_data["consecutive_dividend_payment_count"][
                    ticker
                ]

            if ticker in dividend_data["dividend_count"]:
                df.at[index, "dividend_count"] = dividend_data["dividend_count"][ticker]

            if ticker in dividend_frequencies:
                df.at[index, "dividend_frequency"] = dividend_frequencies[ticker]

        # 필터링된 데이터프레임 선택 (모든 컬럼 유지)
        df_result = df[df["market"].isin(["KOSPI", "KOSDAQ"])].copy()

        for column in df_result.columns:
            if np.issubdtype(df_result[column].dtypes, np.number):
                df_result[column] = df_result[column].replace([np.inf, -np.inf], np.nan)

        self.validate_integer_parts(df, df_result)

        df_result["market"] = df_result["market"].map(MARKET_MAP)

        for column in NEED_TO_MULTIPLY_100:
            df_result[column] = df_result[column] * 100

        if "ttm_dividend_yield" in df_result.columns:
            df_result["ttm_dividend_yield"] = df_result["ttm_dividend_yield"].round(2)

        if "consecutive_dividend_payment_count" in df_result.columns:
            df_result["consecutive_dividend_payment_count"] = (
                df_result["consecutive_dividend_payment_count"].fillna(0).astype(np.int32)
            )

        if "dividend_count" in df_result.columns:
            df_result["dividend_count"] = df_result["dividend_count"].fillna(0).astype(np.int32)

        for column in df_result.columns:
            if np.issubdtype(df_result[column].dtypes, np.number):
                df_result[column] = df_result[column].astype(np.float64)

        df_result["consecutive_dividend_growth_count"] = (
            df_result["consecutive_dividend_growth_count"].fillna(0).astype(np.int32)
        )

        df_result.to_parquet(output_file)

    def process_us_factor_data(self):
        output_file = "parquet/us_stock_factors.parquet"

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

        df = pd.merge(df, stock_info_df, on="merge_code", how="inner")

        df["Name"] = df["kr_name"]
        df["Name_en"] = df["en_name"]
        df = df.drop(["merge_code", "kr_name", "en_name"], axis=1)

        df["Code"] = df["Code"].str.replace("-US", "")

        df["country"] = "us"

        df["is_snp_500"] = df["is_snp_500"].fillna(0).astype(int)
        df["is_activate"] = df["is_activate"].fillna(1).astype(int)
        df["is_delisted"] = df["is_delisted"].fillna(0).astype(int)

        df = df[(df["is_activate"] == 1) & (df["is_delisted"] == 0)]

        unique_tickers = df["Code"].unique().tolist()

        dividend_data = self._get_dividend_data_for_tickers(unique_tickers)
        dividend_utils = DividendUtils()
        dividend_frequencies = dividend_utils.get_dividend_frequency(unique_tickers)

        df["ttm_dividend_yield"] = np.nan
        df["consecutive_dividend_growth_count"] = np.nan
        df["consecutive_dividend_payment_count"] = np.nan
        df["dividend_count"] = np.nan
        df["dividend_frequency"] = ""
        df["last_dividend_per_share"] = np.nan

        for index, row in df.iterrows():
            ticker = row["Code"]

            if ticker in dividend_data["ttm_yield"]:
                df.at[index, "ttm_dividend_yield"] = round(dividend_data["ttm_yield"][ticker], 2)

            if ticker in dividend_data["consecutive_dividend_growth_count"]:
                df.at[index, "consecutive_dividend_growth_count"] = dividend_data["consecutive_dividend_growth_count"][
                    ticker
                ]

            if ticker in dividend_data["consecutive_dividend_payment_count"]:
                df.at[index, "consecutive_dividend_payment_count"] = dividend_data["consecutive_dividend_payment_count"][
                    ticker
                ]

            if ticker in dividend_data["dividend_count"]:
                df.at[index, "dividend_count"] = dividend_data["dividend_count"][ticker]

            if ticker in dividend_data["dividend_per_share"]:
                df.at[index, "last_dividend_per_share"] = dividend_data["dividend_per_share"][ticker]

            if ticker in dividend_frequencies:
                df.at[index, "dividend_frequency"] = dividend_frequencies[ticker]

        # 필터링된 데이터프레임 선택 (모든 컬럼 유지)
        df_result = df[df["market"].isin(["NAS", "NYS"])].copy()

        for column in df_result.columns:
            if np.issubdtype(df_result[column].dtypes, np.number):
                df_result[column] = df_result[column].replace([np.inf, -np.inf], np.nan)

        self.validate_integer_parts(df, df_result)

        df_result["market"] = df_result["market"].map(MARKET_MAP)

        for column in NEED_TO_MULTIPLY_100:
            df_result[column] = df_result[column] * 100

        if "ttm_dividend_yield" in df_result.columns:
            df_result["ttm_dividend_yield"] = df_result["ttm_dividend_yield"].round(2)

        if "consecutive_dividend_payment_count" in df_result.columns:
            df_result["consecutive_dividend_payment_count"] = (
                df_result["consecutive_dividend_payment_count"].fillna(0).astype(np.int32)
            )

        if "dividend_count" in df_result.columns:
            df_result["dividend_count"] = df_result["dividend_count"].fillna(0).astype(np.int32)

        if "last_dividend_per_share" in df_result.columns:
            df_result["last_dividend_per_share"] = df_result["last_dividend_per_share"].fillna(0).astype(np.float64)

        for column in df_result.columns:
            if np.issubdtype(df_result[column].dtypes, np.number):
                df_result[column] = df_result[column].astype(np.float64)

        df_result["consecutive_dividend_growth_count"] = (
            df_result["consecutive_dividend_growth_count"].fillna(0).astype(np.int32)
        )

        df_result.to_parquet(output_file)

    def _get_dividend_data_for_tickers(self, tickers):
        if not tickers:
            return {
                "ttm_yield": {},
                "consecutive_dividend_growth_count": {},
                "consecutive_dividend_payment_count": {},
                "dividend_count": {},
                "dividend_per_share": {},
            }

        dividend_utils = DividendUtils()
        ttm_yield_dict = dividend_utils.get_ttm_dividend_yield(tickers)
        consecutive_dividend_growth_count_dict = dividend_utils.get_consecutive_dividend_growth_count(tickers)
        consecutive_dividend_payment_count_dict = dividend_utils.get_consecutive_dividend_payment_count(tickers)
        dividend_count_dict = dividend_utils.get_dividend_count(tickers)
        dividend_per_share_dict = dividend_utils.get_latest_dividend_per_share(tickers)

        return {
            "ttm_yield": ttm_yield_dict,
            "consecutive_dividend_growth_count": consecutive_dividend_growth_count_dict,
            "consecutive_dividend_payment_count": consecutive_dividend_payment_count_dict,
            "dividend_count": dividend_count_dict,
            "dividend_per_share": dividend_per_share_dict,
        }

    def process_global_factor_data(self):
        big_price_columns = [
            "gross_profit",
            "gross_profit_ttm",
            "marketCap",
            "median_trade",
            "operating_income",
            "operating_income_ttm",
            "rev",
            "rev_ttm",
        ]
        small_price_columns = ["close"]

        manager = KISAPIManager()
        exchange_rate = manager.get_api().get_exchange_rates()
        kr_df = self.get_df_from_parquet(MarketEnum.KR)
        for column in big_price_columns:
            if column in kr_df.columns:
                kr_df[column] = self.convert_krw_billion_to_usd(kr_df[column], exchange_rate)
        for column in small_price_columns:
            if column in kr_df.columns:
                kr_df[column] = kr_df[column] / exchange_rate

        us_df = self.get_df_from_parquet(MarketEnum.US)

        kr_df["country"] = "kr"
        us_df["country"] = "us"

        df = pd.concat([kr_df, us_df])

        df.to_parquet("parquet/global_stock_factors.parquet")

    def process_global_etf_factor_data(self):
        big_price_columns = [
            "marketCap",
            "median_trade",
        ]
        small_price_columns = ["ba_absolute_spread", "ba_relative_spread", "close", "last_dividend_per_share"]

        manager = KISAPIManager()
        exchange_rate = manager.get_api().get_exchange_rates()

        kr_df = self.etf_factor_loader.load_etf_factors(ETFMarketEnum.KR)
        us_df = self.etf_factor_loader.load_etf_factors(ETFMarketEnum.US)

        for column in big_price_columns:
            kr_df[column] = self.convert_krw_billion_to_usd(kr_df[column], exchange_rate)
        for column in small_price_columns:
            kr_df[column] = kr_df[column] / exchange_rate

        kr_df["country"] = "kr"
        us_df["country"] = "us"

        if "total_fee" in kr_df.columns:
            kr_df["total_fee"] = pd.to_numeric(kr_df["total_fee"], errors="coerce")

        if "total_fee" in us_df.columns:
            us_df["total_fee"] = pd.to_numeric(us_df["total_fee"], errors="coerce")

        for column in kr_df.columns:
            if np.issubdtype(kr_df[column].dtypes, np.number):
                kr_df[column] = kr_df[column].astype(np.float64)
        for column in us_df.columns:
            if np.issubdtype(us_df[column].dtypes, np.number):
                us_df[column] = us_df[column].astype(np.float64)

        df = pd.concat([kr_df, us_df])

        df.to_parquet("parquet/global_etf_factors.parquet")

    def get_df_from_parquet(self, market_filter: MarketEnum) -> pd.DataFrame:
        df = None
        if market_filter:
            if market_filter == MarketEnum.ALL:
                df = pd.read_parquet("parquet/global_stock_factors.parquet")
            elif market_filter in [MarketEnum.US, MarketEnum.NASDAQ, MarketEnum.SNP500]:
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

    @time_it
    def filter_stocks(
        self,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        exclude_filters: Optional[List[ExcludeEnum]] = None,
    ) -> List[str]:
        df = self.get_df_from_parquet(market_filter)
        if exclude_filters:
            df = self.add_exclude_flags_to_dataframe(df, exclude_filters)
            # 제외 필터 적용
            if ExcludeEnum.FINANCIAL in exclude_filters:
                df = df[~df["is_financial"]]
            if ExcludeEnum.HOLDING in exclude_filters:
                df = df[~df["is_holding"]]
            if ExcludeEnum.WARNED in exclude_filters:
                df = df[~df["is_warned"]]
            if ExcludeEnum.DEFICIT in exclude_filters:
                df = df[~df["is_deficit"]]
            if ExcludeEnum.ANNUAL_DEFICIT in exclude_filters:
                df = df[~df["is_annual_deficit"]]
            if ExcludeEnum.PTP in exclude_filters:
                df = df[~df["is_ptp"]]

        # 종목 필터링
        if market_filter:
            if market_filter == MarketEnum.SNP500:
                df = df[df["is_snp_500"] == 1]
            elif market_filter in [MarketEnum.NASDAQ, MarketEnum.KOSDAQ, MarketEnum.KOSPI]:
                df = df[df["market"] == market_filter.value]

        if sector_filter:
            df = df[df["sector"].isin(sector_filter)]

        if custom_filters:
            for filter in custom_filters:
                factor = filter["factor"]
                if factor not in df.columns:
                    raise ValueError(f"팩터 '{factor}'가 데이터에 존재하지 않습니다.")

                # SLIDER 타입 팩터인 경우 NULL 값을 가진 종목 제외
                factor_info = self.db._select(table="factors", factor=factor)
                if factor_info and factor_info[0].type == FactorTypeEnum.SLIDER:
                    df = df[~df[factor].isna()]

                if filter["above"] is not None:
                    df = df[df[factor] >= filter["above"]]
                if filter["below"] is not None:
                    df = df[df[factor] <= filter["below"]]
                if filter["values"] is not None:
                    if len(filter["values"]) > 0:
                        # OR
                        value_conditions = pd.Series(False, index=df.index)
                        for value in filter["values"]:
                            value_conditions = value_conditions | (df[factor] == value)
                        df = df[value_conditions]

        stock_codes = df["Code"].tolist()
        return stock_codes

    def filter_etfs(
        self,
        market_filter: Optional[ETFMarketEnum] = None,
        custom_filters: Optional[List[Dict]] = None,
    ) -> List[str]:
        df = self.etf_factor_loader.load_etf_factors(market_filter)
        print(f"Initial DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {df.columns.tolist()}")

        # 종목 필터링
        if market_filter:
            if market_filter == ETFMarketEnum.US:
                df = df[df["country"] == "us"]
            elif market_filter == ETFMarketEnum.KR:
                df = df[df["country"] == "kr"]
            elif market_filter in [ETFMarketEnum.NASDAQ, ETFMarketEnum.NYSE, ETFMarketEnum.BATS]:
                df = df[df["market"] == market_filter.value.upper()]
            print(f"After market filter shape: {df.shape}")

        if custom_filters:
            for filter in custom_filters:
                factor = filter["factor"]
                if factor == "배당 주기":
                    factor = "dividend_frequency"
                if factor not in df.columns:
                    raise ValueError(f"팩터 '{factor}'가 데이터에 존재하지 않습니다.")

                print(f"\nProcessing filter for {factor}:")
                print(f"Unique values before filtering: {df[factor].value_counts().to_dict()}")
                print(f"Filter values: {filter.get('values')}")

                if filter.get("above") is not None:
                    df = df[df[factor] >= filter["above"]]
                    print(f"After 'above' filter shape: {df.shape}")
                if filter.get("below") is not None:
                    df = df[df[factor] <= filter["below"]]
                    print(f"After 'below' filter shape: {df.shape}")
                if filter.get("values") is not None and len(filter.get("values", [])) > 0:
                    print(f"Filtering for values: {filter['values']}")
                    df = df[df[factor].isin(filter["values"])]
                    print(f"After value filter shape: {df.shape}")
                    print(f"Remaining unique values: {df[factor].value_counts().to_dict()}")

        etf_tickers = df["Code"].tolist()
        print(f"Final number of tickers: {len(etf_tickers)}")
        return etf_tickers

    @time_it
    def get_filtered_stocks_df(
        self, market_filter: MarketEnum, codes: List[str], columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        if columns is None:
            columns = []
        required_columns = columns.copy()
        if "sector" in required_columns:
            required_columns.append("sector_en")
        if "Name" in required_columns:
            required_columns.append("Name_en")

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
        existing_columns = df.columns.tolist()
        required_columns = [col for col in required_columns if col in existing_columns]
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
            if np.issubdtype(processed_df[col].dtypes, np.number):
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

    @time_it
    def convert_unit_and_value(
        self, market_filter: MarketEnum, value: float, unit: str, lang: str = "kr"
    ) -> tuple[float, str]:
        nation = "kr" if market_filter in [MarketEnum.KR, MarketEnum.KOSPI, MarketEnum.KOSDAQ] else "us"

        if unit.lower() == "big_price":
            print(f"value: {value}, unit: {unit}, lang: {lang}")
            if nation == "kr":
                print("NATION: KR")
                if value >= 10000 or value <= -10000:  # 1조원 이상
                    return round(value / 10000, 2), "조원"
                return int(value), "억원"
            else:  # US
                print("NATION: US")
                # 1T = 1000B = 1000조원
                if value >= 1000000000 or value <= -1000000000:  # 1000조원 이상
                    return round(value / 1000000000, 2), "T$"
                # 1B = 1조원
                elif value >= 1000000 or value <= -1000000:  # 1조원 이상
                    return round(value / 1000000, 2), "B$"
                # 1M = 10억원
                elif value >= 1000 or value <= -1000:  # 10억원 이상
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

    def convert_krw_billion_to_usd(self, value: float, exchange_rate: float = 1400) -> float:
        if exchange_rate <= 0:
            raise ValueError("환율은 0보다 커야 합니다.")

        # 1억원 = 100,000,000원
        krw_value = value * 100000000

        # 원화 -> 달러 변환
        usd_value = krw_value / exchange_rate

        # 달러 -> 천달러 단위로 변환
        usd_value_in_ten_million = usd_value / 1000

        return usd_value_in_ten_million

    def classify_factors_preset(self, presets):
        """
        FactorsPreset 리스트를 받아서 각 항목의 값을 타입별로 분류하여 리턴하는 함수

        Args:
            presets: FactorsPreset 객체 리스트 또는 단일 객체

        Returns:
            list: 각 항목의 타입과 값을 포함하는 딕셔너리 리스트
                또는 단일 객체가 입력된 경우 딕셔너리
        """
        result = []
        for preset in presets:
            classified_preset = {
                "display": preset.display,
                "value": preset.value,
                "above": preset.above,
                "below": preset.below,
            }
            result.append(classified_preset)

        return result

    def add_exclude_flags_to_dataframe(
        self, df: pd.DataFrame, exclude_filters: Optional[List[ExcludeEnum]] = None
    ) -> pd.DataFrame:
        """
        데이터프레임에 ExcludeEnum에 해당하는 제외 플래그 추가
        """
        if exclude_filters is None:
            return df

        if ExcludeEnum.FINANCIAL in exclude_filters:
            # 금융주 여부 (sector 정보 기반)
            financial_sectors = ["금융서비스", "보험", "은행", "부동산리츠"]
            df["is_financial"] = df["sector"].isin(financial_sectors)

        if ExcludeEnum.HOLDING in exclude_filters:
            # 지주사 여부 (회사명 기반)
            df["is_holding"] = df["Name"].str.contains("홀딩스|지주|Holdings", case=False, na=False)

        if ExcludeEnum.WARNED in exclude_filters:
            # 관리종목 여부
            df["is_warned"] = df["is_warned"] == 1 if "is_warned" in df.columns else False

        if ExcludeEnum.DEFICIT in exclude_filters:
            # 적자기업 여부 (분기)
            df["is_deficit"] = df["net_income_1q"] < 0 if "net_income_1q" in df.columns else False

        if ExcludeEnum.ANNUAL_DEFICIT in exclude_filters:
            # 적자기업 여부 (연간)
            df["is_annual_deficit"] = df["net_income_ttm"] < 0 if "net_income_ttm" in df.columns else False

        # if ExcludeEnum.CHINA in exclude_filters:
        #     # 중국기업 여부
        #     df['is_chinese'] = (df['country'] == 'china') | (df['상장된 시장의 국가'] == 'China') if '상장된 시장의 국가' in df.columns else False

        # PTP 기업 여부 (Penny Stock, 보통 $5 미만 주식)
        is_us_market = df["country"] == "us"
        is_kr_market = df["country"] == "kr"

        # 미국 주식은 $5 미만, 한국 주식은 1000원 미만을 PTP로 간주
        df["is_ptp"] = (is_us_market & (df["close"] < 5)) | (is_kr_market & (df["close"] < 1000))

        return df


screener_utils = ScreenerUtils()
