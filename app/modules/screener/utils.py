import pandas as pd
from app.database.crud import database
from typing import Dict, List, Optional
from Aws.logic.s3 import get_data_from_bucket
import io
from app.modules.screener.stock.schemas import MarketEnum
from app.common.constants import NEED_TO_MULTIPLY_100, MARKET_MAP, UNIT_MAP, UNIT_MAP_EN
import numpy as np
from collections import defaultdict
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
from app.utils.test_utils import time_it
from app.kispy.manager import KISAPIManager

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
                        "factor": factor_name,
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
                        "factor": factor_name,
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

        technical_columns = ["beta", "rsi_14", "sharpe", "momentum_6", "vol"]
        if type == StockType.ETF:
            technical_columns = ["median_trade", "rsi_14", "sharpe", "momentum_6", "vol"]

        additional_columns = {
            CategoryEnum.TECHNICAL: technical_columns,
            CategoryEnum.FUNDAMENTAL: ["roe", "fscore", "deptRatio", "operating_income", "z_score"],
            CategoryEnum.VALUATION: ["pbr", "pcr", "per", "por", "psr"],
            CategoryEnum.DIVIDEND: [
                "recent_dividend_yield",
                "dividend_count",
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

        print(f"한국 factor 데이터 로드 - 행: {df.shape[0]}, 열: {df.shape[1]}")

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

        df = pd.merge(df, stock_info_df, on="merge_code", how="inner")
        df["Name"] = df["kr_name"]
        df["Name_en"] = df["en_name"]
        df = df.drop(["merge_code", "kr_name", "en_name"], axis=1)

        df["country"] = "kr"

        df["is_activate"] = df["is_activate"].fillna(1).astype(int)
        df["is_delisted"] = df["is_delisted"].fillna(0).astype(int)

        df = df[(df["is_activate"] == 1) & (df["is_delisted"] == 0)]

        unique_tickers = df["Code"].unique().tolist()

        dividend_data = self._get_dividend_data_for_tickers(unique_tickers)

        df["ttm_dividend_yield"] = np.nan
        df["consecutive_dividend_growth"] = np.nan

        matched_yield_count = 0
        matched_growth_count = 0

        for index, row in df.iterrows():
            ticker = row["Code"]

            if ticker in dividend_data["ttm_yield"]:
                df.at[index, "ttm_dividend_yield"] = round(dividend_data["ttm_yield"][ticker], 2)
                matched_yield_count += 1

            if ticker in dividend_data["consecutive_growth"]:
                df.at[index, "consecutive_dividend_growth"] = dividend_data["consecutive_growth"][ticker]
                matched_growth_count += 1

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

        # 모든 숫자 컬럼을 float64로 변환하기 전에 ttm_dividend_yield 열을 소수점 둘째자리로 반올림
        if "ttm_dividend_yield" in df_result.columns:
            df_result["ttm_dividend_yield"] = df_result["ttm_dividend_yield"].apply(
                lambda x: round(x, 2) if pd.notnull(x) else x
            )

        for column in df_result.columns:
            if np.issubdtype(df_result[column].dtype, np.number):
                df_result[column] = df_result[column].astype(np.float64)

        df_result["consecutive_dividend_growth"] = df_result["consecutive_dividend_growth"].fillna(0).astype(np.int32)

        # 최종 저장 전 ttm_dividend_yield 확인
        if "ttm_dividend_yield" in df_result.columns:
            print("\nttm_dividend_yield 소수점 확인:")
            non_null_yields = df_result["ttm_dividend_yield"].dropna().head(10)
            print(non_null_yields)

        df_result.to_parquet(output_file)
        print(f"배당 정보가 포함된 한국 주식 factor 데이터가 {output_file}에 저장되었습니다.")

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

        df["ttm_dividend_yield"] = np.nan
        df["consecutive_dividend_growth"] = np.nan

        matched_yield_count = 0
        matched_growth_count = 0

        for index, row in df.iterrows():
            ticker = row["Code"]

            if ticker in dividend_data["ttm_yield"]:
                df.at[index, "ttm_dividend_yield"] = round(dividend_data["ttm_yield"][ticker], 2)
                matched_yield_count += 1

            if ticker in dividend_data["consecutive_growth"]:
                df.at[index, "consecutive_dividend_growth"] = dividend_data["consecutive_growth"][ticker]
                matched_growth_count += 1

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

        if "ttm_dividend_yield" in df_result.columns:
            df_result["ttm_dividend_yield"] = df_result["ttm_dividend_yield"].apply(
                lambda x: round(x, 2) if pd.notnull(x) else x
            )

        for column in df_result.columns:
            if np.issubdtype(df_result[column].dtype, np.number):
                df_result[column] = df_result[column].astype(np.float64)

        df_result["consecutive_dividend_growth"] = df_result["consecutive_dividend_growth"].fillna(0).astype(np.int32)

        df_result.to_parquet(output_file)

    def _get_dividend_data_for_tickers(self, tickers):
        if not tickers:
            return {"ttm_yield": {}, "consecutive_growth": {}}

        one_year_ago = datetime.now() - timedelta(days=365)

        batch_size = 500
        ttm_dividends_all = []

        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i : i + batch_size]

            aggregates = {"total_dividend": ("per_share", "sum")}

            batch_ttm_dividends = self.db._select(
                table="dividend_information",
                columns=["ticker"],
                aggregates=aggregates,
                group_by=["ticker"],
                ex_date__gte=one_year_ago.strftime("%Y-%m-%d"),
                ticker__in=batch_tickers,
            )

            ttm_dividends_all.extend(batch_ttm_dividends)

        price_data_all = []

        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i : i + batch_size]

            batch_price_data = self.db._select(
                table="stock_trend", columns=["ticker", "prev_close"], ticker__in=batch_tickers
            )

            price_data_all.extend(batch_price_data)

        price_dict = {row[0]: row[1] for row in price_data_all}

        ttm_yield_dict = {}

        for dividend_row in ttm_dividends_all:
            ticker = dividend_row[0]
            ttm_dividend = dividend_row[1]

            if ticker in price_dict and price_dict[ticker] > 0:
                yield_percentage = (ttm_dividend / price_dict[ticker]) * 100
                ttm_yield_dict[ticker] = round(yield_percentage, 2)

        dividend_data_all = []

        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i : i + batch_size]

            batch_dividend_data = self.db._select(
                table="dividend_information", columns=["ticker", "ex_date", "per_share"], ticker__in=batch_tickers
            )

            dividend_data_all.extend(batch_dividend_data)

        yearly_dividends = defaultdict(lambda: defaultdict(float))

        for record in dividend_data_all:
            ticker = record[0]
            ex_date = record[1]
            amount = record[2]

            if amount is not None:
                year = ex_date.year
                yearly_dividends[ticker][year] += amount

        growth_dict = {}

        for ticker, yearly_data in yearly_dividends.items():
            sorted_years = sorted(yearly_data.keys(), reverse=True)

            if len(sorted_years) < 2:
                growth_dict[ticker] = 0
                continue

            consecutive_count = 0

            for i in range(len(sorted_years) - 1):
                current_year = sorted_years[i]
                prev_year = sorted_years[i + 1]

                if yearly_data[current_year] > yearly_data[prev_year]:
                    consecutive_count += 1
                else:
                    break

            growth_dict[ticker] = consecutive_count

        return {"ttm_yield": ttm_yield_dict, "consecutive_growth": growth_dict}

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

        if "ttm_dividend_yield" in df.columns:
            non_na_yield = df["ttm_dividend_yield"].notna().sum()
            print(f"글로벌 데이터에서 배당 수익률이 있는 종목 수: {non_na_yield}")

        df.to_parquet("parquet/global_stock_factors.parquet")
        print("글로벌 factor 데이터가 업데이트되었습니다.")

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

        # 문자열 형태의 숫자를 실수형으로 변환
        if "total_fee" in kr_df.columns:
            kr_df["total_fee"] = pd.to_numeric(kr_df["total_fee"], errors="coerce")

        if "total_fee" in us_df.columns:
            us_df["total_fee"] = pd.to_numeric(us_df["total_fee"], errors="coerce")

        # 모든 숫자형 컬럼을 float64로 통일
        for column in kr_df.columns:
            if np.issubdtype(kr_df[column].dtype, np.number):
                kr_df[column] = kr_df[column].astype(np.float64)
        for column in us_df.columns:
            if np.issubdtype(us_df[column].dtype, np.number):
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

    def filter_stocks(
        self,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
    ) -> List[str]:
        df = self.get_df_from_parquet(market_filter)

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
                if filter["above"] is not None:
                    df = df[df[factor] >= filter["above"]]
                if filter["below"] is not None:
                    df = df[df[factor] <= filter["below"]]

        stock_codes = df["Code"].tolist()
        return stock_codes

    def filter_etfs(
        self,
        market_filter: Optional[ETFMarketEnum] = None,
        custom_filters: Optional[List[Dict]] = None,
    ) -> List[str]:
        df = self.etf_factor_loader.load_etf_factors(market_filter)

        # 종목 필터링
        if market_filter:
            if market_filter == ETFMarketEnum.US:
                df = df[df["country"] == "us"]
            elif market_filter == ETFMarketEnum.KR:
                df = df[df["country"] == "kr"]
            elif market_filter in [ETFMarketEnum.NASDAQ, ETFMarketEnum.NYSE, ETFMarketEnum.BATS]:
                df = df[df["market"] == market_filter.value.upper()]

        if custom_filters:
            for filter in custom_filters:
                factor = filter["factor"]
                if factor not in df.columns:
                    raise ValueError(f"팩터 '{factor}'가 데이터에 존재하지 않습니다.")
                if filter["above"] is not None:
                    df = df[df[factor] >= filter["above"]]
                if filter["below"] is not None:
                    df = df[df[factor] <= filter["below"]]

        etf_tickers = df["Code"].tolist()
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


screener_utils = ScreenerUtils()


def test_dividend_calculation():
    """배당 계산 기능을 테스트하는 함수"""
    print("==== 배당 계산 테스트 시작 ====")

    # 1. 테이블에 실제로 데이터가 있는지 확인
    try:
        count = database._count("dividend_information")
        print(f"dividend_information 테이블 레코드 수: {count}")

        if count == 0:
            print("경고: dividend_information 테이블에 데이터가 없습니다.")
            return
    except Exception as e:
        print(f"dividend_information 테이블 접근 오류: {e}")
        return

    # 2. 샘플 티커 목록 가져오기 (테스트 용)
    try:
        sample_tickers = database._select(table="stock_information", columns=["ticker"], limit=10)

        tickers = [row[0] for row in sample_tickers]
        print("테스트 티커 목록:", tickers)

        # 3. 티커별 배당 정보 조회 테스트
        print("\n-- 티커별 배당 정보 조회 테스트 --")
        test_dividend_lookup(tickers)

        # 4. 티커별 가격 정보 조회 테스트
        print("\n-- 티커별 가격 정보 조회 테스트 --")
        test_price_lookup(tickers)

        # 5. 배당 수익률 계산 테스트
        print("\n-- 배당 수익률 계산 테스트 --")
        test_dividend_yield_calculation(tickers[0])

        # 6. 연속 배당 성장 계산 테스트
        print("\n-- 연속 배당 성장 계산 테스트 --")
        test_consecutive_growth_calculation(tickers[0])

    except Exception as e:
        print(f"테스트 중 오류 발생: {e}")

    print("==== 배당 계산 테스트 완료 ====")


def test_dividend_lookup(tickers):
    """티커별 배당 정보 조회 테스트"""
    for ticker in tickers[:3]:  # 처음 3개 티커만 테스트
        try:
            # 최근 배당 정보 조회
            recent_dividend = database._select(
                table="dividend_information",
                columns=["ex_date", "payment_date", "per_share"],
                ticker=ticker,
                order="ex_date",
                ascending=False,
                limit=3,
            )

            if recent_dividend:
                print(f"\n{ticker} 최근 배당 정보:")
                for i, record in enumerate(recent_dividend):
                    print(f"  {i+1}. 배당락일: {record[0]}, 지급일: {record[1]}, 주당배당금: {record[2]}")
            else:
                print(f"  {ticker} 티커에 대한 배당 정보가 없습니다.")
        except Exception as e:
            print(f"  {ticker} 티커 배당 조회 중 오류: {e}")


def test_price_lookup(tickers):
    """티커별 가격 정보 조회 테스트"""
    for ticker in tickers[:3]:  # 처음 3개 티커만 테스트
        try:
            # 가격 정보 조회
            price_info = database._select(
                table="stock_trend", columns=["prev_close", "updated_at"], ticker=ticker, limit=1
            )

            if price_info:
                print(f"{ticker} 가격 정보: 전일종가={price_info[0][0]}, 업데이트={price_info[0][2]}")
            else:
                print(f"{ticker} 티커에 대한 가격 정보가 없습니다.")
        except Exception as e:
            print(f"{ticker} 티커 가격 조회 중 오류: {e}")


def test_dividend_yield_calculation(ticker):
    """배당 수익률 계산 테스트"""
    try:
        # 1년전 날짜 계산
        from datetime import datetime, timedelta

        one_year_ago = datetime.now() - timedelta(days=365)
        one_year_ago_str = one_year_ago.strftime("%Y-%m-%d")

        # 최근 12개월 배당 합계 조회
        aggregates = {"total_dividend": ("per_share", "sum")}
        ttm_dividend = database._select(
            table="dividend_information",
            columns=["ticker"],
            aggregates=aggregates,
            ticker=ticker,
            ex_date__gte=one_year_ago_str,
        )

        if ttm_dividend and ttm_dividend[0][1] is not None:
            total_dividend = ttm_dividend[0][1]
            print(f"{ticker} 최근 12개월 배당 합계: {total_dividend}")

            # 현재 가격 조회
            price_info = database._select(table="stock_trend", columns=["prev_close"], ticker=ticker, limit=1)

            if price_info:
                current_price = price_info[0][0]
                print(f"{ticker} 현재 가격: {current_price}")

                # 배당 수익률 계산
                if current_price > 0:
                    yield_percentage = (total_dividend / current_price) * 100
                    print(f"{ticker} 배당 수익률: {yield_percentage:.2f}%")
                else:
                    print(f"{ticker} 가격이 0 또는 음수입니다.")
            else:
                print(f"{ticker} 가격 정보가 없습니다.")
        else:
            print(f"{ticker} 최근 12개월 배당 정보가 없습니다.")

    except Exception as e:
        print(f"배당 수익률 계산 중 오류: {e}")


def test_consecutive_growth_calculation(ticker):
    """연속 배당 성장 계산 테스트"""
    try:
        # 모든 배당 데이터 조회
        dividend_data = database._select(table="dividend_information", columns=["ex_date", "per_share"], ticker=ticker)

        if not dividend_data:
            print(f"{ticker} 배당 데이터가 없습니다.")
            return

        # 연도별 배당금 합계 계산
        yearly_dividends = {}
        for record in dividend_data:
            ex_date = record[0]
            amount = record[1]
            year = ex_date.year

            if year not in yearly_dividends:
                yearly_dividends[year] = 0
            yearly_dividends[year] += amount

        # 결과 출력
        print(f"{ticker} 연도별 배당금:")
        for year, amount in sorted(yearly_dividends.items(), reverse=True):
            print(f"  {year}: {amount}")

        # 연속 성장 계산
        sorted_years = sorted(yearly_dividends.keys(), reverse=True)
        if len(sorted_years) < 2:
            print(f"{ticker} 연속 배당 성장 계산을 위한 충분한 데이터가 없습니다.")
            return

        consecutive_count = 0
        for i in range(len(sorted_years) - 1):
            current_year = sorted_years[i]
            prev_year = sorted_years[i + 1]

            if yearly_dividends[current_year] > yearly_dividends[prev_year]:
                consecutive_count += 1
                print(
                    f"  {current_year}년({yearly_dividends[current_year]}) > {prev_year}년({yearly_dividends[prev_year]})"
                )
            else:
                print(
                    f"  {current_year}년({yearly_dividends[current_year]}) <= {prev_year}년({yearly_dividends[prev_year]}) - 성장 중단"
                )
                break

        print(f"{ticker} 연속 배당 성장 횟수: {consecutive_count}년")

    except Exception as e:
        print(f"연속 배당 성장 계산 중 오류: {e}")


def test_factor_processing():
    """factor 처리 전체 과정 테스트"""
    print("\n==== Factor 처리 테스트 시작 ====")

    try:
        # 1. 한국 주식 처리
        print("\n-- 한국 주식 처리 테스트 --")
        screener_utils.process_kr_factor_data()

        # 2. 결과 확인
        kr_df = screener_utils.get_df_from_parquet(MarketEnum.KR)
        print(f"한국 주식 데이터프레임 크기: {kr_df.shape}")
        print("배당 수익률 통계:")
        print(kr_df["ttm_dividend_yield"].describe())

        # 배당 수익률이 있는 종목 확인
        has_dividend = kr_df[kr_df["ttm_dividend_yield"].notna()]
        print(f"배당 수익률이 있는 종목 수: {len(has_dividend)}")

        if len(has_dividend) > 0:
            print("\n배당 수익률 상위 5개 종목:")
            top_dividend = has_dividend.sort_values("ttm_dividend_yield", ascending=False).head(5)
            print(top_dividend[["Code", "Name", "ttm_dividend_yield", "consecutive_dividend_growth"]])

        # 3. 미국 주식 처리는 선택적으로 진행
        process_us = input("\n미국 주식도 처리하시겠습니까? (y/n): ").strip().lower() == "y"

        if process_us:
            print("\n-- 미국 주식 처리 테스트 --")
            screener_utils.process_us_factor_data()

            # 4. 결과 확인
            us_df = screener_utils.get_df_from_parquet(MarketEnum.US)
            print(f"미국 주식 데이터프레임 크기: {us_df.shape}")
            print("배당 수익률 통계:")
            print(us_df["ttm_dividend_yield"].describe())

            # 배당 수익률이 있는 종목 확인
            has_dividend = us_df[us_df["ttm_dividend_yield"].notna()]
            print(f"배당 수익률이 있는 종목 수: {len(has_dividend)}")

            if len(has_dividend) > 0:
                print("\n배당 수익률 상위 5개 종목:")
                top_dividend = has_dividend.sort_values("ttm_dividend_yield", ascending=False).head(5)
                print(top_dividend[["Code", "Name", "ttm_dividend_yield", "consecutive_dividend_growth"]])

        # 5. 글로벌 데이터 처리
        process_global = input("\n글로벌 데이터도 처리하시겠습니까? (y/n): ").strip().lower() == "y"

        if process_global:
            print("\n-- 글로벌 데이터 처리 테스트 --")
            screener_utils.process_global_factor_data()

            # 결과 확인
            global_df = pd.read_parquet("parquet/global_stock_factors.parquet")
            print(f"글로벌 데이터프레임 크기: {global_df.shape}")
            print("배당 수익률 통계:")
            print(global_df["ttm_dividend_yield"].describe())

    except Exception as e:
        print(f"Factor 처리 테스트 중 오류 발생: {e}")


def run_specific_tests():
    """특정 테스트만 선택적으로 실행"""
    print("실행할 테스트를 선택하세요:")
    print("1. 배당 계산 테스트")
    print("2. Factor 처리 테스트 (한국 주식)")
    print("3. Factor 처리 테스트 (미국 주식)")
    print("4. Factor 처리 테스트 (글로벌)")
    print("5. 모든 테스트 실행")

    choice = input("선택 (1-5): ").strip()

    if choice == "1":
        test_dividend_calculation()
    elif choice == "2":
        screener_utils.process_kr_factor_data()
        kr_df = screener_utils.get_df_from_parquet(MarketEnum.KR)
        print_dividend_stats(kr_df, "한국")
    elif choice == "3":
        screener_utils.process_us_factor_data()
        us_df = screener_utils.get_df_from_parquet(MarketEnum.US)
        print_dividend_stats(us_df, "미국")
    elif choice == "4":
        screener_utils.process_global_factor_data()
        global_df = pd.read_parquet("parquet/global_stock_factors.parquet")
        print_dividend_stats(global_df, "글로벌")
    elif choice == "5":
        test_dividend_calculation()
        test_factor_processing()
    else:
        print("잘못된 선택입니다.")


def print_dividend_stats(df, market_name):
    """데이터프레임의 배당 통계 출력"""
    print(f"\n{market_name} 주식 데이터프레임 크기: {df.shape}")

    if "ttm_dividend_yield" in df.columns:
        print("배당 수익률 통계:")
        print(df["ttm_dividend_yield"].describe())

        non_na_count = df["ttm_dividend_yield"].notna().sum()
        print(f"배당 수익률이 있는 종목 수: {non_na_count}")

        if non_na_count > 0:
            print("\n배당 수익률 상위 5개 종목:")
            top_dividend = df[df["ttm_dividend_yield"].notna()].sort_values("ttm_dividend_yield", ascending=False).head(5)
            print(top_dividend[["Code", "Name", "ttm_dividend_yield", "consecutive_dividend_growth"]])
    else:
        print("배당 수익률 컬럼이 존재하지 않습니다.")


# 메인 실행 부분
if __name__ == "__main__":
    try:
        # 테스트 메뉴 실행
        run_specific_tests()
    except Exception as e:
        print(f"테스트 실행 중 오류 발생: {e}")
