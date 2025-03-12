import datetime
from io import BytesIO
import os
from typing import List, Literal
from fastapi import HTTPException
import numpy as np
import pandas as pd
from Aws.logic.s3 import get_data_from_bucket
from app.common.constants import (
    ETF_DEFAULT_SCREENER_COLUMNS,
    FACTOR_MAP,
    FACTOR_MAP_EN,
    NON_NUMERIC_COLUMNS_ETF,
    PARQUET_DIR,
    REVERSE_FACTOR_MAP,
    REVERSE_FACTOR_MAP_EN,
    UNIT_MAP,
)
from app.core.exception.base import CustomException
from app.core.logging.config import get_logger
from app.modules.screener_etf.enum import ETFMarketEnum
from app.modules.screener.schemas import GroupFilter
from app.modules.screener.service import ScreenerService
from app.modules.screener_etf.schemas import FilteredETF
from app.utils.date_utils import get_business_days
from app.utils.score_utils import etf_score_utils
from app.utils.factor_utils import factor_utils
from app.cache.factors import etf_factors_cache

logger = get_logger(__name__)


class ScreenerETFService(ScreenerService):
    def __init__(self):
        super().__init__()

    def get_etf_factors(self, market: ETFMarketEnum):
        try:
            factors = self.factor_utils.get_etf_factors(market)

            if market in [ETFMarketEnum.US, ETFMarketEnum.NASDAQ, ETFMarketEnum.NYSE, ETFMarketEnum.BATS]:
                nation = "us"
            else:
                nation = "kr"

            result = []
            for factor in factors:
                if factor["unit"] == "small_price":
                    unit = "원" if nation == "kr" else "$"
                    type = "input"
                elif factor["unit"] == "big_price":
                    unit = "억원" if nation == "kr" else "K$"
                    type = "input"
                else:
                    unit = UNIT_MAP[factor["unit"]]
                    type = "slider"
                    
                result.append(
                    {
                        "factor": factor["factor"],
                        "description": factor["description"],
                        "unit": unit,
                        "category": factor["category"],
                        "direction": factor["direction"],
                        "min_value": factor["min_value"],
                        "max_value": factor["max_value"],
                        "type": type,
                    }
                )

            return result

        except Exception as e:
            logger.exception(f"Error in get_factors: {e}")
            raise e

    def get_filtered_etfs(self, filtered_etf: FilteredETF):
        non_numeric_columns = [FACTOR_MAP[col] for col in NON_NUMERIC_COLUMNS_ETF]
        if filtered_etf.sort_by not in filtered_etf.columns and filtered_etf.sort_by not in non_numeric_columns:
            raise CustomException(status_code=400, message="sort_by must be in columns")

        etfs = factor_utils.filter_etfs(filtered_etf.market_filter, filtered_etf.custom_filters)
        # 필터링
        filtered_df = factor_utils.get_filtered_etfs_df(filtered_etf.market_filter, etfs, filtered_etf.columns)
        # 점수 계산
        scored_df = etf_score_utils.calculate_factor_score(filtered_df)
        if scored_df.empty:
            return [], 0 
        # 병합
        merged_df = filtered_df.merge(scored_df, on="ticker", how="inner")
        # 정렬
        sorted_df = merged_df.sort_values(by=filtered_etf.sort_by, ascending=filtered_etf.ascending).reset_index(drop=True)
        # 페이징
        total_count = len(sorted_df)
        sorted_df = sorted_df.iloc[filtered_etf.offset * filtered_etf.limit : filtered_etf.offset * filtered_etf.limit + filtered_etf.limit + 1]
        
        factors = etf_factors_cache.get_configs()
        result = []

        if filtered_etf.columns:
            ordered_columns = []
            for col in filtered_etf.columns:
                mapped_col = next((k for k, v in FACTOR_MAP.items() if v == col), col)
                if mapped_col not in ordered_columns:
                    ordered_columns.append(mapped_col)
        else:
            ordered_columns = ["ticker", "kr_name", "manager", "score"]
        
        selected_columns = ordered_columns.copy()

        sorted_df = sorted_df[selected_columns]

        for _, row in sorted_df.iterrows():
            # 기본으로 표시될 컬럼들
            etf_data = {}

            # 숫자형 데이터 처리
            for col in sorted_df.columns:
                if col in NON_NUMERIC_COLUMNS_ETF:
                    if col in row:
                        etf_data[col] = row[col]
                elif col == "score":
                    etf_data[col] = float(row[col])
                elif col in row:
                    if pd.isna(row[col]) or np.isinf(row[col]):
                        etf_data[col] = {"value": "", "unit": ""}
                    else:
                        value, unit = factor_utils.convert_unit_and_value(
                            filtered_etf.market_filter,
                            float(row[col]),
                            factors[col].get("unit", "") if col in factors else "",
                            filtered_etf.lang,
                        )
                        etf_data[col] = {"value": value, "unit": unit}

            result.append(etf_data)

        mapped_result = []
        factor_map = FACTOR_MAP if filtered_etf.lang == "kr" else FACTOR_MAP_EN
        for item in result:
            mapped_item = {}
            for key, value in item.items():
                mapped_key = factor_map.get(key, key)
                mapped_item[mapped_key] = value
            mapped_result.append(mapped_item)

        return mapped_result, total_count


    def _filter_etfs(self, df_etfs: pd.DataFrame, filtered_etf: FilteredETF):
        # 종목 필터 - 기본 필터
        if filtered_etf.market_filter:
            # if filtered_etf.market_filter == ETFMarketEnum.US:
            #     df_etfs = df_etfs[df_etfs["country"] == "us"]
            # if filtered_etf.market_filter in [ETFMarketEnum.KR]:
            #     df_etfs = df_etfs[df_etfs["market"] == "KRX"]
            if filtered_etf.market_filter in [ETFMarketEnum.NYSE, ETFMarketEnum.NASDAQ, ETFMarketEnum.BATS]:
                df_etfs = df_etfs[df_etfs["market"] == filtered_etf.market_filter.value]

        # 종목 필터 - 커스텀 필터링
        custom_filters = []
        if filtered_etf.custom_filters:
            custom_filters = [
                {
                    "factor": REVERSE_FACTOR_MAP[condition.factor],
                    "above": condition.above,
                    "below": condition.below,
                }
                for condition in filtered_etf.custom_filters
            ]
            for filter in custom_filters:
                if filter["factor"] not in df_etfs.columns:
                    raise ValueError(f"팩터 '{filter['factor']}'가 데이터에 존재하지 않습니다.")

                if filter["above"] is not None:
                    df_etfs = df_etfs[df_etfs[filter["factor"]] >= filter["above"]]

                if filter["below"] is not None:
                    df_etfs = df_etfs[df_etfs[filter["factor"]] <= filter["below"]]

        # 컬럼 필터
        required_columns = [col for col in ETF_DEFAULT_SCREENER_COLUMNS if col in df_etfs.columns]

        if filtered_etf.columns is not None:
            reversed_factors = [REVERSE_FACTOR_MAP[col] for col in filtered_etf.columns]
            reversed_factors = [col for col in reversed_factors if col in df_etfs.columns]
            required_columns = required_columns + [col for col in reversed_factors if col not in required_columns]

        df_etfs = df_etfs[required_columns]

        return df_etfs

    def get_filtered_etfs_count(self, filtered_etf: FilteredETF):
        df_etfs = self.factor_loader.load_etf_factors(filtered_etf.market_filter.value)

        df_etfs = self._filter_etfs(df_etfs, filtered_etf)

        return df_etfs.shape[0]


    def update_parquet(self, ctry: Literal["KR", "US"]):
        today = datetime.datetime.now().date()
        start_date = today - datetime.timedelta(days=7)

        if ctry == "KR":
            country = "kr"
        elif ctry == "US":
            country = "us"
        else:
            raise ValueError("Invalid country")

        business_days = get_business_days(country=ctry, start_date=start_date, end_date=today)
        str_business_days = [business_day.strftime("%Y%m%d") for business_day in business_days]
        str_business_days.sort(reverse=True)

        for business_day in str_business_days:
            try:
                data = get_data_from_bucket(
                    bucket="alpha-finder-factors",
                    dir=f"etf/{country}",
                    key=f"{country}_etf_factors_{business_day}.parquet",
                )
                df = pd.read_parquet(BytesIO(data), engine="pyarrow")
                file_name = f"{country}_etf_factors.parquet"
                df.to_parquet(os.path.join(PARQUET_DIR, file_name), index=False)
                print(df.head())
                return True
            except Exception:
                continue
        return False
