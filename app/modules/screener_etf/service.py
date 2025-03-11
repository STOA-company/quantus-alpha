import datetime
from io import BytesIO
import os
from typing import List, Literal, Optional
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
from app.modules.screener_etf.enum import ETFCategoryEnum, ETFMarketEnum
from app.modules.screener.schemas import FactorResponse, GroupFilter
from app.modules.screener.service import ScreenerService
from app.modules.screener_etf.schemas import FilteredETF
from app.utils.data_utils import ceil_to_integer, floor_to_integer
from app.utils.date_utils import get_business_days
from app.utils.etf_utils import ETFDataLoader
from app.utils.factor_utils import FactorUtils
from app.utils.score_utils import etf_score_utils
from app.cache.factors import etf_factors_cache
from app.database.crud import database

logger = get_logger(__name__)


class ScreenerETFService(ScreenerService):
    def __init__(self):
        super().__init__()
        self.factor_utils = FactorUtils()
        self.factor_loader = ETFDataLoader()
        self.factors_cache = etf_factors_cache
        self.db = database

    def get_etf_factors(self, market: ETFMarketEnum):
        factors = self.db._select(table="factors", is_etf=True)
        market_data = self.factor_loader.load_etf_factors(market.value)

        # 시장별 데이터 필터링
        if market in [ETFMarketEnum.NYSE, ETFMarketEnum.NASDAQ, ETFMarketEnum.BATS]:
            filtered_market_data = market_data[market_data["market"] == market.value]
        else:
            filtered_market_data = market_data

        # 국가 설정
        if market in [ETFMarketEnum.US, ETFMarketEnum.NYSE, ETFMarketEnum.NASDAQ, ETFMarketEnum.BATS]:
            nation = "us"
        else:
            nation = "kr"

        result = []
        for factor in factors:
            if factor.unit == "SMALL_PRICE":
                unit = "원" if nation == "kr" else "$"
                type = "input"
            elif factor.unit == "BIG_PRICE":
                unit = "억원" if nation == "kr" else "K$"
                type = "input"
            else:
                unit = UNIT_MAP[factor.unit.lower()]
                type = "slider"
            factor_name = factor.factor

            if factor_name not in filtered_market_data.columns:
                raise ValueError(f"팩터 '{factor_name}'가 데이터에 존재하지 않습니다.")

            # 데이터 타입 확인 및 숫자 데이터만 필터링
            numeric_data = pd.to_numeric(filtered_market_data[factor_name], errors="coerce")

            # NaN 값 제외하고 최소/최대값 계산
            min_value = numeric_data.min()
            max_value = numeric_data.max()

            # 단위 결정
            if factor.unit == "SMALL_PRICE":
                unit = "원" if nation == "kr" else "$"
            elif factor.unit == "BIG_PRICE":
                unit = "억원" if nation == "kr" else "K$"
            else:
                unit = UNIT_MAP[factor.unit.lower()]

            result.append(
                FactorResponse(
                    factor=FACTOR_MAP[factor.factor],
                    description=factor.description,
                    unit=unit,
                    category=factor.category,
                    direction=factor.sort_direction,
                    min_value=floor_to_integer(min_value),
                    max_value=ceil_to_integer(max_value),
                    type=type,
                )
            )

        return result

    def get_filtered_etfs(self, filtered_etf: FilteredETF):
        non_numaric_columns = [FACTOR_MAP[col] for col in NON_NUMERIC_COLUMNS_ETF]
        reverse_factor_map = REVERSE_FACTOR_MAP if filtered_etf.lang == "kr" else REVERSE_FACTOR_MAP_EN
        reverse_sort_by = reverse_factor_map[filtered_etf.sort_by] if filtered_etf.sort_by is not None else "score"
        if filtered_etf.sort_by is None:
            filtered_etf.sort_by = "score"
        else:
            if filtered_etf.sort_by not in filtered_etf.columns and filtered_etf.sort_by not in non_numaric_columns:
                raise CustomException(status_code=400, message="sort_by must be in columns")

        if filtered_etf.market_filter == ETFMarketEnum.KR:
            ctry = "kr"
        else:
            ctry = "us"

        # 데이터 로드
        df_etfs = self.factor_loader.load_etf_factors(filtered_etf.market_filter.value)

        # 필터링
        df_etfs = self._filter_etfs(df_etfs=df_etfs, filtered_etf=filtered_etf)
        # 점수 계산
        df_scored = etf_score_utils.calculate_factor_score(df_etfs)
        # 병합
        df_etfs = df_etfs.merge(df_scored, on="ticker", how="inner")
        # 정렬
        df_etfs = df_etfs.sort_values(reverse_sort_by, ascending=filtered_etf.ascending).reset_index(drop=True)
        # 페이징
        df_etfs = df_etfs.iloc[
            filtered_etf.offset * filtered_etf.limit : filtered_etf.offset * filtered_etf.limit + filtered_etf.limit + 1
        ]
        need_count = filtered_etf.limit

        if len(df_etfs) <= need_count:
            has_next = False
        else:
            df_etfs = df_etfs.iloc[:-1]
            has_next = True
        factors = self.factors_cache.get_configs()

        # 컬럼 정렬
        if filtered_etf.columns:
            df_etfs = self.sort_columns(df=df_etfs, columns=filtered_etf.columns, lang=filtered_etf.lang, ctry=ctry)

        result = []
        for _, row in df_etfs.iterrows():
            # 기본으로 표시될 컬럼들
            etf_data = {}

            # 숫자형 데이터 처리
            for col in df_etfs.columns:
                if col in NON_NUMERIC_COLUMNS_ETF:
                    if col in row:
                        etf_data[col] = row[col]
                elif col == "score":
                    etf_data[col] = float(row[col])
                elif col in row:
                    if pd.isna(row[col]) or np.isinf(row[col]):
                        etf_data[col] = {"value": "", "unit": ""}
                    else:
                        value, unit = self.factor_utils.convert_unit_and_value(
                            filtered_etf.market_filter,
                            float(row[col]),
                            factors[col].get("unit", "") if col in factors else "",
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

        return mapped_result, has_next

    def sort_columns(self, df: pd.DataFrame, columns: List[str], lang: str, ctry: str):
        if ctry == "kr":
            default_columns = ["ticker", "kr_name", "manager", "score"]
        else:
            default_columns = ["ticker", "en_name", "manager", "score"]

        request_columns = default_columns.copy()
        reverse_factor_map = REVERSE_FACTOR_MAP if lang == "kr" else REVERSE_FACTOR_MAP_EN

        for column in columns:
            internal_column = reverse_factor_map.get(column)
            if internal_column and internal_column not in request_columns:
                request_columns.append(internal_column)

        ordered_columns = []
        for col in request_columns:
            if col in df.columns:
                ordered_columns.append(col)

        df = df[ordered_columns]

        return df

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

    def create_or_update_group(self, current_user: str, group_filter: GroupFilter, type: str = "STOCK"):
        if current_user.id is None:
            raise HTTPException(status_code=401, detail="Unauthorized")

        if group_filter.id and group_filter.id != 0:
            is_success = self.update_group(
                group_id=group_filter.id,
                name=group_filter.name,
                market_filter=group_filter.market_filter,
                sector_filter=group_filter.sector_filter,
                custom_filters=group_filter.custom_filters,
                factor_filters=group_filter.factor_filters,
            )
            message = "Filter updated successfully"
        else:
            group_length = self.get_group_length(current_user.id)
            if group_length >= self.MAX_GROUPS:
                raise HTTPException(status_code=400, detail="Groups is too long")
            is_success = self.create_group(
                user_id=current_user.id,
                name=group_filter.name,
                market_filter=group_filter.market_filter,
                sector_filter=group_filter.sector_filter,
                custom_filters=group_filter.custom_filters,
                factor_filters=group_filter.factor_filters,
                type=group_filter.type or type,
            )
            message = "Group created successfully"

        if is_success:
            return {"message": message}
        else:
            raise HTTPException(status_code=500, detail="Failed to create or update group")

    def get_columns(self, category: ETFCategoryEnum, group_id: Optional[int] = None) -> List[str]:
        if category == ETFCategoryEnum.CUSTOM:
            if not group_id:
                raise ValueError("GroupId is required for custom category")
            group = self.database._select(table="screener_groups", columns=["id"], id=group_id)[0]
            factor_filters = self.database._select(table="screener_factor_filters", columns=["factor"], group_id=group.id)
            factor_filters = [factor_filter.factor for factor_filter in factor_filters]

        else:
            factor_filters = self.factor_utils.get_columns(category)

        columns = [factor_filter for factor_filter in factor_filters]

        result = ETF_DEFAULT_SCREENER_COLUMNS + columns
        return [FACTOR_MAP[column] for column in result]

    def get_filtered_etfs_description(self, filtered_etf: FilteredETF):
        non_numaric_columns = [FACTOR_MAP[col] for col in NON_NUMERIC_COLUMNS_ETF]
        reverse_factor_map = REVERSE_FACTOR_MAP if filtered_etf.lang == "kr" else REVERSE_FACTOR_MAP_EN
        reverse_sort_by = reverse_factor_map[filtered_etf.sort_by] if filtered_etf.sort_by is not None else "score"
        if filtered_etf.sort_by is None:
            filtered_etf.sort_by = "score"
        else:
            if filtered_etf.sort_by not in filtered_etf.columns and filtered_etf.sort_by not in non_numaric_columns:
                raise CustomException(status_code=400, message="sort_by must be in columns")

        if filtered_etf.market_filter == ETFMarketEnum.KR:
            ctry = "kr"
        else:
            ctry = "us"

        # 데이터 로드
        df_etfs = self.factor_loader.load_etf_factors(filtered_etf.market_filter.value)

        # 필터링
        df_etfs = self._filter_etfs(df_etfs=df_etfs, filtered_etf=filtered_etf)

        # 점수 계산
        df_scored = etf_score_utils.calculate_factor_score(df_etfs)

        # 병합
        df_etfs = df_etfs.merge(df_scored, on="ticker", how="inner")

        # 정렬
        df_etfs = df_etfs.sort_values(reverse_sort_by, ascending=filtered_etf.ascending).reset_index(drop=True)

        # 페이징
        df_etfs = df_etfs.iloc[
            filtered_etf.offset * filtered_etf.limit : filtered_etf.offset * filtered_etf.limit + filtered_etf.limit + 1
        ]
        need_count = filtered_etf.limit

        if len(df_etfs) <= need_count:
            has_next = False
        else:
            df_etfs = df_etfs.iloc[:-1]
            has_next = True
        factors = self.factors_cache.get_configs()

        # 컬럼 정렬
        if filtered_etf.columns:
            df_etfs = self.sort_columns(df=df_etfs, columns=filtered_etf.columns, lang=filtered_etf.lang, ctry=ctry)

        if "description" not in df_etfs.columns and "description" in df_etfs.columns:
            df_etfs["description"] = ""

        result = []
        for _, row in df_etfs.iterrows():
            # 기본으로 표시될 컬럼들
            etf_data = {}

            # 숫자형 데이터 처리
            for col in df_etfs.columns:
                if col in NON_NUMERIC_COLUMNS_ETF or col == "description":
                    if col in row:
                        etf_data[col] = row[col]
                elif col == "score":
                    etf_data[col] = {"value": float(row[col]), "unit": ""}
                elif col in row:
                    if pd.isna(row[col]) or np.isinf(row[col]):
                        etf_data[col] = {"value": "", "unit": ""}
                    else:
                        value, unit = self.factor_utils.convert_unit_and_value(
                            filtered_etf.market_filter,
                            float(row[col]),
                            factors[col].get("unit", "") if col in factors else "",
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

        return mapped_result, has_next

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
