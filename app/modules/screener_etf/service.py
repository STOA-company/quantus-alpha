from typing import List, Optional
from fastapi import HTTPException
import numpy as np
import pandas as pd
from app.common.constants import (
    ETF_DEFAULT_SCREENER_COLUMNS,
    FACTOR_MAP,
    FACTOR_MAP_ETF_KR,
    FACTOR_MAP_ETF_US,
    REVERSE_FACTOR_MAP,
    UNIT_MAP,
)
from app.modules.screener_etf.enum import ETFCategoryEnum, ETFMarketEnum
from app.modules.screener.schemas import FactorResponse, GroupFilter
from app.modules.screener.service import ScreenerService
from app.modules.screener_etf.schemas import FilteredETF
from app.utils import factor_utils
from app.utils.etf_utils import ETFDataLoader
from app.utils.factor_utils import FactorUtils
from app.utils.score_utils import calculate_factor_score
from app.cache.factors import factors_cache


class ScreenerETFService(ScreenerService):
    def __init__(self):
        super().__init__()
        self.factor_utils = FactorUtils()
        self.factor_loader = ETFDataLoader()

    def get_etf_factors(self, market: ETFMarketEnum):
        factors = self.factor_utils.get_factors(is_stock=False, is_etf=True)
        if market in [ETFMarketEnum.US, ETFMarketEnum.NYSE, ETFMarketEnum.NASDAQ, ETFMarketEnum.BATS]:
            nation = "us"
        else:
            nation = "kr"

        result = []
        for factor in factors:
            # 국가별 팩터 필터링 (국가별로 지원하는 Etf factor가 다르기 때문에)
            if nation == "kr" and factor["factor"] not in FACTOR_MAP_ETF_KR:
                continue
            if nation == "us" and factor["factor"] not in FACTOR_MAP_ETF_US:
                continue

            if factor["unit"] == "small_price":
                unit = "원" if nation == "kr" else "$"
            elif factor["unit"] == "big_price":
                unit = "억원" if nation == "kr" else "K$"
            else:
                unit = UNIT_MAP[factor["unit"]]
            result.append(
                FactorResponse(
                    factor=factor["factor"],
                    description=factor["description"],
                    unit=unit,
                    category=factor["category"],
                    direction=factor["direction"],
                    min_value=factor["min_value"],
                    max_value=factor["max_value"],
                )
            )
        return result

    def get_filtered_etfs(self, filtered_etf: FilteredETF):
        # 데이터 로드
        df_etfs = self.factor_loader.load_etf_factors(filtered_etf.market_filter.value)
        # 필터링
        df_etfs = self._filter_etfs(df_etfs, filtered_etf)

        # 점수 계산
        df_scored = calculate_factor_score(df_etfs, is_etf=True)

        # 병합
        df_etfs = df_etfs.merge(df_scored, on="ticker", how="inner")

        # 정렬
        df_etfs = df_etfs.sort_values("score", ascending=False).reset_index(drop=True)

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
        factors = factors_cache.get_configs()
        result = []
        for _, row in df_etfs.iterrows():
            # 기본으로 표시될 컬럼들
            etf_data = {
                "ticker": row["ticker"],
                "name": row["kr_name"],
            }

            # 숫자형 데이터 처리
            for col in df_etfs.columns:
                if col in [
                    "ticker",
                    "ctry",
                    "kr_name",
                    "manager",
                    "date",
                    "market",
                    "kr_name",
                    "en_name",
                    "listing_date",
                    "base_index_name",
                    "replication_method",
                    "base_asset_classification",
                    "tax_type",
                    "is_hedge",
                ]:
                    continue

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
        for item in result:
            mapped_item = {}
            for key, value in item.items():
                mapped_key = FACTOR_MAP.get(key, key)
                mapped_item[mapped_key] = value
            mapped_result.append(mapped_item)

        return mapped_result, has_next

    def _filter_etfs(self, df_etfs: pd.DataFrame, filtered_etf: FilteredETF):
        # 종목 필터 - 기본 필터
        if filtered_etf.market_filter:
            # if filtered_etf.market_filter == ETFMarketEnum.US:
            #     df_etfs = df_etfs[df_etfs["country"] == "us"]
            if filtered_etf.market_filter in [ETFMarketEnum.KR]:
                df_etfs = df_etfs[df_etfs["market"] == "KRX"]
            elif filtered_etf.market_filter in [ETFMarketEnum.NYSE, ETFMarketEnum.NASDAQ, ETFMarketEnum.BATS]:
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
        required_columns = ETF_DEFAULT_SCREENER_COLUMNS
        if filtered_etf.columns is not None:
            required_columns = required_columns + [col["factor"] for col in custom_filters if col not in required_columns]

        df_etfs = df_etfs[required_columns]

        return df_etfs

    def get_filtered_etfs_count(self, filtered_etf: FilteredETF):
        df_etfs = self.factor_loader.load_etf_factors(filtered_etf.market_filter.value)

        df_etfs = self._filter_etfs(df_etfs, filtered_etf)

        return df_etfs.shape[0]

    def create_or_update_group(self, current_user: str, group_filter: GroupFilter):
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
                type=group_filter.type,
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
            factor_filters = factor_utils.get_columns(category)

        columns = [factor_filter for factor_filter in factor_filters]

        result = ETF_DEFAULT_SCREENER_COLUMNS + columns
        return [FACTOR_MAP[column] for column in result]
