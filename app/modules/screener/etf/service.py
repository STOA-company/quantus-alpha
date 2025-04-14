import datetime
from io import BytesIO
import os
from typing import List, Literal, Optional, Dict, Tuple
import numpy as np
import pandas as pd
from Aws.logic.s3 import get_data_from_bucket
from app.common.constants import (
    FACTOR_MAP,
    FACTOR_MAP_EN,
    BASE_COLUMNS_ETF,
    SELECT_MAP,
    PARQUET_DIR,
    UNIT_MAP,
)
from app.core.exception.base import CustomException
from app.core.logger import setup_logger
from app.modules.screener.etf.enum import ETFMarketEnum
from app.modules.screener.base import BaseScreenerService
from app.modules.screener.stock.schemas import ExcludeEnum
from app.utils.date_utils import get_business_days
from app.utils.score_utils import etf_score_utils
from app.modules.screener.utils import screener_utils
from app.cache.factors import etf_factors_cache

logger = setup_logger(__name__)


class ScreenerETFService(BaseScreenerService):
    """ETF 스크리너 서비스 클래스"""

    def __init__(self):
        super().__init__()

    def _is_stock(self) -> bool:
        return False

    def get_factors(self, market: ETFMarketEnum):
        """
        ETF 팩터 정보 조회
        """
        try:
            factors = screener_utils.get_etf_factors(market)

            if market in [ETFMarketEnum.US, ETFMarketEnum.NASDAQ, ETFMarketEnum.NYSE, ETFMarketEnum.BATS]:
                nation = "us"
            elif market == ETFMarketEnum.ALL:
                nation = "global"
            else:
                nation = "kr"

            result = []
            for factor in factors:
                if factor["unit"] == "small_price":
                    unit = "원" if nation == "kr" else "$"
                elif factor["unit"] == "big_price":
                    unit = "억원" if nation == "kr" else "K$"
                else:
                    unit = UNIT_MAP[factor["unit"]]

                result.append(
                    {
                        "factor": FACTOR_MAP[factor["factor"]],
                        "description": factor["description"],
                        "unit": unit,
                        "category": factor["category"],
                        "direction": factor["direction"],
                        "min_value": factor["min_value"],
                        "max_value": factor["max_value"],
                        "type": factor["type"],
                        "presets": factor["presets"],
                    }
                )

            return result

        except Exception as e:
            logger.exception(f"Error in get_factors: {e}")
            raise e

    def get_filtered_etfs(
        self,
        market_filter: Optional[ETFMarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        exclude_filters: Optional[List[ExcludeEnum]] = None,
        custom_filters: Optional[List[Dict]] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = 50,
        offset: Optional[int] = 0,
        sort_by: Optional[str] = "score",
        ascending: Optional[bool] = False,
        lang: Optional[str] = "kr",
    ) -> Tuple[List[Dict], int]:
        """
        필터링된 ETF 목록 조회
        """
        try:
            if columns is None:
                columns = []

            if sort_by not in columns and sort_by not in BASE_COLUMNS_ETF:
                raise CustomException(status_code=400, message="sort_by must be in columns")

            etfs = screener_utils.filter_etfs(market_filter, custom_filters)
            filtered_df = screener_utils.get_filtered_etfs_df(market_filter, etfs, columns)

            # 여기서 display가 null인 값들을 미리 필터링
            for col in filtered_df.columns:
                if col in SELECT_MAP:
                    # 해당 컬럼의 값이 SELECT_MAP에 있고, display가 null인 값들을 찾아내기
                    values_with_null_display = [item["value"] for item in SELECT_MAP[col] if item["display"] is None]

                    if values_with_null_display:
                        # 해당 값을 가진 행 제외
                        filtered_df = filtered_df[~filtered_df[col].isin(values_with_null_display)]

            # 이제 필터링된 데이터프레임으로 점수 계산
            scored_df = etf_score_utils.calculate_factor_score(filtered_df)
            if scored_df.empty:
                return [], 0

            # 병합
            merged_df = filtered_df.merge(scored_df, on="Code", how="inner")

            # 티커(Code) 기준으로 중복 데이터 제거 (첫번째 항목만 유지)
            merged_df = merged_df.drop_duplicates(subset=["Code"])

            # 정렬
            sorted_df = merged_df.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)
            # 페이징
            total_count = len(sorted_df)
            sorted_df = sorted_df.iloc[offset * limit : offset * limit + limit]

            factors = etf_factors_cache.get_configs()
            result = []

            if columns:
                ordered_columns = []
                for col in columns:
                    mapped_col = next((k for k, v in FACTOR_MAP.items() if v == col), col)
                    if mapped_col not in ordered_columns:
                        ordered_columns.append(mapped_col)
            else:
                ordered_columns = ["Code", "Name", "manager", "score", "country"]

            selected_columns = ordered_columns.copy()
            existing_columns = sorted_df.columns.tolist()
            selected_columns = [col for col in selected_columns if col in existing_columns]
            sorted_df = sorted_df[selected_columns]

            for _, row in sorted_df.iterrows():
                # 기본으로 표시될 컬럼들
                etf_data = {}

                # 숫자형 데이터 처리
                for col in sorted_df.columns:
                    if col in BASE_COLUMNS_ETF:
                        etf_data[col] = row[col]
                    elif col in SELECT_MAP:
                        value_info = next(
                            (
                                {"value": item["value"], "display": item["display"]}
                                for item in SELECT_MAP[col]
                                if item["value"] == row[col]
                            ),
                            {"value": row[col], "display": row[col]},
                        )
                        # display가 None인 경우 value 값을 display로 사용
                        if value_info["display"] is None:
                            value_info["display"] = value_info["value"]
                        etf_data[col] = value_info
                    elif col == "score":
                        etf_data[col] = float(row[col])
                    elif col in row:
                        if col in row:
                            if isinstance(row[col], (int, float)):  # 값이 숫자인지 확인
                                if pd.isna(row[col]) or np.isinf(row[col]):
                                    etf_data[col] = {"value": "", "unit": ""}
                                else:
                                    value, unit = screener_utils.convert_unit_and_value(
                                        market_filter,
                                        float(row[col]),
                                        factors[col].get("unit", "") if col in factors else "",
                                        lang,
                                    )
                                    etf_data[col] = {"value": value, "unit": unit}
                            else:  # 숫자가 아닌 타입 처리
                                if pd.isna(row[col]):
                                    etf_data[col] = {"value": "", "unit": ""}
                                else:
                                    etf_data[col] = {"value": str(row[col]), "unit": ""}

                result.append(etf_data)

            mapped_result = []
            factor_map = FACTOR_MAP if lang == "kr" else FACTOR_MAP_EN

            # 티커 중복 방지를 위한 세트
            seen_codes = set()
            for item in result:
                # 티커 코드가 이미 처리된 경우 스킵
                if "Code" in item and item["Code"] in seen_codes:
                    continue

                # 새로운 티커 코드 기록
                if "Code" in item:
                    seen_codes.add(item["Code"])

                mapped_item = {}
                for key, value in item.items():
                    mapped_key = factor_map.get(key, key)
                    mapped_item[mapped_key] = value
                mapped_result.append(mapped_item)

            return mapped_result, total_count

        except Exception as e:
            logger.error(f"Error in get_filtered_etfs: {e}")
            raise e

    def get_filtered_data(self, **kwargs):
        """
        필터링된 데이터 조회 (BaseScreenerService 추상 메서드 구현)
        """
        return self.get_filtered_etfs(**kwargs)

    def get_filtered_data_count(
        self,
        market_filter: Optional[ETFMarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        columns: Optional[List[str]] = None,
    ) -> int:
        """
        필터링된 ETF 개수 조회
        """
        etfs = screener_utils.filter_etfs(market_filter, custom_filters)
        filtered_df = screener_utils.get_filtered_etfs_df(market_filter, etfs, columns)

        return len(filtered_df)

    def update_parquet(self, ctry: Literal["KR", "US"]):
        """
        파케이 파일 업데이트
        """
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

    def get_filtered_etfs_download(
        self,
        market_filter: Optional[ETFMarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        columns: Optional[List[str]] = None,
        sort_by: Optional[str] = "score",
        ascending: Optional[bool] = False,
        lang: Optional[str] = "kr",
    ) -> pd.DataFrame:
        try:
            valid_sort_cols = ["Code", "Name", "country", "market", "score"]
            if columns is None:
                columns = []
            if sort_by not in columns and sort_by not in valid_sort_cols:
                raise CustomException(status_code=400, message="sort_by must be in columns")

            etfs = screener_utils.filter_etfs(market_filter, custom_filters)
            filtered_df = screener_utils.get_filtered_etfs_df(market_filter, etfs, columns)
            scored_df = etf_score_utils.calculate_factor_score(filtered_df)
            if scored_df.empty:
                print(f"scored_df is empty. filtered_df columns: {filtered_df.columns.tolist()}")

                return pd.DataFrame()

            merged_df = filtered_df.merge(scored_df, on="Code", how="inner")
            print(f"merged_df shape: {merged_df.shape}")
            sorted_df = merged_df.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)
            print(f"sorted_df shape: {sorted_df.shape}")

            if market_filter in [ETFMarketEnum.US, ETFMarketEnum.NASDAQ, ETFMarketEnum.NYSE, ETFMarketEnum.BATS]:
                sorted_df["Code"] = sorted_df["Code"].str.replace("-US", "")

            if columns:
                ordered_columns = []
                for col in columns:
                    mapped_col = next((k for k, v in FACTOR_MAP.items() if v == col), col)
                    if mapped_col not in ordered_columns:
                        ordered_columns.append(mapped_col)
            else:
                ordered_columns = ["Code", "Name", "score", "country"]

            sorted_df = sorted_df[ordered_columns]

            factors = etf_factors_cache.get_configs()
            for col in ordered_columns:
                if col in BASE_COLUMNS_ETF or col in ["Code", "Name"]:
                    continue
                elif col == "score":
                    sorted_df[col] = sorted_df[col].astype(float)
                elif col in sorted_df.columns:

                    def convert_value(x):
                        try:
                            if pd.isna(x):
                                return ""
                            if isinstance(x, (int, float)) and np.isinf(x):
                                return ""
                            if not isinstance(x, (int, float)):
                                return x
                            value, _ = screener_utils.convert_unit_and_value(
                                market_filter,
                                float(x),
                                factors[col].get("unit", "") if col in factors else "",
                                lang,
                            )
                            return value
                        except Exception:
                            return x

                    sorted_df[col] = sorted_df[col].apply(convert_value)

            factor_map = FACTOR_MAP_EN if lang == "en" else FACTOR_MAP
            sorted_df.rename(columns=lambda x: factor_map.get(x, x), inplace=True)

            return sorted_df

        except Exception as e:
            logger.error(f"Error in get_filtered_etfs_download: {e}")
            raise e
