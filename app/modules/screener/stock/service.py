from typing import Optional, List, Dict, Tuple
import logging
from app.utils.score_utils import score_utils
from app.cache.factors import factors_cache
import pandas as pd
import numpy as np
from app.modules.screener.stock.schemas import MarketEnum
from app.modules.screener.utils import screener_utils
from app.common.constants import NON_NUMERIC_COLUMNS, UNIT_MAP, FACTOR_MAP, FACTOR_MAP_EN
from app.core.exception.custom import CustomException
from app.modules.screener.base import BaseScreenerService

logger = logging.getLogger(__name__)


class ScreenerStockService(BaseScreenerService):
    """주식 스크리너 서비스 클래스"""

    def __init__(self):
        super().__init__()

    def _is_stock(self) -> bool:
        """주식 관련 서비스임을 표시"""
        return True

    def get_factors(self, market: Optional[MarketEnum] = None, lang: str = "kr"):
        """
        팩터 정보 조회
        """
        try:
            factors = screener_utils.get_factors(market)

            if market in [MarketEnum.US, MarketEnum.SNP500, MarketEnum.NASDAQ]:
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

                factor_map = FACTOR_MAP if lang == "kr" else FACTOR_MAP_EN
                result.append(
                    {
                        "factor": factor["factor"],
                        "display": factor_map[factor["factor"]],
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

    def get_filtered_stocks(
        self,
        market_filter: Optional[MarketEnum] = MarketEnum.ALL,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        columns: Optional[List[str]] = [],
        limit: Optional[int] = 50,
        offset: Optional[int] = 0,
        sort_by: Optional[str] = "score",
        ascending: Optional[bool] = False,
        lang: Optional[str] = "kr",
    ) -> Tuple[List[Dict], int]:
        """
        필터링된 주식 목록 조회
        """
        try:
            if sort_by not in columns and sort_by not in [
                "Code",
                "Name",
                "Name_en",
                "country",
                "market",
                "sector",
                "sector_en",
                "score",
            ]:
                raise CustomException(status_code=400, message="sort_by must be in columns")

            if sector_filter:
                for sector in sector_filter:
                    if sector not in self.get_available_sectors():
                        raise CustomException(status_code=400, message=f"Invalid sector: {sector}")

            stocks = screener_utils.filter_stocks(market_filter, sector_filter, custom_filters)
            filtered_df = screener_utils.get_filtered_stocks_df(market_filter, stocks, columns)
            scored_df = score_utils.calculate_factor_score(filtered_df)
            if scored_df.empty:
                return [], 0
            merged_df = filtered_df.merge(scored_df, on="Code", how="inner")
            sorted_df = merged_df.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)
            if market_filter in [MarketEnum.US, MarketEnum.SNP500, MarketEnum.NASDAQ]:
                sorted_df["Code"] = sorted_df["Code"].str.replace("-US", "")

            total_count = len(sorted_df)
            sorted_df = sorted_df.iloc[offset * limit : offset * limit + limit]

            factors = factors_cache.get_configs()
            result = []

            if lang == "en":
                sorted_df["sector"] = sorted_df["sector_en"] if "sector" in columns else None
                sorted_df["Name"] = sorted_df["Name_en"] if "Name" in columns else None

            sorted_df = sorted_df[columns]

            factor_map = FACTOR_MAP if lang == "kr" else FACTOR_MAP_EN

            for _, row in sorted_df.iterrows():
                stock_data = {}

                for col in columns:
                    if col in NON_NUMERIC_COLUMNS:
                        if col in row:
                            stock_data[col] = {"display": factor_map[col], "value": row[col], "unit": ""}
                    elif col == "score":
                        stock_data[col] = {"display": factor_map["score"], "value": float(row[col]), "unit": ""}
                    elif col in row:
                        if pd.isna(row[col]) or np.isinf(row[col]):  # NA / INF -> 빈 문자열
                            stock_data[col] = {"display": factor_map[col], "value": "", "unit": ""}
                        else:
                            value, unit = screener_utils.convert_unit_and_value(
                                market_filter,
                                float(row[col]),
                                factors[col].get("unit", "") if col in factors else "",
                                lang,
                            )
                            stock_data[col] = {"display": factor_map[col], "value": value, "unit": unit}

                result.append(stock_data)

            return result, total_count

        except Exception as e:
            logger.error(f"Error in get_filtered_stocks: {e}")
            raise e

    def get_filtered_data(self, **kwargs):
        """
        필터링된 데이터 조회 (BaseScreenerService 추상 메서드 구현)
        """
        return self.get_filtered_stocks(**kwargs)

    def get_available_sectors(self, lang: str = "kr") -> List[str]:
        """
        사용 가능한 섹터 목록 조회
        """
        kr_df = pd.read_parquet("parquet/kr_stock_factors.parquet")
        us_df = pd.read_parquet("parquet/us_stock_factors.parquet")
        sector_lang = "sector" if lang == "kr" else "sector_en"
        kr_sectors = kr_df[sector_lang].unique().tolist()
        us_sectors = us_df[sector_lang].unique().tolist()

        sectors = list(set(kr_sectors + us_sectors))

        return sectors

    def get_filtered_data_count(
        self,
        market_filter: Optional[MarketEnum] = None,
        sector_filter: Optional[List[str]] = None,
        custom_filters: Optional[List[Dict]] = None,
        columns: Optional[List[str]] = None,
    ) -> int:
        """
        필터링된 주식 개수 조회
        """
        try:
            stocks = screener_utils.filter_stocks(market_filter, sector_filter, custom_filters)
            filtered_df = screener_utils.get_filtered_stocks_df(market_filter, stocks, columns)

            return len(filtered_df)
        except Exception as e:
            logger.error(f"Error in get_filtered_stocks_count: {e}")
            raise e
