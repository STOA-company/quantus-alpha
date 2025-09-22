import asyncio
from typing import Dict, List, Tuple, Optional
import os
from functools import lru_cache
import json

import pandas as pd
from fastapi import HTTPException
from sqlalchemy import select
from datetime import datetime

from app.cache.leaderboard import StockLeaderboard
from app.core.logger import setup_logger
from app.database.crud import JoinInfo, database
from app.models.models_stock import StockInformation
from app.modules.common.enum import StabilityStatus, StabilityType, TranslateCountry
from app.modules.common.utils import contry_mapping
from app.modules.stock_info.mapping import STABILITY_INFO
from app.modules.stock_info.schemas import Indicators, SimilarStock, StockInfo

logger = setup_logger(__name__)


class StockInfoService:
    def __init__(self):
        self.db = database
        self.file_path = "static"
        self.file_name = "stock_{}_info.csv"
        # 메모리 캐시: 파일 데이터를 딕셔너리로 저장하여 pandas 의존성 줄이기
        self._summary_cache: Dict[str, Dict] = {}
        self._etf_cache: Optional[Dict] = None
        self._morningstar_cache: Optional[Dict] = None
        # 섹터별 지표 캐시 (섹터별로 계산된 평균 지표를 캐시)
        self._sector_metrics_cache: Dict[str, Dict[str, float]] = {}

    def _load_summary_data(self, ctry: str) -> Dict[str, Dict]:
        """
        요약 데이터를 메모리 캐시로 로드 (pandas 사용 최소화)
        """
        if ctry not in self._summary_cache:
            try:
                intro_file_path = f"{self.file_path}/summary_{ctry}.parquet"
                if os.path.exists(intro_file_path):
                    # pandas로 한 번만 읽어서 딕셔너리로 변환 후 캐시
                    df = pd.read_parquet(intro_file_path)
                    # Code를 키로 하는 딕셔너리로 변환
                    self._summary_cache[ctry] = {
                        str(row['Code']): row.to_dict() 
                        for _, row in df.iterrows()
                    }
                    # logger.info(f"Loaded summary data for {ctry}: {len(self._summary_cache[ctry])} records")
                else:
                    logger.warning(f"Summary file not found: {intro_file_path}")
                    self._summary_cache[ctry] = {}
            except Exception as e:
                logger.error(f"Error loading summary data for {ctry}: {str(e)}")
                self._summary_cache[ctry] = {}
        
        return self._summary_cache[ctry]

    def _get_stock_introduction(self, ctry: str, ticker: str, lang: TranslateCountry) -> str:
        """
        종목 소개 정보 조회 (캐시 사용)
        """
        if ctry == "kr":
            lookup_ticker = ticker.replace("A", "")
        else:
            lookup_ticker = ticker
            
        summary_data = self._load_summary_data(ctry)
        intro_result = summary_data.get(lookup_ticker, {})
        
        if not intro_result:
            return ""
            
        if lang == TranslateCountry.KO:
            if ctry == "kr":
                return intro_result.get("overview", "")
            else:
                return intro_result.get("translated_overview", "")
        else:
            if ctry == "kr":
                return ""
            else:
                return intro_result.get("overview", "")

    async def get_ctry_by_ticker(self, ticker: str) -> str:
        """
        종목 코드에 따른 국가 코드 조회
        """
        result = await self.db._select_async(table="stock_information", columns=["ctry"], **{"ticker": ticker})
        return result[0].ctry
        # return await self.db._select_async(table="stock_information", columns=["ctry"], **{"ticker": ticker})[0].ctry

    def get_name_by_ticker(self, ticker: str) -> Tuple[str, str]:
        """
        종목 코드에 따른 종목명 조회
        """
        kr_name, en_name = self.db._select(
            table="stock_information", columns=["kr_name", "en_name"], **{"ticker": ticker}
        )[0]
        return kr_name, en_name

    async def get_stock_info(self, ctry: str, ticker: str, lang: TranslateCountry) -> StockInfo:
        """
        주식 정보 조회
        """
        try:
            if ctry == "kr":
                ticker = ticker.replace("A", "")

            intro_result = {}
            result = {}

            # Parquet 파일에서 데이터 읽기
            intro_file_path = f"{self.file_path}/summary_{ctry}.parquet"
            intro_df = pd.read_parquet(intro_file_path)
            intro_df_filtered = intro_df[intro_df["Code"] == ticker]
            if not intro_df_filtered.empty:
                intro_result = intro_df_filtered.to_dict(orient="records")[0]

            if ctry == "kr":
                ticker = "A" + ticker

            # DB에서 데이터 읽기
            table_name = "stock_information"
            columns = ["ticker", "homepage_url", "ceo", "establishment_date", "listing_date"]
            db_result = self.db._select(table=table_name, columns=columns, **{"ticker": ticker})
            

            if db_result:
                result = db_result[0]._asdict()
                # datetime.date 타입인 경우에만 문자열로 변환
                if result.get("establishment_date") and hasattr(result["establishment_date"], "strftime"):
                    result["establishment_date"] = result["establishment_date"].strftime("%Y-%m-%d")
                if result.get("listing_date") and hasattr(result["listing_date"], "strftime"):
                    result["listing_date"] = result["listing_date"].strftime("%Y-%m-%d")

            if lang == TranslateCountry.KO:
                if ctry == "kr":
                    introduction = intro_result.get("overview", "")
                else:
                    introduction = intro_result.get("translated_overview", "")
            else:
                if ctry == "kr":
                    introduction = ""
                else:
                    introduction = intro_result.get("overview", "")

            return StockInfo(
                introduction=introduction,
                homepage_url=result.get("homepage_url", ""),
                ceo_name=result.get("ceo", ""),
                establishment_date=result.get("establishment_date", ""),
                listing_date=result.get("listing_date", ""),
            )

        except Exception as e:
            logger.error(f"Error in get_stock_info for {ticker}: {str(e)}")
            return StockInfo(introduction="", homepage_url="", ceo_name="", establishment_date="", listing_date="")

    def round_and_clean(self, value: float, round_num: int = 1) -> float:
        """
        소수점 첫째자리에서 반올림하고, 소수점이 0이면 정수로 변환
        예: 15.7 -> 15.7, 15.0 -> 15
        """
        rounded = round(value, round_num)
        return int(rounded) if rounded.is_integer() else rounded

    def get_stability_status(self, score: float, stability_type: StabilityType) -> StabilityStatus:
        """
        점수에 따른 안정성 상태를 반환합니다.

        Args:
            score (float): 안정성 점수
            threshold (StabilityThreshold): 임계값 설정

        Returns:
            StabilityStatus: 안정성 상태 (좋음, 보통, 나쁨)
        """
        threshold = STABILITY_INFO[stability_type].threshold

        if score >= threshold.GOOD:
            return StabilityStatus.GOOD
        elif score >= threshold.BAD:
            return StabilityStatus.NORMAL
        return StabilityStatus.BAD

    async def get_indicators(self, ctry: str, ticker: str) -> Indicators:
        """지표 조회"""

        if ctry == "us":
            ticker = f"{ticker}-US"

        # ctry 3자리 코드로 변환
        ctry_3 = contry_mapping[ctry]

        # 현재 종목의 지표 조회
        table_name = f"{ctry_3}_stock_factors"
        logger.info(f"[get_indicators] Querying {table_name} for ticker: {ticker}")
        basic_columns = ["per", "pbr", "roe"]
        stability_columns = [info.db_column for info in STABILITY_INFO.values()]
        columns = basic_columns + stability_columns

        current_stock = await self.db._select_async(
            table=table_name,
            columns=columns,
            **{"ticker": ticker},
        )
        logger.info(f"[get_indicators] Query result count: {len(current_stock) if current_stock else 0}")

        if not current_stock:
            return Indicators(
                per=None,
                industry_per=None,
                pbr=None,
                industry_pbr=None,
                roe=None,
                industry_roe=None,
                financial_data=None,
                price_trend=None,
                market_situation=None,
                industry_situation=None,
            )

        # 섹터 관련 데이터 계산
        sector_metrics = await self._calculate_sector_metrics(ticker, ctry, table_name, basic_columns)

        # 안정성 지표 상태 계산
        stability_statuses = {}
        for stability_type, info in STABILITY_INFO.items():
            score = getattr(current_stock[0], info.db_column)
            status = self.get_stability_status(score, stability_type)
            stability_statuses[info.api_field] = status.value

        return Indicators(
            per=self.round_and_clean(current_stock[0].per) if current_stock[0].per is not None else None,
            industry_per=sector_metrics["per"] if sector_metrics["per"] is not None else None,
            pbr=self.round_and_clean(current_stock[0].pbr) if current_stock[0].pbr is not None else None,
            industry_pbr=sector_metrics["pbr"] if sector_metrics["pbr"] is not None else None,
            roe=self.round_and_clean(current_stock[0].roe) if current_stock[0].roe is not None else None,
            industry_roe=sector_metrics["roe"] if sector_metrics["roe"] is not None else None,
            **stability_statuses,
        )

    # 관련 섹터 조회
    async def get_related_sectors(self, ticker: str) -> List[StockInformation]:
        # 섹터 조회
        query = select(StockInformation.sector_2).where(StockInformation.ticker == ticker)
        result = await self.db._execute_async(query)
        sector = result.scalars().first()

        if not sector:
            return []

        # 관련 섹터의 ticker 조회
        query = select(StockInformation).where(StockInformation.sector_2 == sector)
        result = await self.db._execute_async(query)
        related_sectors = result.scalars().all()

        return related_sectors

    async def get_similar_stocks(self, ticker: str, lang: TranslateCountry) -> List[SimilarStock]:
        """
        연관 종목 조회 - DB 접근 최적화: 단일 조인 쿼리로 통합

        Args:
            ticker (str): 종목 코드
            lang (TranslateCountry): 언어 설정

        Returns:
            List[SimilarStock]: 연관 종목 리스트
        """
        try:
            # 언어에 따른 컬럼 선택
            name_column = "kr_name" if lang == TranslateCountry.KO else "en_name"
            
            # 단일 조인 쿼리로 모든 필요한 데이터를 한 번에 조회
            similar_stocks_data = await self.db._select_async(
                table="stock_trend",
                columns=["ticker", name_column, "ctry", "current_price", "change_rt"],
                join_info=JoinInfo(
                    primary_table="stock_trend",
                    secondary_table="stock_information",
                    primary_column="ticker",
                    secondary_column="ticker",
                    columns=["sector_2", "is_delisted", "is_trading_stopped"],
                    secondary_condition={
                        "is_delisted": 0, 
                        "is_trading_stopped": 0,
                        "is_activate": True,
                        "ticker__not": ticker  # 자기 자신 제외
                    },
                ),
                limit=6,
                # 서브쿼리로 현재 종목의 섹터와 국가를 조건으로 사용
                **{
                    "sector_2__in": f"(SELECT sector_2 FROM stock_information WHERE ticker = '{ticker}')",
                    "ctry__in": f"(SELECT ctry FROM stock_information WHERE ticker = '{ticker}')"
                }
            )

            if not similar_stocks_data:
                # 대안: 기존 방식 사용 (fallback)
                return await self._get_similar_stocks_fallback(ticker, lang)

            similar_stocks = []
            for stock in similar_stocks_data:
                name = getattr(stock, name_column)
                similar_stocks.append(
                    SimilarStock(
                        ticker=stock.ticker,
                        name=name,
                        ctry=stock.ctry,
                        current_price=stock.current_price,
                        current_price_rate=stock.change_rt,
                    )
                )

            return similar_stocks
            
        except Exception as e:
            logger.error(f"Error in optimized get_similar_stocks for {ticker}: {str(e)}")
            # 에러 발생 시 기존 방식으로 fallback
            return await self._get_similar_stocks_fallback(ticker, lang)

    async def _get_similar_stocks_fallback(self, ticker: str, lang: TranslateCountry) -> List[SimilarStock]:
        """
        연관 종목 조회 - 기존 방식 (fallback용)
        """
        ticker_sector = await self.db._select_async(table="stock_information", columns=["sector_2", "ctry"], **{"ticker": ticker})
        if not ticker_sector:
            raise HTTPException(status_code=404, detail=f"Stock not found: {ticker}")
        ctry = ticker_sector[0].ctry

        similar_tickers = await self.db._select_async(
            table="stock_information",
            columns=["ticker"],
            limit=6,
            **{"sector_2": ticker_sector[0].sector_2, "ticker__not": ticker, "is_activate": True, "ctry": ctry},
        )
        similar_tickers = [ticker.ticker for ticker in similar_tickers]

        if lang == TranslateCountry.KO:
            columns = ["ticker", "kr_name", "ctry", "current_price", "change_rt"]
        elif lang == TranslateCountry.EN:
            columns = ["ticker", "en_name", "ctry", "current_price", "change_rt"]

        similar_stocks_data = await self.db._select_async(
            table="stock_trend",
            columns=columns,
            join_info=JoinInfo(
                primary_table="stock_trend",
                secondary_table="stock_information",
                primary_column="ticker",
                secondary_column="ticker",
                columns=["is_delisted", "is_trading_stopped"],
                secondary_condition={"is_delisted": 0, "is_trading_stopped": 0},
            ),
            **{"ticker__in": similar_tickers},
        )

        similar_stocks = []
        for stock in similar_stocks_data:
            name = stock.kr_name if lang == TranslateCountry.KO else stock.en_name
            similar_stocks.append(
                SimilarStock(
                    ticker=stock.ticker,
                    name=name,
                    ctry=stock.ctry,
                    current_price=stock.current_price,
                    current_price_rate=stock.change_rt,
                )
            )

        return similar_stocks

    async def get_current_price(self, ticker: str, table_name: str) -> Tuple[float, float]:
        """
        현재가와 변동률 조회
        Args:
            ticker: 종목코드
            table_name: 테이블명
        Returns:
            Tuple[float, float]: (현재가, 변동률)
        """
        result = self.db._select(
            table=table_name,
            columns=["Close", "Open"],
            order="Date",
            ascending=False,
            limit=1,
            **{"Ticker": ticker},  # kwargs로 전달
        )

        if not result:
            return 0.0, 0.0

        row = result[0]  # fetchall()의 결과이므로 인덱싱으로 접근
        current_price = float(row.Close)
        open_price = float(row.Open)

        # 변동률 계산: ((종가 - 시가) / 시가) * 100
        price_rate = round(((current_price - open_price) / open_price * 100), 2) if open_price != 0 else 0.0

        return current_price, price_rate

    async def _calculate_sector_metrics(
        self, ticker: str, ctry: str, table_name: str, columns: List[str]
    ) -> Dict[str, float]:
        """섹터 관련 지표 계산 - 캐시 사용으로 최적화"""
        sector_ticker = ticker.replace("-US", "")
        
        # 캐시 키 생성
        cache_key = f"{sector_ticker}_{ctry}_{table_name}"
        # logger.info(f"[CACHE] Looking for cache key: {cache_key}")
        # logger.info(f"[CACHE] Current cache keys: {list(self._sector_metrics_cache.keys())}")
        
        # 캐시에서 확인
        if cache_key in self._sector_metrics_cache:
            logger.info(f"[CACHE] Using cached sector metrics for {cache_key}")
            return self._sector_metrics_cache[cache_key]
        
        try:
            sector_tickers = await self.get_related_sectors(sector_ticker)
            
            # sector_tickers가 이미 문자열 리스트인 경우
            if sector_tickers and isinstance(sector_tickers[0], str):
                # 이미 문자열 리스트이므로 그대로 사용
                if ctry == "us":
                    sector_tickers = [f"{t}-US" for t in sector_tickers]
                # else: 이미 올바른 형태
            else:
                # StockInformation 객체 리스트인 경우
                if ctry == "us":
                    sector_tickers = [f"{t.ticker}-US" for t in sector_tickers if hasattr(t, 'ticker') and t.ticker]
                else:
                    sector_tickers = [t.ticker for t in sector_tickers if hasattr(t, 'ticker') and t.ticker]

            if not sector_tickers:
                result = {metric: 0 for metric in columns}
                self._sector_metrics_cache[cache_key] = result
                return result

            sector_results = await self.db._select_async(table=table_name, columns=columns, **{"ticker__in": sector_tickers})

            if not sector_results:
                result = {metric: 0 for metric in columns}
                self._sector_metrics_cache[cache_key] = result
                return result

            # 섹터 평균 계산
            sector_metrics = {}
            for metric in columns:
                values = [getattr(stock, metric) for stock in sector_results if getattr(stock, metric) is not None]
                if values:
                    sector_metrics[metric] = self.round_and_clean(sum(values) / len(values))
                else:
                    sector_metrics[metric] = 0

            # 캐시에 저장
            self._sector_metrics_cache[cache_key] = sector_metrics
            logger.info(f"Cached sector metrics for {cache_key}: {len(sector_tickers)} tickers")
            
            return sector_metrics
            
        except Exception as e:
            logger.error(f"Error calculating sector metrics for {ticker}: {str(e)}")
            result = {metric: 0 for metric in columns}
            self._sector_metrics_cache[cache_key] = result
            return result

    def increment_search_score(self, ticker: str) -> None:
        redis = StockLeaderboard()
        stock_info = self.db._select(table="stock_information", columns=["kr_name", "en_name"], **{"ticker": ticker})
        kr_name = stock_info[0].kr_name
        en_name = stock_info[0].en_name
        redis.increment_score(ticker, kr_name, en_name)

    async def get_type(self, ticker: str) -> str:
        """
        종목 타입 조회
        """
        result = await self.db._select_async(table="stock_information", columns=["type"], **{"ticker": ticker})
        return result[0].type

    async def get_etf_info(self, ticker: str) -> dict:
        """
        ETF 정보 조회
        """
        original_ticker = ticker
        ticker = ticker.replace("A", "")
        df = pd.read_parquet("check_data/etf_krx/etf_integrated.parquet")
        df = df[df["단축코드"] == ticker]

        column_mapping = {
            "단축코드": "ticker",
            "한글종목명": "kr_name",
            "상장일": "listing_date",
            "운용사": "company",
            "순자산가치(NAV)": "nav",
            "시가총액": "market_cap",
            "상장좌수": "listed_shares",
            "순자산총액": "total_net_assets",
        }

        df = df.rename(columns=column_mapping)
        _, en_name = self.get_name_by_ticker(original_ticker)

        df["en_name"] = en_name
        df["ticker"] = original_ticker

        etf_info = df.to_dict(orient="records")[0] if not df.empty else {}

        return etf_info

    async def get_etf_holdings(self, ticker: str) -> List[dict]:
        """
        ETF 구성 종목 조회
        """
        join_info = JoinInfo(
            primary_table="etf_top_holdings",
            secondary_table="stock_information",
            primary_column="holding_ticker",
            secondary_column="ticker",
            columns=["kr_name", "en_name"],
        )

        data = self.db._select(
            table="etf_top_holdings",
            columns=["holding_ticker", "weight", "kr_name", "en_name"],
            join_info=join_info,
            ticker=ticker,
        )

        result = []
        for row in data:
            result.append(
                {"ticker": row.holding_ticker, "weight": row.weight, "kr_name": row.kr_name, "en_name": row.en_name}
            )

        result.sort(key=lambda x: x["weight"], reverse=True)
        sum_weight = sum(holding["weight"] for holding in result)
        if sum_weight != 100:
            result.append({"ticker": None, "weight": 100 - sum_weight, "kr_name": "기타", "en_name": "Others"})
        return result

    async def get_us_etf_info(self, ticker: str) -> dict:
        """
        미국 ETF 정보 조회

        Args:
            ticker (str): 미국 ETF 티커

        Returns:
            dict: ETF 정보 (종목코드, 종목명, 상장일, 운용사, 시가총액, 상장좌수)
        """
        try:
            etf_info = {
                "ticker": ticker,
                "en_name": None,
                "kr_name": None,
                "listing_date": None,
                "company": None,
                "market_cap": None,
                "listed_shares": None,
                "nav": None,
                "total_net_assets": None,
            }

            # 기본 정보 가져오기 (이름, 상장일 등)
            stock_info = await self.db._select_async(
                table="stock_information", columns=["ticker", "en_name", "kr_name", "listing_date"], **{"ticker": ticker}
            )

            if stock_info:
                etf_info["ticker"] = stock_info[0].ticker
                etf_info["en_name"] = stock_info[0].en_name
                etf_info["kr_name"] = stock_info[0].kr_name if stock_info[0].kr_name else stock_info[0].en_name
                if stock_info[0].listing_date and hasattr(stock_info[0].listing_date, "strftime"):
                    etf_info["listing_date"] = stock_info[0].listing_date.strftime("%Y-%m-%d")

            # 운용사 정보 가져오기
            try:
                morningstar_df = pd.read_parquet("check_data/etf_morningstar/us_etf_morningstar_rating.parquet")
                morningstar_data = morningstar_df[morningstar_df["ticker"] == ticker]

                if not morningstar_data.empty:
                    company_name = morningstar_data.iloc[0]["company_name"]
                    etf_info["company"] = company_name if company_name is not None else None
            except Exception as e:
                logger.error(f"Error loading morningstar data for {ticker}: {str(e)}")

            # ETF 가격 및 시가총액 정보 가져오기
            try:
                price_df = pd.read_parquet("check_data/etf/us_etf_price.parquet")
                # 최신 데이터만 선택
                price_df = price_df[price_df["Ticker"] == ticker].sort_values("MarketDate", ascending=False)

                if not price_df.empty:
                    latest_data = price_df.iloc[0]
                    etf_info["market_cap"] = float(latest_data["MktCap"]) if pd.notna(latest_data["MktCap"]) else None
                    etf_info["listed_shares"] = (
                        float(latest_data["NumShrs"]) if pd.notna(latest_data["NumShrs"]) else None
                    )
            except Exception as e:
                logger.error(f"Error loading price data for {ticker}: {str(e)}")

            return etf_info

        except Exception as e:
            logger.error(f"Error in get_us_etf_info for {ticker}: {str(e)}")
            return {"ticker": ticker, "error": str(e)}

    async def get_stock_info_db(self, ticker: str) -> StockInformation:
        result = await self.db._select_async(table="stock_information", **{"ticker": ticker})
        if not result:
            raise HTTPException(status_code=404, detail=f"Stock not found: {ticker}")
        return result[0]

    async def get_stock_factors_db(self, ctry: str, ticker: str):
        
        if ctry == "us":
            ticker = f"{ticker}-US"
        elif ctry == "USA":
            ticker = f"{ticker}-US"
        # ctry 3자리 코드로 변환
        ctry_3 = contry_mapping[ctry]

        # 현재 종목의 지표 조회
        table_name = f"{ctry_3}_stock_factors"
        logger.info(f"[get_stock_factors_db] Querying {table_name} for ticker: {ticker}")
        result = await self.db._select_async(table=table_name, **{"ticker": ticker})
        logger.info(f"[get_stock_factors_db] Query result count: {len(result) if result else 0}")
        if not result:
            logger.warning(f"Stock factors not found for ticker: {ticker} in table: {table_name}")
            return None
        return result[0]

    async def get_stock_info_with_factors_db(self, ticker: str):
        """
        종목 정보와 지표를 한 번의 조인 쿼리로 조회하여 DB 접근 최소화
        """
        try:
            # 1. 기본 종목 정보 조회
            stock_info_result = await self.db._select_async(
                table="stock_information", 
                **{"ticker": ticker}
            )
            
            if not stock_info_result:
                raise HTTPException(status_code=404, detail=f"Stock not found: {ticker}")
            
            stock_info = stock_info_result[0]
            ctry = stock_info.ctry
            
            # 2. 지표 정보 조회 (필요한 경우에만)
            stock_factors = None
            if stock_info.type == "stock":  # stock인 경우에만 지표 조회
                factors_ticker = ticker
                if ctry == "us":
                    factors_ticker = f"{ticker}-US"
                elif ctry == "USA":
                    factors_ticker = f"{ticker}-US"
                
                # ctry 3자리 코드로 변환
                ctry_3 = contry_mapping[ctry]
                table_name = f"{ctry_3}_stock_factors"
                
                logger.info(f"[get_stock_info_with_factors_db] Querying {table_name} for ticker: {factors_ticker}")
                factors_result = await self.db._select_async(table=table_name, **{"ticker": factors_ticker})
                
                if factors_result:
                    stock_factors = factors_result[0]
                    logger.info(f"[get_stock_info_with_factors_db] Found stock factors for {factors_ticker}")
                else:
                    logger.warning(f"Stock factors not found for ticker: {factors_ticker} in table: {table_name}")
            
            return stock_info, stock_factors
            
        except Exception as e:
            logger.error(f"Error in get_stock_info_with_factors_db for {ticker}: {str(e)}")
            raise

    async def get_stock_info_v2(self, ctry: str, ticker: str, lang: TranslateCountry, stock_info: StockInformation) -> StockInfo:
        """
        주식 정보 조회 - 최적화된 버전 (pandas 사용 최소화)
        """
        try:
            # 캐시된 소개 정보 조회
            introduction = self._get_stock_introduction(ctry, ticker, lang)
            
            # DB 결과를 직접 사용
            result = {
                "ticker": stock_info.ticker,
                "homepage_url": stock_info.homepage_url,
                "ceo": stock_info.ceo,
                "establishment_date": stock_info.establishment_date,
                "listing_date": stock_info.listing_date
            }

            # datetime.date 타입인 경우에만 문자열로 변환
            if result.get("establishment_date") and hasattr(result["establishment_date"], "strftime"):
                result["establishment_date"] = result["establishment_date"].strftime("%Y-%m-%d")
            if result.get("listing_date") and hasattr(result["listing_date"], "strftime"):
                result["listing_date"] = result["listing_date"].strftime("%Y-%m-%d")

            return StockInfo(
                introduction=introduction,
                homepage_url=result.get("homepage_url", ""),
                ceo_name=result.get("ceo", ""),
                establishment_date=result.get("establishment_date", ""),
                listing_date=result.get("listing_date", ""),
            )

        except Exception as e:
            logger.error(f"Error in get_stock_info_v2 for {ticker}: {str(e)}")
            return StockInfo(introduction="", homepage_url="", ceo_name="", establishment_date="", listing_date="")

    async def get_indicators_v2(self, ctry: str, ticker: str, stock_factors) -> Indicators:
        """지표 조회"""

        if ctry == "us":
            ticker = f"{ticker}-US"

        # ctry 3자리 코드로 변환
        ctry_3 = contry_mapping[ctry]

        # 현재 종목의 지표 조회
        table_name = f"{ctry_3}_stock_factors"
        basic_columns = ["per", "pbr", "roe"]
        # stability_columns = [info.db_column for info in STABILITY_INFO.values()]
        # columns = basic_columns + stability_columns


        # current_stock = await self.db._select_async(
        #     table=table_name,
        #     columns=columns,
        #     **{"ticker": ticker},
        # )
        if stock_factors:
            current_stock = {
                "per": stock_factors.per,
                "pbr": stock_factors.pbr,
                "roe": stock_factors.roe,
                "financial_stability_score": stock_factors.financial_stability_score,
                "price_stability_score": stock_factors.price_stability_score,
                "market_stability_score": stock_factors.market_stability_score,
                "sector_stability_score": stock_factors.sector_stability_score,
            }
        else:
            current_stock = None


        if not current_stock:
            return Indicators(
                per=None,
                industry_per=None,
                pbr=None,
                industry_pbr=None,
                roe=None,
                industry_roe=None,
                financial_data=None,
                price_trend=None,
                market_situation=None,
                industry_situation=None,
            )

        # 섹터 관련 데이터 계산
        sector_metrics = await self._calculate_sector_metrics(ticker, ctry, table_name, basic_columns)

        # 안정성 지표 상태 계산
        stability_statuses = {}

        for stability_type, info in STABILITY_INFO.items():
            if info.db_column not in current_stock:
                logger.error(f"Column {info.db_column} not found in current_stock")
                continue
            score = current_stock[info.db_column]
            status = self.get_stability_status(score, stability_type)
            stability_statuses[info.api_field] = status.value

        return Indicators(
            per=self.round_and_clean(current_stock["per"]) if current_stock["per"] is not None else None,
            industry_per=sector_metrics["per"] if sector_metrics["per"] is not None else None,
            pbr=self.round_and_clean(current_stock["pbr"]) if current_stock["pbr"] is not None else None,
            industry_pbr=sector_metrics["pbr"] if sector_metrics["pbr"] is not None else None,
            roe=self.round_and_clean(current_stock["roe"]) if current_stock["roe"] is not None else None,
            industry_roe=sector_metrics["roe"] if sector_metrics["roe"] is not None else None,
            **stability_statuses,
        )
def get_stock_info_service() -> StockInfoService:
    return StockInfoService()


if __name__ == "__main__":
    stock_info_service = get_stock_info_service()
    data = asyncio.run(stock_info_service.get_us_etf_info("ACES"))
    print(data)
