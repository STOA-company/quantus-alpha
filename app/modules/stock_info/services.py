import random
from typing import List, Tuple
from fastapi import HTTPException
import pandas as pd
from sqlalchemy import and_, func, select
from app.database.crud import database
from app.models.models_stock import StockInformation
from app.modules.common.enum import StabilityStatus
from app.modules.stock_info.mapping import STABILITY_INFO
from app.modules.stock_info.schemas import Indicators, SimilarStock, StockInfo
from app.core.logging.config import get_logger
from sqlalchemy.ext.asyncio import AsyncSession
from app.modules.common.utils import contry_mapping
from typing import Dict


logger = get_logger(__name__)


class StockInfoService:
    def __init__(self):
        self.db = database
        self.file_path = "static"
        self.file_name = "stock_{}_info.csv"

    async def get_stock_info(self, ctry: str, ticker: str) -> StockInfo:
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

            return StockInfo(
                introduction=intro_result.get("translated_overview" if ctry == "us" else "overview", ""),
                homepage_url=result.get("homepage_url", ""),
                ceo_name=result.get("ceo", ""),
                establishment_date=result.get("establishment_date", ""),
                listing_date=result.get("listing_date", ""),
            )

        except Exception as e:
            logger.error(f"Error in get_stock_info for {ticker}: {str(e)}")
            return StockInfo(introduction="", homepage_url="", ceo_name="", establishment_date="", listing_date="")

    def round_and_clean(self, value: float) -> float:
        """
        소수점 첫째자리에서 반올림하고, 소수점이 0이면 정수로 변환
        예: 15.7 -> 15.7, 15.0 -> 15
        """
        rounded = round(value, 1)
        return int(rounded) if rounded.is_integer() else rounded

    async def get_indicators(self, ctry: str, ticker: str) -> Indicators:
        """지표 조회"""
        if ctry == "us":
            ticker = f"{ticker}-US"

        # ctry 3자리 코드로 변환
        ctry_3 = contry_mapping[ctry]

        # 현재 종목의 지표 조회
        table_name = f"{ctry_3}_stock_factors"
        basic_columns = ["per", "pbr", "roe"]
        stability_columns = [info.db_column for info in STABILITY_INFO.values()]
        columns = basic_columns + stability_columns

        current_stock = self.db._select(
            table=table_name,
            columns=columns,
            **{"ticker": ticker},
        )

        if not current_stock:
            return Indicators(
                per=0,
                industry_per=0,
                pbr=0,
                industry_pbr=0,
                roe=0,
                industry_roe=0,
                financial_data="보통",
                price_trend="보통",
                market_situation="보통",
                industry_situation="보통",
            )

        # 섹터 관련 데이터 계산
        sector_metrics = await self._calculate_sector_metrics(ticker, ctry, table_name, basic_columns)

        # 안정성 지표 상태 계산
        stability_statuses = {}
        for stability_type, info in STABILITY_INFO.items():
            if ctry == "kr":
                score = getattr(current_stock[0], info.db_column)
                status = self.get_stability_status(score, stability_type)
            else:
                status = random.choice(list(StabilityStatus))
            stability_statuses[info.api_field] = status.value

        return Indicators(
            per=self.round_and_clean(current_stock[0].per),
            industry_per=sector_metrics["per"],
            pbr=self.round_and_clean(current_stock[0].pbr),
            industry_pbr=sector_metrics["pbr"],
            roe=self.round_and_clean(current_stock[0].roe),
            industry_roe=sector_metrics["roe"],
            **stability_statuses,
        )

    # 관련 섹터 조회
    async def get_related_sectors(self, ticker: str) -> List[str]:
        # 섹터 조회
        query = select(StockInformation.sector_2).where(StockInformation.ticker == ticker)
        result = self.db._execute(query)
        sector = result.scalars().first()

        # 관련 섹터의 ticker 조회
        query = select(StockInformation).where(StockInformation.sector_2 == sector)
        result = self.db._execute(query)
        related_sectors = result.scalars().all()

        return related_sectors

    async def get_similar_stocks(self, ctry: str, ticker: str, db: AsyncSession) -> List[SimilarStock]:
        """
        연관 종목 조회

        Args:
            ctry (str): 국가 코드
            ticker (str): 종목 코드

        Returns:
            List[SimilarStock]: 연관 종목 리스트
        """
        # ticker의 섹터 조회
        query = select(StockInformation).where(StockInformation.ticker == ticker)
        result = await db.execute(query)
        stock_info = result.scalars().first()

        if not stock_info:
            raise HTTPException(status_code=404, detail=f"Stock not found: {ticker}")

        sector = stock_info.sector_2

        # 같은 섹터의 다른 종목들을 랜덤하게 6개 조회
        query = (
            select(StockInformation)
            .where(and_(StockInformation.sector_2 == sector, StockInformation.ticker != ticker))
            .order_by(func.rand())
            .limit(6)
        )

        result = await db.execute(query)
        stocks = result.scalars().all()

        # 종목 SimilarStock 리스트 생성
        similar_stocks = []
        for stock in stocks:
            # 각 종목별로 현재가와 변동률 조회
            current_price, current_price_rate = await self.get_current_price(
                ticker=stock.ticker,  # 각 종목의 ticker 사용
                table_name=f"stock_{stock.ctry}_1d",  # 각 종목의 국가에 맞는 테이블 사용
            )

            similar_stocks.append(
                SimilarStock(
                    ticker=stock.ticker,
                    name=stock.kr_name,
                    ctry=stock.ctry,
                    current_price=current_price,
                    current_price_rate=current_price_rate,
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
        """섹터 관련 지표 계산"""
        sector_ticker = ticker.replace("-US", "")
        sector_tickers = await self.get_related_sectors(sector_ticker)

        if ctry == "us":
            sector_tickers = [f"{t}-US" for t in sector_tickers]

        if not sector_tickers:
            return {metric: 0 for metric in columns}

        sector_results = self.db._select(table=table_name, columns=columns, **{"ticker__in": sector_tickers})

        if not sector_results:
            return {metric: 0 for metric in columns}

        # 섹터 평균 계산
        sector_metrics = {}
        for metric in columns:
            values = [getattr(stock, metric) for stock in sector_results if getattr(stock, metric)]
            if values:
                sector_metrics[metric] = self.round_and_clean(sum(values) / len(values))
            else:
                sector_metrics[metric] = 0

        return sector_metrics


def get_stock_info_service() -> StockInfoService:
    return StockInfoService()
