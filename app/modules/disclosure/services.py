import json
from datetime import datetime
from typing import Dict

import numpy as np
import pandas as pd
from app.core.exception.custom import DataNotFoundException
from app.database.crud import JoinInfo, database
from app.core.logging.config import get_logger
from app.modules.common.enum import TranslateCountry
from app.modules.common.utils import check_ticker_country_len_3
from app.utils.ctry_utils import check_ticker_country_len_2
from .mapping import document_type_mapping


logger = get_logger(__name__)


class DisclosureService:
    def __init__(self):
        self.db = database

    async def get_disclosure(
        self, ticker: str, year: str = None, language: TranslateCountry = "ko", page: int = 1, size: int = 6
    ):
        if not year:
            year = datetime.now().strftime("%Y")

        ctry = "usa"
        if ticker:
            ctry = check_ticker_country_len_3(ticker)

        if ctry != "usa":
            raise DataNotFoundException(ticker=ctry, data_type="공시")

        table_name = f"{ctry}_disclosure"

        conditions = {}
        if ticker:
            conditions["ticker"] = ticker
        if year:
            conditions["filing_date__like"] = f"{year}%"
        conditions["ai_processed"] = 1

        columns = ["filing_id", "form_type", "ticker", "url", "filing_date", "company_name", "ai_processed"]
        offset = (page - 1) * size
        total_count = self.db._count(table=table_name, **conditions)

        results = self.db._select(
            table=table_name,
            columns=columns,
            order="filing_date",
            ascending=False,
            limit=size,
            offset=offset,
            **conditions,
        )
        if not results:
            raise DataNotFoundException(ticker=ticker, data_type="공시")

        analysis_table_name = f"{ctry}_disclosure_analysis"
        analysis_conditions = {}
        analysis_conditions["filing_id__in"] = [result.filing_id for result in results]
        analysis_columns = ["filing_id", "ai_summary", "market_impact", "impact_reason", "key_points", "translated"]

        analysis_results = self.db._select(table=analysis_table_name, columns=analysis_columns, **analysis_conditions)

        translated_filing_ids = [result.filing_id for result in analysis_results if result.translated]
        translated_table_name = f"{analysis_table_name}_translation"
        translated_columns = ["filing_id", "ai_summary", "impact_reason", "key_points"]
        translated_conditions = {"filing_id__in": translated_filing_ids}

        translated_results = self.db._select(
            table=translated_table_name, columns=translated_columns, **translated_conditions
        )

        # emotion 카운트 초기화
        emotion_counts = {"positive": 0, "negative": 0, "neutral": 0}

        if not ticker:
            results_ticker = [result.ticker for result in results]
            ctry = "us"
            ticker_dict = {}

            table_name = f"stock_{ctry}_1d"
            columns = ["Date", "Ticker", "Open", "Close"]
            join_columns = ["korean_name", "english_name"]
            # join_info 설정
            join_info = JoinInfo(
                primary_table=table_name,  # 메인 테이블 (stock_us_1d)
                secondary_table="stock_us_tickers",  # 조인할 테이블
                primary_column="Ticker",  # 메인 테이블의 조인 컬럼
                secondary_column="ticker",  # stock_us_tickers의 조인 컬럼
                columns=join_columns,  # 조인 테이블에서 가져올 컬럼
            )
            max_date = self.db._select(table=table_name, columns=["Date"], order="Date", ascending=False, limit=1)
            conditions = {"Ticker__in": results_ticker, "Date": max_date[0][0].strftime("%Y-%m-%d")}

            stock_results = self.db._select(
                table=table_name, columns=columns + join_columns, join_info=join_info, **conditions
            )
            if stock_results:
                ticker_dict = {result.Ticker: result for result in stock_results}
        analysis_dict = {result.filing_id: result for result in analysis_results}
        translated_dict = {result.filing_id: result for result in translated_results}

        items = []
        for row in results:
            filing_date = getattr(row, "filing_date", None)
            date_str = filing_date.strftime("%Y-%m-%d %H:%M:%S") if filing_date else None

            analysis_data = analysis_dict.get(row.filing_id)
            translated_data = translated_dict.get(row.filing_id)
            if not ticker:
                ticker_data = ticker_dict.get(row.ticker)
            # emotion 카운트 업데이트
            emotion = analysis_data.market_impact.lower() if analysis_data and analysis_data.market_impact else "neutral"
            emotion_counts[emotion] += 1

            # key_points 파싱
            key_points_list = [None] * 5
            if analysis_data and analysis_data.key_points:
                try:
                    # translated가 True이면 translated_data의 key_points 사용
                    key_points = (
                        translated_data.key_points
                        if analysis_data.translated and translated_data
                        else analysis_data.key_points
                    )

                    # 이미 리스트인 경우와 문자열인 경우를 모두 처리
                    if isinstance(key_points, str):
                        key_points = json.loads(key_points)

                    # 최대 5개의 key points 저장
                    for i, point in enumerate(key_points[:5]):
                        key_points_list[i] = point

                except Exception as e:
                    logger.error(f"Failed to parse key_points for filing_id: {row.filing_id}, error: {str(e)}")

            if not ticker:
                price_change = (
                    round((ticker_data.Close - ticker_data.Open) / ticker_data.Open * 100, 2) if ticker_data else None
                )
                name = ticker_data.korean_name if language == TranslateCountry.KO else ticker_data.english_name

            items.append(
                {
                    "title": row.company_name + " " + document_type_mapping.get(row.form_type, row.form_type),
                    "date": date_str,
                    "emotion": analysis_data.market_impact.lower() if analysis_data else None,
                    "impact_reason": translated_data.impact_reason
                    if analysis_data.translated
                    else analysis_data.impact_reason,
                    "key_points_1": key_points_list[0],
                    "key_points_2": key_points_list[1],
                    "key_points_3": key_points_list[2],
                    "key_points_4": key_points_list[3],
                    "key_points_5": key_points_list[4],
                    "summary": translated_data.ai_summary if analysis_data.translated else analysis_data.ai_summary,
                    "document_url": row.url,
                    "name": name if not ticker else None,
                    "price_change": price_change if not ticker else None,
                }
            )

        return {
            "data": items,
            "total_count": total_count,
            "total_pages": (total_count + size - 1) // size,
            "current_page": page,
            "offset": offset,
            "size": size,
            "positive_count": emotion_counts["positive"],
            "negative_count": emotion_counts["negative"],
            "neutral_count": emotion_counts["neutral"],
            "name": "None",
            "price_change": None,
        }

    @staticmethod
    def _process_dataframe_disclosure(df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 전처리 및 필터링"""

        df = df.dropna(subset=["emotion"]).sort_values(by=["date"], ascending=[False])

        df["emotion"] = np.where(
            df["emotion"] == "POSITIVE",
            "positive",
            np.where(
                df["emotion"] == "NEGATIVE", "negative", np.where(df["emotion"] == "NEUTRAL", "neutral", df["emotion"])
            ),
        )
        df["ctry"] = np.where(df["ctry"] == "KR", "kr", np.where(df["ctry"] == "US", "us", df["ctry"]))

        return df

    @staticmethod
    def _count_emotions(df: pd.DataFrame) -> Dict[str, int]:
        """감정 분석 결과 카운트"""
        emotion_counts = df["emotion"].value_counts()
        return {
            "positive_count": int(emotion_counts.get("positive", 0)),
            "negative_count": int(emotion_counts.get("negative", 0)),
            "neutral_count": int(emotion_counts.get("neutral", 0)),
        }

    async def renewal_disclosure(self, ticker: str, date: str, page: int, size: int):
        if not date:
            year = datetime.now().strftime("%Y")
        elif len(date) == 8:
            year = date[:4]
        else:
            year = date

        ctry = check_ticker_country_len_2(ticker)

        df_disclosure = pd.DataFrame(
            self.db._select(
                table="disclosure_information",
                columns=[
                    "id",
                    "ticker",
                    "ko_name",
                    "en_name",
                    "ctry",
                    "date",
                    "url",
                    "summary",
                    "impact_reason",
                    "key_points",
                    "emotion",
                    "form_type",
                    "that_time_price",
                ],
                **{"ticker": ticker, "date__like": f"{year}%"},
            )
        )

        if df_disclosure.empty:
            raise DataNotFoundException(ticker=ticker, data_type="disclosure")

        df_disclosure = self._process_dataframe_disclosure(df_disclosure)

        current_price = self.db._select(table="stock_trend", columns=["ticker", "current_price"], **{"ticker": ticker})
        df_disclosure["price_impact"] = 0.00
        if current_price:
            df_disclosure["price_impact"] = round(
                (df_disclosure["that_time_price"] - current_price[0][1]) / current_price[0][1] * 100, 2
            )

        total_count = len(df_disclosure)
        total_pages = (total_count + size - 1) // size
        offset = (page - 1) * size
        emotion_counts = self._count_emotions(df_disclosure)

        # key_points 파싱
        df_disclosure["key_points"] = df_disclosure["key_points"].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )

        data = []

        for _, row in df_disclosure.iterrows():
            form_type = (
                document_type_mapping.get(row["form_type"], row["form_type"]) if ctry == "us" else row["form_type"]
            )
            data.append(
                {
                    "title": row["ko_name"] + " " + form_type,
                    "date": row["date"],
                    "emotion": row["emotion"],
                    "impact_reason": row["impact_reason"],
                    "key_points_1": row["key_points"][0],
                    "key_points_2": row["key_points"][1],
                    "key_points_3": row["key_points"][2],
                    "key_points_4": row["key_points"][3],
                    "key_points_5": row["key_points"][4],
                    "summary": row["summary"],
                    "document_url": row["url"],
                    "price_impact": row["price_impact"],
                }
            )

        return data, total_count, total_pages, offset, emotion_counts


def get_disclosure_service() -> DisclosureService:
    return DisclosureService()
