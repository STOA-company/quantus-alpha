import json
from datetime import datetime, timedelta
from typing import Dict

import pandas as pd
from app.common.constants import KST
from app.core.exception.custom import DataNotFoundException
from app.database.crud import JoinInfo, database
from app.core.logging.config import get_logger
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from app.modules.common.utils import check_ticker_country_len_2, check_ticker_country_len_3
from app.utils.date_utils import now_kr
from .mapping import CATEGORY_TYPE_MAPPING_EN, DOCUMENT_TYPE_MAPPING, DOCUMENT_TYPE_MAPPING_EN, FORM_TYPE_MAPPING


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
                    "title": row.company_name + " " + DOCUMENT_TYPE_MAPPING.get(row.form_type, row.form_type),
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

        df["emotion"] = df["emotion"].str.lower()
        df["ctry"] = df["ctry"].str.lower()

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

    async def renewal_disclosure(
        self, ticker: str, date: str, page: int, size: int, lang: TranslateCountry, user: AlphafinderUser
    ):
        if not date:
            year = datetime.now().strftime("%Y")
        elif len(date) == 8:
            year = date[:4]
        else:
            year = date

        ctry = check_ticker_country_len_2(ticker)

        if lang == TranslateCountry.KO:
            name = "ko_name"
            lang = "ko-KR"
            document_type_mapping = DOCUMENT_TYPE_MAPPING

            def category_type_mapping(x):
                return x
        elif lang == TranslateCountry.EN:
            name = "en_name"
            document_type_mapping = FORM_TYPE_MAPPING if ctry == "kr" else DOCUMENT_TYPE_MAPPING_EN
            lang = "en-US"

            def category_type_mapping(x):
                return CATEGORY_TYPE_MAPPING_EN.get(x, x)
        else:
            raise ValueError("Invalid language")

        df_disclosure = pd.DataFrame(
            self.db._select(
                table="disclosure_information",
                columns=[
                    "id",
                    "ticker",
                    name,
                    "ctry",
                    "date",
                    "url",
                    "summary",
                    "impact_reason",
                    "key_points",
                    "emotion",
                    "form_type",
                    "category_type",
                    "that_time_price",
                ],
                **{"ticker": ticker, "date__like": f"{year}%", "lang": lang, "is_exist": 1},
            )
        )

        if df_disclosure.empty:
            raise DataNotFoundException(ticker=ticker, data_type="disclosure")

        df_disclosure = self._process_dataframe_disclosure(df_disclosure)
        df_disclosure["date"] = pd.to_datetime(df_disclosure["date"]).dt.tz_localize("UTC").dt.tz_convert("Asia/Seoul")

        # current_price = self.db._select(table="stock_trend", columns=["ticker", "current_price"], **{"ticker": ticker})
        df_disclosure["price_impact"] = 0.00
        # if current_price:
        #     df_disclosure["price_impact"] = round(
        #         (df_disclosure["that_time_price"] - current_price[0][1]) / current_price[0][1] * 100, 2
        #     )

        total_count = len(df_disclosure)
        total_pages = (total_count + size - 1) // size
        offset = (page - 1) * size
        emotion_counts = self._count_emotions(df_disclosure)
        df_disclosure = df_disclosure[offset : offset + size]

        if offset >= total_count:
            page = total_pages
            offset = (page - 1) * size
            df_disclosure = df_disclosure[offset : offset + size]

        # key_points 파싱
        df_disclosure["key_points"] = df_disclosure["key_points"].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )

        # 권한에 따른 데이터 마스킹
        user_level = user.subscription_level if user else 1
        if user_level == 1:
            df_disclosure = self.mask_fields_disclosure(df_disclosure)

        data = []
        for _, row in df_disclosure.iterrows():
            form_type = document_type_mapping.get(row["form_type"], row["form_type"])
            res_name = row.get(name, "") or ""
            category_type = (
                "[" + category_type_mapping(row["category_type"]) + "]" if row.get("category_type", "") else ""
            )
            data.append(
                {
                    "id": row["id"],
                    "title": f"{res_name} {form_type} {category_type}".strip(),
                    "date": row["date"],
                    "emotion": row["emotion"],
                    "impact_reason": row["impact_reason"],
                    "key_points_1": row["key_points"][0] if row["key_points"] else "",
                    "key_points_2": row["key_points"][1] if row["key_points"] else "",
                    "key_points_3": row["key_points"][2] if row["key_points"] else "",
                    "key_points_4": row["key_points"][3] if row["key_points"] else "",
                    "key_points_5": row["key_points"][4] if row["key_points"] else "",
                    "summary": row["summary"],
                    "document_url": row["url"],
                    "price_impact": row["price_impact"],
                }
            )

        return data, total_count, total_pages, offset, emotion_counts

    def mask_fields_disclosure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        사용자 레벨에 따라 민감 정보 필드를 필터링하는 함수

        Parameters:
        - df: 필터링할 데이터프레임

        Returns:
        - 필터링된 데이터프레임
        """

        # 필터링할 필드 리스트
        fields_to_filter = ["impact_reason", "key_points"]

        # df에 date 컬럼이 없거나 비어있는 경우 처리
        if "date" not in df.columns or df.empty:
            return df

        try:
            # 날짜 컬럼의 타임존 정보 확인
            if pd.api.types.is_datetime64tz_dtype(df["date"]):
                # 타임존이 있는 경우
                # 기준 날짜 계산 (6시간 전) 타임존 적용
                cutoff_date = pd.Timestamp(now_kr() - timedelta(days=7)).tz_localize(KST)
            else:
                # 타임존이 없는 경우
                cutoff_date = pd.Timestamp(now_kr() - timedelta(days=7))

            # 마스크 생성 - 7일보다 오래된 데이터 필터링 (최근 7일 데이터는 유지)
            old_data_mask = df["date"] < cutoff_date

            # 마스크 적용 - 오래된 데이터의 필드 비우기
            if old_data_mask.any():
                for field in fields_to_filter:
                    if field in df.columns:
                        df.loc[old_data_mask, field] = ""

        except Exception as e:
            # 오류 로깅
            import logging

            logging.error(f"Error in filter_sensitive_fields_by_user_level: {str(e)}")

        return df


def get_disclosure_service() -> DisclosureService:
    return DisclosureService()
