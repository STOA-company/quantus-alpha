import math
import re
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pytz
from fastapi import Request, Response
from sqlalchemy import text

from app.cache.leaderboard import DisclosureLeaderboard, NewsLeaderboard
from app.cache.story_view import get_story_view_cache
from app.common.constants import KST, UTC
from app.core.exception.custom import DataNotFoundException
from app.database.crud import JoinInfo, database
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from app.modules.disclosure.mapping import (
    CATEGORY_TYPE_MAPPING_EN,
    DOCUMENT_TYPE_MAPPING,
    DOCUMENT_TYPE_MAPPING_EN,
    FORM_TYPE_MAPPING,
)
from app.modules.news.schemas import (
    DisclosureRenewalItem,
    NewsDetailItem,
    NewsDetailItemV2,
    NewsRenewalItem,
    TopStoriesItem,
    TopStoriesResponse,
)
from app.utils.ctry_utils import check_ticker_country_len_2
from app.utils.date_utils import now_utc


class NewsService:
    def __init__(self):
        self.db = database

    @staticmethod
    def _convert_to_kst(df: pd.DataFrame) -> pd.DataFrame:
        dates = pd.to_datetime(df["date"])
        if dates.dt.tz is None:
            df["date"] = dates.dt.tz_localize(UTC).dt.tz_convert(KST)
        else:
            df["date"] = dates.dt.tz_convert(KST)
        return df

    @staticmethod
    def _process_dataframe_news(df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 전처리 및 필터링"""

        df = df.dropna(subset=["emotion"]).sort_values(by=["date"], ascending=[False])
        df = df[df["title"].str.strip() != ""]  # titles가 "" 인 경우 행 삭제
        df = NewsService._convert_to_kst(df)

        df["emotion"] = df["emotion"].str.lower()
        df["ctry"] = np.where(df["ctry"] == "KR", "kr", np.where(df["ctry"] == "US", "us", df["ctry"]))

        # summary 필드 전처리
        df["summary"] = (
            df["summary"]
            .str.replace(r'[\[\]"]', "", regex=True)  # 대괄호와 따옴표 제거
            .str.replace(r"\n", " ", regex=True)  # 줄바꿈을 공백으로 변환
            .str.replace(r"\*\*기사 요약\*\*\s*:\s*", "", regex=True)  # "**기사 요약**:" 제거
            .str.replace(
                r"\*\*주가에 영향을 줄 만한 내용\*\*\s*:\s*", "", regex=True
            )  # "**주가에 영향을 줄 만한 내용**:" 제거
            .str.replace(r"\s+\*\*뉴스 감성분석\*\*.*$", "", regex=True)  # 뉴스 감성분석 이후 내용 제거
            .str.replace(r". -", ".-", regex=True)
        )

        return df

    @staticmethod
    def _process_dataframe_news_detail(df: pd.DataFrame, offset: int, size: int) -> pd.DataFrame:
        """DataFrame 전처리 및 필터링"""

        df = df.dropna(subset=["emotion"]).sort_values(by=["date"], ascending=[False])

        df = df.iloc[offset : offset + size]

        df["emotion"] = np.where(
            df["emotion"] == "긍정",
            "positive",
            np.where(df["emotion"] == "부정", "negative", np.where(df["emotion"] == "중립", "neutral", df["emotion"])),
        )
        df["ctry"] = np.where(df["ctry"] == "KR", "kr", np.where(df["ctry"] == "US", "us", df["ctry"]))

        # # summary 필드 전처리 # TODO :: 이 부분을 사용하게 된다면 이 메서드를 지우고 `_process_dataframe_news`를 사용하면 됨. 파싱을 현재 프론트에서 처리하고 있음
        # df["summary"] = (
        #     df["summary"]
        #     .str.replace(r'[\[\]"]', "", regex=True)  # 대괄호와 따옴표 제거
        #     .str.replace(r"\n", " ", regex=True)  # 줄바꿈을 공백으로 변환
        #     .str.replace(r"\*\*기사 요약\*\*\s*:\s*", "", regex=True)  # "**기사 요약**:" 제거
        #     .str.replace(
        #         r"\*\*주가에 영향을 줄 만한 내용\*\*\s*:\s*", "", regex=True
        #     )  # "**주가에 영향을 줄 만한 내용**:" 제거
        #     .str.strip()  # 앞뒤 공백 제거
        # )

        return df

    @staticmethod
    def _count_emotion(df: pd.DataFrame) -> dict:
        return df["emotion"].value_counts().to_dict()

    @staticmethod
    def _process_dataframe_disclosure(df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 전처리 및 필터링"""
        df = df.dropna(subset=["emotion"]).sort_values(by=["date"], ascending=[False])
        df = NewsService._convert_to_kst(df)

        df["emotion"] = df["emotion"].str.lower()
        df["ctry"] = np.where(df["ctry"] == "KR", "kr", np.where(df["ctry"] == "US", "us", df["ctry"]))

        return df

    async def get_renewal_data(
        self, ctry: str = None, lang: TranslateCountry | None = None, tickers: Optional[List[str]] = None
    ) -> Tuple[List[NewsRenewalItem], List[DisclosureRenewalItem]]:
        news_data = self.get_news(ctry=ctry, lang=lang, tickers=tickers)
        disclosure_data = self.get_disclosure(ctry=ctry, lang=lang, tickers=tickers)

        return news_data, disclosure_data

    async def get_news(
        self, ctry: str = None, lang: TranslateCountry | None = None, tickers: Optional[List[str]] = None
    ) -> List[NewsRenewalItem]:
        if lang is None:
            lang = TranslateCountry.KO

        condition = {"is_exist": True}
        if tickers:
            condition["ticker__in"] = tickers
        if ctry:
            condition["ctry"] = "KR" if ctry == "kr" else "US" if ctry == "us" else None

        if lang == TranslateCountry.KO:
            condition["lang"] = "ko-KR"
            news_name = "kr_name"
        else:
            condition["lang"] = "en-US"
            news_name = "en_name"

        news_condition = condition.copy()
        news_condition["is_related"] = True

        # 현재 시간에 5분을 더한 시간까지 허용
        current_time = datetime.now(UTC)
        allowed_time = current_time + timedelta(minutes=5)
        news_condition["date__lte"] = allowed_time

        change_rate_column = "change_rt"

        join_info = lambda table: JoinInfo(  # noqa: E731
            primary_table=table,
            secondary_table="stock_trend",
            primary_column="ticker",
            secondary_column="ticker",
            columns=["current_price", change_rate_column],
            is_outer=True,
        )

        df_news = pd.DataFrame(
            await self.db._select_async(
                table="news_analysis",
                columns=[
                    "id",
                    "ticker",
                    news_name,
                    "ctry",
                    "date",
                    "title",
                    "summary",
                    "impact_reason",
                    "key_points",
                    "emotion",
                    "that_time_price",
                    "current_price",
                    "is_related",
                    change_rate_column,
                ],
                order="date",
                ascending=False,
                limit=100,
                join_info=join_info("news_analysis"),
                **news_condition,
            )
        )

        news_data = [] if df_news.empty else self._process_price_data(df=self._process_dataframe_news(df_news), lang=lang)

        return news_data

    async def get_disclosure(
        self, ctry: str = None, lang: TranslateCountry | None = None, tickers: Optional[List[str]] = None
    ) -> List[DisclosureRenewalItem]:
        if lang is None:
            lang = TranslateCountry.KO

        condition = {"is_exist": True}
        if tickers:
            condition["ticker__in"] = tickers
        if ctry:
            condition["ctry"] = "KR" if ctry == "kr" else "US" if ctry == "us" else None

        if lang == TranslateCountry.KO:
            condition["lang"] = "ko-KR"
            disclosure_name = "ko_name"
        else:
            condition["lang"] = "en-US"
            disclosure_name = "en_name"

        disclosure_condition = condition.copy()

        # 현재 시간에 5분을 더한 시간까지 허용
        current_time = datetime.now(UTC)
        allowed_time = current_time + timedelta(minutes=5)
        disclosure_condition["date__lte"] = allowed_time

        change_rate_column = "change_rt"

        join_info = lambda table: JoinInfo(  # noqa: E731
            primary_table=table,
            secondary_table="stock_trend",
            primary_column="ticker",
            secondary_column="ticker",
            columns=["current_price", change_rate_column],
            is_outer=True,
        )

        df_disclosure = pd.DataFrame(
            await self.db._select_async(
                table="disclosure_information",
                columns=[
                    "id",
                    "ticker",
                    disclosure_name,
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
                    "current_price",
                    change_rate_column,
                ],
                order="date",
                ascending=False,
                limit=100,
                join_info=join_info("disclosure_information"),
                **disclosure_condition,
            )
        )

        disclosure_data = (
            []
            if df_disclosure.empty
            else self._process_price_data(
                self._process_dataframe_disclosure(df_disclosure), lang=lang, is_disclosure=True
            )
        )

        return disclosure_data

    def get_news_by_id(self, news_id: int | List[int], lang: TranslateCountry | None = None) -> Optional[NewsRenewalItem]:
        if lang is None:
            lang = TranslateCountry.KO

        condition = {"is_exist": True}

        if lang == TranslateCountry.KO:
            condition["lang"] = "ko-KR"
            news_name = "kr_name"
        else:
            condition["lang"] = "en-US"
            news_name = "en_name"

        if isinstance(news_id, list):
            condition["id__in"] = news_id
        else:
            condition["id"] = news_id

        change_rate_column = "change_rt"

        join_info = JoinInfo(  # noqa: E731
            primary_table="news_analysis",
            secondary_table="stock_trend",
            primary_column="ticker",
            secondary_column="ticker",
            columns=["current_price", change_rate_column],
            is_outer=True,
        )

        df_news = pd.DataFrame(
            self.db._select(
                table="news_analysis",
                columns=[
                    "id",
                    "ticker",
                    news_name,
                    "ctry",
                    "date",
                    "title",
                    "summary",
                    "impact_reason",
                    "key_points",
                    "emotion",
                    "that_time_price",
                    "current_price",
                    "is_related",
                    change_rate_column,
                ],
                join_info=join_info,
                **condition,
            )
        )

        if df_news.empty:
            return None
        processed_df = self._process_dataframe_news(df_news)
        news_items = self._process_price_data(df=processed_df, lang=lang)

        return news_items

    def get_disclosure_by_id(
        self, disclosure_id: int | List[int], lang: TranslateCountry | None = None
    ) -> Optional[DisclosureRenewalItem]:
        if lang is None:
            lang = TranslateCountry.KO

        condition = {"is_exist": True}

        if lang == TranslateCountry.KO:
            condition["lang"] = "ko-KR"
            disclosure_name = "ko_name"
        else:
            condition["lang"] = "en-US"
            disclosure_name = "en_name"

        if isinstance(disclosure_id, list):
            condition["id__in"] = disclosure_id
        else:
            condition["id"] = disclosure_id

        change_rate_column = "change_rt"

        join_info = JoinInfo(  # noqa: E731
            primary_table="disclosure_information",
            secondary_table="stock_trend",
            primary_column="ticker",
            secondary_column="ticker",
            columns=["current_price", change_rate_column],
            is_outer=True,
        )

        df_disclosure = pd.DataFrame(
            self.db._select(
                table="disclosure_information",
                columns=[
                    "id",
                    "ticker",
                    disclosure_name,
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
                    "current_price",
                    change_rate_column,
                ],
                join_info=join_info,
                **condition,
            )
        )

        if df_disclosure.empty:
            return None

        processed_df = self._process_dataframe_disclosure(df_disclosure)
        disclosure_items = self._process_price_data(processed_df, lang=lang, is_disclosure=True)

        return disclosure_items

    def _process_price_data(
        self, df: pd.DataFrame, lang: TranslateCountry, is_disclosure: bool = False
    ) -> List[Union[NewsRenewalItem, DisclosureRenewalItem]]:
        if df.empty:
            return []

        change_rate_column = "change_rt" if "change_rt" in df.columns else "change_1d"

        numeric_columns = ["that_time_price", "current_price", change_rate_column]
        df[numeric_columns] = df[numeric_columns].astype("float64").fillna(0)

        mask = (df["current_price"] == 0) & (df["that_time_price"] != 0)
        df.loc[mask, "that_time_price"] = 0

        df["price_impact"] = np.where(
            df["that_time_price"] != 0, (df["current_price"] - df["that_time_price"]) / df["that_time_price"] * 100, 0
        )

        df["price_impact"] = df["price_impact"].round(2).fillna(0)
        df["change_rate"] = df[change_rate_column].round(2)

        if is_disclosure:
            if lang == TranslateCountry.KO:
                document_type_mapping = DOCUMENT_TYPE_MAPPING
                name = "ko_name"

                def category_type_mapping(x):
                    return x

                def form_type_mapping(x):
                    return x

            elif lang == TranslateCountry.EN:
                document_type_mapping = DOCUMENT_TYPE_MAPPING_EN
                name = "en_name"

                def category_type_mapping(x):
                    return CATEGORY_TYPE_MAPPING_EN.get(x, x)

                def form_type_mapping(x):
                    return FORM_TYPE_MAPPING.get(x.strip().split()[0], x)

            result = []
            for _, row in df.iterrows():
                form_type = (
                    form_type_mapping(row["form_type"])
                    if row["ctry"] == "kr"
                    else document_type_mapping.get(row["form_type"], row["form_type"])
                )
                category = "[" + category_type_mapping(row["category_type"]) + "]" if row.get("category_type", "") else ""

                result.append(
                    DisclosureRenewalItem(
                        id=row["id"],
                        date=row["date"],
                        ctry=row["ctry"],
                        ticker=row["ticker"],
                        title=f"{row[name]} {form_type} {category}".strip(),
                        summary=row["summary"],
                        impact_reason=row["impact_reason"],
                        key_points=row["key_points"],
                        emotion=row["emotion"],
                        name=row[name],
                        change_rate=row["change_rate"],
                        price_impact=row["price_impact"],
                        document_url=row["url"],
                    )
                )
            return result

        news_name = "kr_name" if lang == TranslateCountry.KO else "en_name"
        return [
            NewsRenewalItem(
                id=row["id"],
                date=row["date"],
                ctry=row["ctry"],
                title=row["title"],
                summary=row["summary"],
                impact_reason=row["impact_reason"],
                key_points=row["key_points"],
                emotion=row["emotion"],
                name=row[news_name],
                change_rate=row["change_rate"],
                price_impact=row["price_impact"],
                ticker=row["ticker"],
            )
            for _, row in df.iterrows()
        ]

    async def top_stories(
        self,
        request: Request,
        tickers: Optional[List[str]] = None,
        lang: TranslateCountry | None = None,
        stories_count: int = 30,
        user: Optional[AlphafinderUser] = None,
    ):
        if lang is None:
            lang = TranslateCountry.KO

        current_datetime = now_utc()
        before_24_hours = current_datetime - timedelta(hours=24)
        allowed_time = current_datetime + timedelta(minutes=5)

        if tickers:
            top_stories_tickers = tickers

            # 해당 티커의 가격 데이터 조회
            price_data = await self.db._select_async(
                table="stock_trend", columns=["ticker", "current_price", "change_rt", "ctry"], ticker__in=tickers
            )

            ticker_to_price_data = {}
            for row in price_data:
                ticker = row[0]
                ticker_to_price_data[ticker] = {"current_price": row[1], "change_rt": row[2]}
        else:
            query_us = f"""
                SELECT st.ticker, st.volume_change_rt, st.current_price, st.change_rt
                FROM stock_trend st
                JOIN (
                    SELECT DISTINCT ticker
                    FROM news_analysis
                    WHERE date >= '{before_24_hours}'
                    AND date <= '{allowed_time}'
                    AND is_related = TRUE
                    AND is_exist = TRUE
                ) na ON st.ticker = na.ticker
                WHERE ctry = 'US'
                ORDER BY st.volume_change_rt DESC
                LIMIT 6
            """
            top_stories_data_us = await self.db._execute_async(text(query_us))
            query_kr = f"""
                SELECT st.ticker, st.volume_change_rt, st.current_price, st.change_rt
                FROM stock_trend st
                JOIN (
                    SELECT DISTINCT ticker
                    FROM news_analysis
                    WHERE date >= '{before_24_hours}'
                    AND date <= '{allowed_time}'
                    AND is_related = TRUE
                    AND is_exist = TRUE
                ) na ON st.ticker = na.ticker
                WHERE ctry = 'KR'
                ORDER BY st.volume_change_rt DESC
                LIMIT 5
            """
            top_stories_data_kr = await self.db._execute_async(text(query_kr))

            # 티커 및 관련 데이터 추출
            top_stories_tickers = []
            ticker_to_price_data = {}

            for row in top_stories_data_us:
                ticker = row[0]
                if ticker not in top_stories_tickers:
                    top_stories_tickers.append(ticker)
                    ticker_to_price_data[ticker] = {"current_price": row[2], "change_rt": row[3]}

            for row in top_stories_data_kr:
                ticker = row[0]
                if ticker not in top_stories_tickers:
                    top_stories_tickers.append(ticker)
                    ticker_to_price_data[ticker] = {"current_price": row[2], "change_rt": row[3]}

        if not top_stories_tickers:
            return []  # 빠른 반환

        condition = {
            "is_exist": True,
            "date__gte": before_24_hours,
            "date__lte": allowed_time,
            "ticker__in": top_stories_tickers,
        }

        news_condition = condition.copy()
        disclosure_condition = condition.copy()

        news_condition["is_related"] = True
        if lang == TranslateCountry.KO:
            # 뉴스 데이터
            news_condition["lang"] = "ko-KR"
            disclosure_condition["lang"] = "ko-KR"

            news_name = "kr_name"
            disclosure_name = "ko_name"
        else:
            # 뉴스 데이터
            news_condition["lang"] = "en-US"
            disclosure_condition["lang"] = "en-US"

            news_name = "en_name"
            disclosure_name = "en_name"

        # 뉴스 데이터 수집
        df_news = pd.DataFrame(
            await self.db._select_async(
                table="news_analysis",
                columns=[
                    "id",
                    "ticker",
                    news_name,
                    "ctry",
                    "date",
                    "title",
                    "summary",
                    "impact_reason",
                    "key_points",
                    "emotion",
                    "that_time_price",
                ],
                order="date",
                ascending=False,
                **news_condition,
            )
        )
        if not df_news.empty:
            df_news = self._process_dataframe_news(df_news)
            df_news["type"] = "news"
            if lang == TranslateCountry.KO:
                df_news = df_news.rename(columns={"kr_name": "ko_name"})

        # 공시 데이터 수집
        df_disclosure = pd.DataFrame(
            await self.db._select_async(
                table="disclosure_information",
                columns=[
                    "id",
                    "ticker",
                    disclosure_name,
                    "ctry",
                    "date",
                    "summary",
                    "impact_reason",
                    "key_points",
                    "emotion",
                    "form_type",
                    "that_time_price",
                ],
                order="date",
                ascending=False,
                **disclosure_condition,
            )
        )
        if not df_disclosure.empty:
            if lang != TranslateCountry.KO:
                df_disclosure = df_disclosure.rename(
                    columns={"en_summary": "summary", "en_impact_reason": "impact_reason", "en_key_points": "key_points"}
                )
            df_disclosure = self._process_dataframe_disclosure(df_disclosure)

            # 공시의 form_type 매핑 로직 수정
            # 각 행에 적용할 함수 정의
            def get_form_type_mapping(row):
                ctry = row["ctry"]

                # 조건에 따라 적절한 매핑 선택
                if ctry == "kr":
                    if lang == TranslateCountry.KO:
                        # 한국 공시, 한국어: 원본 그대로 사용
                        return row["form_type"]
                    else:
                        # 한국 공시, 영어: DOCUMENT_TYPE_MAPPING_EN 사용
                        return FORM_TYPE_MAPPING.get(row["form_type"].strip().split()[0], row["form_type"])
                else:  # ctry == "us"
                    if lang == TranslateCountry.KO:
                        # 미국 공시, 한국어: DOCUMENT_TYPE_MAPPING 사용
                        return DOCUMENT_TYPE_MAPPING.get(row["form_type"], row["form_type"])
                    else:
                        # 미국 공시, 영어: DOCUMENT_TYPE_MAPPING_EN 사용
                        return DOCUMENT_TYPE_MAPPING_EN.get(row["form_type"], row["form_type"])

            # 각 행에 함수 적용하여 매핑된 form_type 생성
            df_disclosure["mapped_form_type"] = df_disclosure.apply(get_form_type_mapping, axis=1)

            # 제목 생성에 매핑된 form_type 사용
            df_disclosure["title"] = df_disclosure[disclosure_name] + " " + df_disclosure["mapped_form_type"]

            df_disclosure.drop(columns=["form_type", "mapped_form_type"], inplace=True)
            df_disclosure["type"] = "disclosure"

        # 데이터 통합 및 정렬
        total_df = pd.DataFrame()
        if not df_news.empty and not df_disclosure.empty:
            total_df = pd.concat([df_news, df_disclosure], ignore_index=True)
            total_df = total_df.sort_values(by=["date"], ascending=[False])
        elif not df_news.empty:
            total_df = df_news
        elif not df_disclosure.empty:
            total_df = df_disclosure

        if total_df.empty:
            return []
        unique_tickers = total_df["ticker"].unique().tolist()
        total_df["price_impact"] = 0.0

        if not total_df.empty:
            # 미리 가져온 가격 데이터 추가
            total_df["current_price"] = total_df["ticker"].map(
                lambda x: ticker_to_price_data.get(x, {}).get("current_price")
            )
            total_df["change_rt"] = total_df["ticker"].map(lambda x: ticker_to_price_data.get(x, {}).get("change_rt"))

            total_df["current_price"] = total_df["current_price"].fillna(total_df["that_time_price"])
            total_df["that_time_price"] = total_df["that_time_price"].fillna(0.0)

            # mask = (total_df["current_price"] != 0) & (total_df["that_time_price"] != 0)
            total_df["price_impact"] = 0.0
            # total_df.loc[mask, "price_impact"] = (
            #     (total_df.loc[mask, "current_price"] - total_df.loc[mask, "that_time_price"])
            #     / total_df.loc[mask, "that_time_price"]
            #     * 100
            # ).round(2)
            total_df = NewsService._convert_to_kst(total_df)

        # 결과 생성
        result = []
        for ticker in unique_tickers:
            ticker_news = total_df[total_df["ticker"] == ticker]
            if ticker_news.empty:
                continue

            news_items = []
            for _, row in ticker_news.iterrows():
                if len(news_items) >= stories_count:
                    break

                # Remove the cookie-based view checking
                price_impact = float(row["price_impact"]) if pd.notnull(row["price_impact"]) else 0.0
                news_items.append(
                    TopStoriesItem(
                        id=row["id"],
                        price_impact=price_impact,
                        date=row["date"],
                        title=row["title"],
                        summary=row["summary"],
                        impact_reason=row["impact_reason"],
                        key_points=row["key_points"],
                        emotion=row["emotion"],
                        type=row["type"],
                        is_viewed=False,  # Default to false, will be updated later
                    )
                )
            if lang == TranslateCountry.KO:
                name = self.remove_parentheses(ticker_news.iloc[0]["ko_name"])
            else:
                name = self.remove_parentheses(ticker_news.iloc[0]["en_name"])
            result.append(
                TopStoriesResponse(
                    name=name,
                    ticker=ticker,
                    logo_image="추후 반영",
                    ctry=ticker_news.iloc[0]["ctry"],
                    current_price=ticker_news.iloc[0]["current_price"]
                    if ticker_news.iloc[0].get("current_price")
                    else 0.0,
                    change_rate=ticker_news.iloc[0]["change_rt"],
                    items_count=len(news_items),
                    news=news_items,
                    is_viewed=False,  # Default to false, will be updated later
                )
            )
            print(f"news_items: {len(news_items)}")

        ticker_news = ticker_news.sort_values(by="date", ascending=True)

        # Update the viewed status using Redis
        result = self.check_stories_viewed_status(result, request, user)

        return result

    def mark_story_as_viewed(
        self,
        ticker: str,
        type: str,
        id: int,
        request: Request,
        response: Response,
        user: Optional[AlphafinderUser] = None,
    ) -> bool:
        """
        Mark a story as viewed by a user using Redis.
        For authenticated users, use their user ID.
        For anonymous users, use a cookie-based ID.

        Args:
            ticker: Stock ticker symbol
            type: Type of story ('news' or 'disclosure')
            id: ID of the story
            request: FastAPI request object
            response: FastAPI response object
            user: Optional authenticated user

        Returns:
            bool: True if the operation was successful
        """
        story_cache = get_story_view_cache()

        # For authenticated users, use their user ID
        if user:
            user_id = f"auth_{user.id}"
        else:
            # For anonymous users, use a cookie-based ID
            user_id = f"anon_{story_cache.get_or_create_anonymous_id(request, response)}"

        # Mark the story as viewed in Redis
        story_cache.mark_story_as_viewed(user_id=user_id, ticker=ticker, story_type=type, story_id=id)

        return True

    def is_story_viewed(
        self, ticker: str, type: str, id: int, request: Request, user: Optional[AlphafinderUser] = None
    ) -> bool:
        """
        Check if a story has been viewed by the current user.

        Args:
            ticker: Stock ticker symbol
            type: Type of story ('news' or 'disclosure')
            id: ID of the story
            request: FastAPI request object
            user: Optional authenticated user

        Returns:
            bool: True if the story has been viewed
        """
        story_cache = get_story_view_cache()

        # For authenticated users, use their user ID
        if user:
            user_id = f"auth_{user.id}"
        else:
            # For anonymous users, get ID from cookie (don't create if doesn't exist)
            anonymous_id = request.cookies.get(story_cache.anonymous_cookie_name)
            if not anonymous_id:
                return False
            user_id = f"anon_{anonymous_id}"

        return story_cache.is_story_viewed(user_id=user_id, ticker=ticker, story_type=type, story_id=id)

    def check_stories_viewed_status(
        self, stories_data: List[TopStoriesResponse], request: Request, user: Optional[AlphafinderUser] = None
    ) -> List[TopStoriesResponse]:
        """
        Check if stories have been viewed by the current user and update their status.
        This is used to update the viewed status of stories in the top_stories response.

        Args:
            stories_data: List of TopStoriesResponse objects
            request: FastAPI request object
            user: Optional authenticated user

        Returns:
            List[TopStoriesResponse]: Updated stories data with viewed status
        """
        story_cache = get_story_view_cache()

        # For authenticated users, use their user ID
        if user:
            user_id = f"auth_{user.id}"
        else:
            # For anonymous users, get ID from cookie (don't create if doesn't exist)
            anonymous_id = request.cookies.get(story_cache.anonymous_cookie_name)
            if not anonymous_id:
                return stories_data  # Return as is, all will be marked as unviewed
            user_id = f"anon_{anonymous_id}"

        # Get all viewed stories for this user
        viewed_stories = story_cache.get_viewed_stories(user_id)

        # Update viewed status for each story and sort items within each ticker group
        for story_group in stories_data:
            ticker = story_group.ticker
            has_unviewed = False

            # First update the viewed status for all items
            for item in story_group.news:
                story_key = f"{ticker}_{item.type}_{item.id}"
                item.is_viewed = story_key in viewed_stories
                if not item.is_viewed:
                    has_unviewed = True

            # Then sort items: viewed first, then by date (newest first)
            story_group.news.sort(key=lambda x: (-x.is_viewed, -x.date.timestamp()))

            # Update group level viewed status
            story_group.is_viewed = not has_unviewed

        # Sort groups: unviewed first, then by date of most recent story
        stories_data.sort(
            key=lambda x: (
                x.is_viewed,  # Unviewed groups first
                -max([item.date.timestamp() for item in x.news] if x.news else [0]),  # Then by most recent story date
            )
        )

        return stories_data

    def news_detail(self, ticker: str, date: str = None, page: int = 1, size: int = 6):
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")
        else:
            date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")

        ctry = check_ticker_country_len_2(ticker)

        condition = {
            "ticker": ticker,
            "date__gte": f"{date} 00:00:00",
            "date__lt": f"{date} 23:59:59",
            "is_exist": True,
        }
        df_news = pd.DataFrame(
            self.db._select(
                table="news_information",
                columns=["id", "ticker", "ctry", "date", "title", "summary", "emotion", "that_time_price"],
                **condition,
            )
        )
        if df_news.empty:
            raise DataNotFoundException(ticker=ticker, data_type="news")

        offset = (page - 1) * size
        df_news = self._process_dataframe_news_detail(df_news, offset, size)
        total_count = df_news.shape[0]
        total_page = math.ceil(total_count / size)
        emotion_count = self._count_emotion(df_news)
        df_news["that_time_price"] = df_news["that_time_price"].fillna(0.0)
        unique_tickers = df_news["ticker"].unique().tolist()

        df_price = pd.DataFrame(
            self.db._select(
                table="stock_trend",
                columns=["ticker", "current_price"],
                **{"ticker__in": unique_tickers},
            )
        )
        df_news["summary1"], df_news["summary2"] = self._split_parsing_summary(df_news["summary"])
        df_news["price_impact"] = 0.0
        if not df_price.empty:
            df_price["current_price"] = df_price["current_price"].fillna(0.0)

            df_news = pd.merge(df_news, df_price, on="ticker", how="left")

            df_news["price_impact"] = (
                (df_news["current_price"] - df_news["that_time_price"]) / df_news["that_time_price"] * 100
            )
            df_news["price_impact"] = round(df_news["price_impact"].replace([np.inf, -np.inf, np.nan], 0), 2)

        data = []
        for _, row in df_news.iterrows():
            data.append(
                NewsDetailItem(
                    id=row["id"],
                    ctry=row["ctry"],
                    date=row["date"],
                    title=row["title"],
                    summary1=row["summary1"],
                    summary2=row["summary2"],
                    emotion=row["emotion"],
                    price_impact=row["price_impact"],
                )
            )
        return data, total_count, total_page, offset, emotion_count, ctry

    def news_detail_v2(
        self,
        ticker: str,
        date: str = None,
        end_date: str = None,
        page: int = 1,
        size: int = 6,
        lang: TranslateCountry | None = None,
        user: AlphafinderUser | None = None,
    ):
        if lang is None:
            lang = TranslateCountry.KO

        if lang == TranslateCountry.KO:
            lang = "ko-KR"
        else:
            lang = "en-US"

        kst = pytz.timezone("Asia/Seoul")
        utc = pytz.timezone("UTC")

        # 시작 날짜 설정
        if not date:
            kst_date = datetime.now(kst).replace(tzinfo=None)
        else:
            kst_date = datetime.strptime(date, "%Y%m%d")

        # 종료 날짜 설정
        if end_date:
            kst_end_date = datetime.strptime(end_date, "%Y%m%d")
        else:
            kst_end_date = kst_date

        # 시작 / 종료 시간 localize
        kst_start_datetime = kst.localize(datetime.combine(kst_date, datetime.min.time()))
        kst_end_datetime = kst.localize(datetime.combine(kst_end_date, time(23, 59, 59)))

        # 시작 / 종료 시간 UTC로 변환
        utc_start_datetime = kst_start_datetime.astimezone(utc)
        utc_end_datetime = kst_end_datetime.astimezone(utc)

        # 현재 시간 + 5분까지 허용
        current_time = datetime.now(UTC)
        allowed_time = current_time + timedelta(minutes=5)

        if end_date:
            end_date = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")

        ctry = check_ticker_country_len_2(ticker)

        stock_info_columns = ["ticker", "en_name", "kr_name", "is_pub"]
        stock_info = self.db._select(
            table="stock_information",
            columns=stock_info_columns,
            ticker=ticker,
        )
        duplicate_stock_info = self.db._select(
            table="stock_information",
            columns=["ticker"],
            en_name=stock_info[0][1],
        )
        condition = {
            "date__gte": utc_start_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "date__lt": utc_end_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "date__lte": allowed_time.strftime("%Y-%m-%d %H:%M:%S"),
            "is_exist": True,
            "is_related": True,
            "lang": lang,
        }
        recent_news_condition = {
            "is_related": True,
            "lang": lang,
            "date__lte": allowed_time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        if len(duplicate_stock_info) == 2:
            unique_tickers = [info[0] for info in duplicate_stock_info]
            condition["ticker__in"] = unique_tickers
            recent_news_condition["ticker__in"] = unique_tickers
        else:
            condition["ticker"] = ticker
            recent_news_condition["ticker"] = ticker

        recent_news_id = self.db._select(
            table="news_analysis",
            columns=["id"],
            order="date",
            ascending=False,
            limit=10,
            **recent_news_condition,
        )
        recent_news_id = [id[0] for id in recent_news_id]

        df_news = pd.DataFrame(
            self.db._select(
                table="news_analysis",
                columns=[
                    "id",
                    "ticker",
                    "ctry",
                    "date",
                    "title",
                    "summary",
                    "impact_reason",
                    "key_points",
                    "emotion",
                    "that_time_price",
                ],
                order="date",
                ascending=False,
                **condition,
            )
        )
        if df_news.empty:
            if stock_info[0].is_pub:
                emotion_count = {"positive": 0, "negative": 0, "neutral": 0}
                return [], 0, 0, 0, emotion_count, ctry
            else:
                raise DataNotFoundException(ticker=ticker, data_type="news")

        user_level = user.subscription_level if user else 1
        # 최근 10개를 제외한 데이터 마스킹
        if user_level == 1:
            df_news = self.mask_fields(df=df_news, recent_news_id=recent_news_id)

        offset = (page - 1) * size
        df_news["emotion"] = df_news["emotion"].str.lower()
        total_count = df_news.shape[0]
        total_page = math.ceil(total_count / size)
        emotion_count = self._count_emotion(df_news)

        if offset >= total_count:
            page = total_page
            offset = (page - 1) * size

        df_news = df_news[offset : offset + size]
        df_news["date"] = pd.to_datetime(df_news["date"]).dt.tz_localize(utc).dt.tz_convert(kst)
        df_news["that_time_price"] = df_news["that_time_price"].fillna(0.0)

        kr_name = stock_info[0][2] if len(stock_info[0]) > 2 else None
        en_name = stock_info[0][1] if len(stock_info[0]) > 1 else None

        data = []
        for _, row in df_news.iterrows():
            data.append(
                NewsDetailItemV2(
                    id=row["id"],
                    ctry=row["ctry"],
                    date=row["date"],
                    title=row["title"],
                    summary=row["summary"],
                    impact_reason=row["impact_reason"],
                    key_points=row["key_points"],
                    emotion=row["emotion"],
                    price_impact=0,
                    kr_name=kr_name,
                    en_name=en_name,
                )
            )
        return data, total_count, total_page, offset, emotion_count, ctry

    def etf_news_detail_v2(
        self,
        ticker: str,
        date: str = None,
        end_date: str = None,
        page: int = 1,
        size: int = 6,
        lang: TranslateCountry | None = None,
        user: AlphafinderUser | None = None,
    ):
        """
        ETF 구성 종목들의 뉴스를 조회하는 서비스 메소드

        Args:
            ticker: ETF 티커
            date: 시작 날짜 (YYYYMMDD 형식)
            end_date: 종료 날짜 (YYYYMMDD 형식)
            page: 페이지 번호
            size: 페이지 크기
            lang: 언어 설정
            user: 사용자 정보

        Returns:
            뉴스 데이터, 총 개수, 총 페이지, 오프셋, 감정 분석 결과, 국가 코드
        """
        if lang is None:
            lang = TranslateCountry.KO

        if lang == TranslateCountry.KO:
            lang = "ko-KR"
        else:
            lang = "en-US"

        kst = pytz.timezone("Asia/Seoul")
        utc = pytz.timezone("UTC")

        # 시작 날짜 설정
        if not date:
            kst_date = datetime.now(kst).replace(tzinfo=None)
        else:
            kst_date = datetime.strptime(date, "%Y%m%d")

        # 종료 날짜 설정
        if end_date:
            kst_end_date = datetime.strptime(end_date, "%Y%m%d")
        else:
            kst_end_date = kst_date

        # 시작 / 종료 시간 localize
        kst_start_datetime = kst.localize(datetime.combine(kst_date, datetime.min.time()))
        kst_end_datetime = kst.localize(datetime.combine(kst_end_date, time(23, 59, 59)))

        # 시작 / 종료 시간 UTC로 변환
        utc_start_datetime = kst_start_datetime.astimezone(utc)
        utc_end_datetime = kst_end_datetime.astimezone(utc)

        # 현재 시간 + 5분까지 허용
        current_time = datetime.now(UTC)
        allowed_time = current_time + timedelta(minutes=5)

        if end_date:
            end_date = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")

        ctry = check_ticker_country_len_2(ticker)
        print("Ticker: ", ticker)

        holdings = self.db._select(
            table="etf_top_holdings",
            columns=["holding_ticker"],
            ticker=ticker,
        )

        print("HOLDINGS : ", holdings)

        if not holdings:
            print("ETF 구성 종목이 없습니다.")
            emotion_count = {"positive": 0, "negative": 0, "neutral": 0}
            return [], 0, 0, 0, emotion_count, ctry

        holding_tickers = [holding[0] for holding in holdings if holding[0]]

        if not holding_tickers:
            emotion_count = {"positive": 0, "negative": 0, "neutral": 0}
            return [], 0, 0, 0, emotion_count, ctry

        # 구성 종목별 정보 조회
        stock_info_map = {}
        stock_infos = self.db._select(
            table="stock_information",
            columns=["ticker", "en_name", "kr_name"],
            ticker__in=holding_tickers,
        )

        for stock_info in stock_infos:
            stock_info_map[stock_info[0]] = {
                "en_name": stock_info[1],
                "kr_name": stock_info[2] if len(stock_info) > 2 else None,
            }

        # 뉴스 조회 조건
        condition = {
            "date__gte": utc_start_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "date__lt": utc_end_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "date__lte": allowed_time.strftime("%Y-%m-%d %H:%M:%S"),
            "is_exist": True,
            "is_related": True,
            "lang": lang,
            "ticker__in": holding_tickers,
        }

        # 구성 종목들의 뉴스 데이터 조회
        df_news = pd.DataFrame(
            self.db._select(
                table="news_analysis",
                columns=[
                    "id",
                    "ticker",
                    "ctry",
                    "date",
                    "title",
                    "summary",
                    "impact_reason",
                    "key_points",
                    "emotion",
                    "that_time_price",
                ],
                order="date",
                ascending=False,
                **condition,
            )
        )

        if df_news.empty:
            emotion_count = {"positive": 0, "negative": 0, "neutral": 0}
            return [], 0, 0, 0, emotion_count, ctry

        # 유저 레벨에 따른 마스킹 처리
        recent_news_id = self.db._select(
            table="news_analysis",
            columns=["id"],
            order="date",
            ascending=False,
            limit=10,
            **{
                "is_related": True,
                "lang": lang,
                "date__lte": allowed_time.strftime("%Y-%m-%d %H:%M:%S"),
                "ticker__in": holding_tickers,
            },
        )
        recent_news_id = [id[0] for id in recent_news_id]

        user_level = user.subscription_level if user else 1
        # 최근 10개를 제외한 데이터 마스킹
        if user_level == 1:
            df_news = self.mask_fields(df=df_news, recent_news_id=recent_news_id)

        # 감정 분석 및 페이지네이션 처리
        offset = (page - 1) * size
        df_news["emotion"] = df_news["emotion"].str.lower()
        total_count = df_news.shape[0]
        total_page = math.ceil(total_count / size)
        emotion_count = self._count_emotion(df_news)

        if offset >= total_count:
            page = total_page
            offset = (page - 1) * size

        # 현재 페이지 데이터 추출
        df_news = df_news[offset : offset + size]

        # 타임존 변환
        df_news["date"] = pd.to_datetime(df_news["date"]).dt.tz_localize(utc).dt.tz_convert(kst)
        df_news["that_time_price"] = df_news["that_time_price"].fillna(0.0)

        # 응답 데이터 생성
        data = []
        for _, row in df_news.iterrows():
            ticker_info = stock_info_map.get(row["ticker"], {})
            data.append(
                NewsDetailItemV2(
                    id=row["id"],
                    ctry=row["ctry"],
                    date=row["date"],
                    title=row["title"],
                    summary=row["summary"],
                    impact_reason=row["impact_reason"],
                    key_points=row["key_points"],
                    emotion=row["emotion"],
                    price_impact=0,
                    kr_name=ticker_info.get("kr_name"),
                    en_name=ticker_info.get("en_name"),
                )
            )

        return data, total_count, total_page, offset, emotion_count, ctry

    def remove_parentheses(self, text):
        if not text:  # None이나 빈 문자열 체크
            return text
        # \(.*?\)$ : 마지막에 있는 괄호와 그 내용을 매칭
        # .*? : 괄호 안의 모든 문자 (non-greedy)
        # $ : 문자열의 끝
        cleaned_text = re.sub(r"\(.*?\)$", "", text).strip()
        return cleaned_text

    def _split_parsing_summary(self, summaries):
        """
        DataFrame의 summary 컬럼을 summary1과 summary2로 나누는 함수

        Args:
            summaries (pd.Series): 뉴스 요약 텍스트가 담긴 Series

        Returns:
            tuple[pd.Series, pd.Series]: (summary1_series, summary2_series) 형태로 반환
        """

        def split_single_summary(summary):
            if not summary:
                return None, None

            # 기본 구분자로 나누기
            parts = summary.split("**기사 요약**")
            if len(parts) < 2:
                return summary, ""

            # summary1 추출 (기사 요약 부분)
            summary1_part = parts[1].split("**주가에")[0].strip(" :\n-")
            summary1_lines = [line.strip(" -") for line in summary1_part.split("\n") if line.strip()]
            summary1 = "\n".join(f"- {line}" for line in summary1_lines if line)

            # summary2 추출 (주가 영향 및 감성분석 부분)
            remaining_text = "**주가에" + "".join(parts[1:])
            impact_part = remaining_text.split("**뉴스 감성분석**")[0]
            sentiment_part = remaining_text.split("**뉴스 감성분석**")[1]

            # # 감성 값 추출
            # sentiment = sentiment_part.split(":")[1].split("\n")[0].strip()

            # 주가 영향 부분 처리
            impact_lines = [line.strip(" -") for line in impact_part.split("\n") if line.strip()]
            impact_lines = [line for line in impact_lines if not line.startswith("**")]
            impact_text = "\n".join(f"- {line}" for line in impact_lines if line)

            # 감성분석 부분 처리
            sentiment_lines = [line.strip(" -") for line in sentiment_part.split("\n") if line.strip()]
            sentiment_lines = [line for line in sentiment_lines if not line.startswith("**") and ":" not in line]
            sentiment_text = "\n".join(f"- {line}" for line in sentiment_lines if line)
            # sentiment_text = sentiment_text.replace("있음.", "있어요.").replace("보임.", "보여요.").replace("없음", "없어요.").replace("존재함", "존재해요.")

            # summary2 조합
            summary2 = f"주가에 영향을 줄 수 있어요\n{impact_text}\n\n세네카 AI는 해당 뉴스가 {{emotion}} 이라고 판단했어요\n{sentiment_text}"

            return summary1, summary2

        # Series의 각 요소에 대해 split_single_summary 함수 적용
        summary1_list = []
        summary2_list = []

        for summary in summaries:
            s1, s2 = split_single_summary(summary)
            summary1_list.append(s1)
            summary2_list.append(s2)

        return pd.Series(summary1_list, index=summaries.index), pd.Series(summary2_list, index=summaries.index)

    def increase_news_search_count(self, news_id: int, ticker: str) -> None:
        redis = NewsLeaderboard()
        redis.increment_score(news_id, ticker)

    def increase_disclosure_search_count(self, disclosure_id: int, ticker: str) -> None:
        redis = DisclosureLeaderboard()
        redis.increment_score(disclosure_id, ticker)

    def mask_fields(self, df: pd.DataFrame, masked_count: int = 10, recent_news_id: list[int] = []) -> pd.DataFrame:
        # 최근 10개를 제외한 모든 데이터를 마스킹
        if df.empty:
            return df

        # 복사본 생성
        df_masked = df.copy()

        # 마스킹할 컬럼
        mask_columns = ["impact_reason", "key_points"]

        # 마스킹 메시지
        mask_message = ""

        # recent_ten_news_id에 포함되지 않은 뉴스에 대해 마스킹 적용
        if recent_news_id:
            # ID가 recent_ten_news_id에 없는 행 찾기
            mask_indices = df_masked[~df_masked["id"].isin(recent_news_id)].index

            # 마스킹 적용
            for column in mask_columns:
                if column in df_masked.columns:
                    df_masked.loc[mask_indices, column] = mask_message
        else:
            # recent_ten_news_id가 제공되지 않은 경우 기존 로직 사용 (최근 10개 제외 마스킹)
            # 데이터를 날짜순으로 정렬
            df_masked = df_masked.sort_values(by=["date"], ascending=[False])
            mask_indices = df_masked.index[masked_count:]

            for column in mask_columns:
                if column in df_masked.columns:
                    df_masked.loc[mask_indices, column] = mask_message

        return df_masked

    def mask_news_items(self, items: list, masked_count: int = 10) -> list:
        """
        뉴스 아이템 리스트에 마스킹을 적용합니다.
        최근 masked_count개의 아이템은 원래대로 유지하고 나머지는 특정 필드를 마스킹합니다.

        Args:
            items: 마스킹할 뉴스 아이템 리스트 (NewsRenewalItem 객체)
            masked_count: 마스킹하지 않을 뉴스 아이템 개수 (기본값: 10)

        Returns:
            마스킹이 적용된 아이템 리스트
        """
        if not items or len(items) <= masked_count:
            return items

        # 마스킹되지 않을 아이템
        unmasked_items = items[:masked_count]

        # 마스킹될 아이템
        masked_items = []
        masked_item = items[masked_count].model_copy(update={"impact_reason": "", "key_points": ""})
        masked_items.append(masked_item)

        # 마스킹되지 않은 아이템과 마스킹된 아이템 결합
        return unmasked_items + masked_items

    def mask_disclosure_items(self, items: list, days: int = 7) -> list:
        """
        공시 아이템 리스트에 시간 기준 마스킹을 적용합니다.
        최근 days일 이내 공시는 원래대로 유지하고, 그 이전 공시는 마스킹합니다.

        Args:
            items: 마스킹할 공시 아이템 리스트 (DisclosureRenewalItem 객체)
            days: 마스킹하지 않을 기간(일) (기본값: 7)

        Returns:
            마스킹이 적용된 아이템 리스트
        """
        if not items:
            return items

        from datetime import datetime, timedelta

        from app.common.constants import KST

        # 기준일 계산
        now = datetime.now(KST)
        cutoff_date = now - timedelta(days=days)

        # 기준일 이내/이전 아이템 분류
        recent_items = []
        older_items = []

        for item in items:
            if item.date >= cutoff_date:
                recent_items.append(item)
            else:
                older_items.append(item)

        # 오래된 아이템 마스킹
        masked_older_items = []
        masked_item = older_items[0].model_copy(update={"impact_reason": "", "key_points": ""})
        masked_older_items.append(masked_item)

        # 최신 아이템과 마스킹된 오래된 아이템 결합
        return recent_items + masked_older_items

    def get_recent_news_ids_by_ticker(
        self, tickers: List[str], limit: int = 10, lang: TranslateCountry | None = None
    ) -> Dict[str, List[int]]:
        """
        각 티커별로 최신 뉴스 ID를 가져옵니다.

        Args:
            tickers: 티커 리스트
            limit: 각 티커별로 가져올 최신 뉴스 개수
            lang: 언어 설정

        Returns:
            {ticker: [news_id1, news_id2, ...]} 형태의 딕셔너리
        """
        if lang is None:
            lang = TranslateCountry.KO

        condition = {"is_exist": True, "is_related": True}

        if lang == TranslateCountry.KO:
            condition["lang"] = "ko-KR"
        else:
            condition["lang"] = "en-US"

        result = {}

        for ticker in tickers:
            ticker_condition = condition.copy()
            ticker_condition["ticker"] = ticker

            news_ids = self.db._select(
                table="news_analysis",
                columns=["id"],
                order="date",
                ascending=False,
                limit=limit,
                **ticker_condition,
            )

            result[ticker] = [news_id.id for news_id in news_ids]

        return result

    def get_recent_disclosure_ids_by_ticker(
        self, tickers: List[str], limit: int = 10, lang: TranslateCountry | None = None
    ) -> Dict[str, List[int]]:
        """
        각 티커별로 최신 공시 ID를 가져옵니다.

        Args:
            tickers: 티커 리스트
            limit: 각 티커별로 가져올 최신 공시 개수
            lang: 언어 설정

        Returns:
            {ticker: [disclosure_id1, disclosure_id2, ...]} 형태의 딕셔너리
        """
        if lang is None:
            lang = TranslateCountry.KO

        condition = {"is_exist": True}

        if lang == TranslateCountry.KO:
            condition["lang"] = "ko-KR"
        else:
            condition["lang"] = "en-US"

        result = {}

        for ticker in tickers:
            ticker_condition = condition.copy()
            ticker_condition["ticker"] = ticker

            disclosure_ids = self.db._select(
                table="disclosure_information",
                columns=["id"],
                order="date",
                ascending=False,
                limit=limit,
                **ticker_condition,
            )

            result[ticker] = [disclosure_id[0] for disclosure_id in disclosure_ids]

        return result

    def mask_news_items_by_id(
        self, items: List[NewsRenewalItem], ticker_based_ids: Dict[str, List[int]]
    ) -> List[NewsRenewalItem]:
        """
        뉴스 아이템 리스트에 티커별 ID 기반으로 마스킹을 적용합니다.

        Args:
            items: 마스킹할 뉴스 아이템 리스트 (NewsRenewalItem 객체)
            ticker_based_ids: 티커별 마스킹하지 않을 뉴스 ID 딕셔너리 {ticker: [id1, id2, ...]}

        Returns:
            마스킹이 적용된 아이템 리스트
        """
        if not items or not ticker_based_ids:
            return items

        # 결과 리스트
        masked_items = []

        # 각 아이템에 대해 마스킹 적용 여부 결정
        for item in items:
            ticker = item.ticker

            # 해당 티커의 허용된 ID 목록 가져오기
            allowed_ids = ticker_based_ids.get(ticker, [])

            # ID가 허용 목록에 있으면 원본 그대로, 없으면 마스킹 적용
            if item.id in allowed_ids:
                masked_items.append(item)
            else:
                # 마스킹 적용 (impact_reason과 key_points 필드를 비움)
                masked_item = item.model_copy(update={"impact_reason": "", "key_points": ""})
                masked_items.append(masked_item)

        return masked_items

    def mask_disclosure_items_by_date(
        self, items: List[DisclosureRenewalItem], days: int = 7
    ) -> List[DisclosureRenewalItem]:
        """
        공시 아이템 리스트에 날짜 기반으로 마스킹을 적용합니다.
        최근 days일 이내 공시는 원래대로 유지하고, 그 이전 공시는 마스킹합니다.

        Args:
            items: 마스킹할 공시 아이템 리스트 (DisclosureRenewalItem 객체)
            days: 마스킹하지 않을 기간(일) (기본값: 7)

        Returns:
            마스킹이 적용된 아이템 리스트
        """
        if not items:
            return items

        from datetime import datetime, timedelta

        from app.common.constants import KST

        # 기준일 계산
        now = datetime.now(KST)
        cutoff_date = now - timedelta(days=days)

        # 결과 리스트
        masked_items = []

        # 각 아이템에 대해 마스킹 적용 여부 결정
        for item in items:
            # 날짜가 cutoff_date보다 최신이면 원본 그대로, 아니면 마스킹 적용
            if item.date >= cutoff_date:
                masked_items.append(item)
            else:
                # 마스킹 적용 (impact_reason과 key_points 필드를 비움)
                masked_item = item.model_copy(update={"impact_reason": "", "key_points": ""})
                masked_items.append(masked_item)

        return masked_items


def get_news_service() -> NewsService:
    return NewsService()
