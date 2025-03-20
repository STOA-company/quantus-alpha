from datetime import datetime, time, timedelta
import json
import math
import re
from typing import List, Tuple, Union, Optional

from fastapi import Request, Response
import pytz
from sqlalchemy import text
from app.core.exception.custom import DataNotFoundException
from app.modules.common.enum import TranslateCountry
from app.modules.disclosure.mapping import (
    CATEGORY_TYPE_MAPPING_EN,
    DOCUMENT_TYPE_MAPPING,
    DOCUMENT_TYPE_MAPPING_EN,
    FORM_TYPE_MAPPING,
)

import numpy as np
import pandas as pd
from app.modules.news.schemas import (
    DisclosureRenewalItem,
    NewsDetailItem,
    NewsDetailItemV2,
    NewsRenewalItem,
    TopStoriesItem,
    TopStoriesResponse,
)
from app.database.crud import database, JoinInfo
from app.utils.ctry_utils import check_ticker_country_len_2
from app.common.constants import KST, UTC
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

    def get_renewal_data(
        self, ctry: str = None, lang: TranslateCountry | None = None, tickers: Optional[List[str]] = None
    ) -> Tuple[List[NewsRenewalItem], List[DisclosureRenewalItem]]:
        if lang is None:
            lang = TranslateCountry.KO

        condition = {"is_exist": True}
        if tickers:
            condition["ticker__in"] = tickers
        if ctry:
            condition["ctry"] = "KR" if ctry == "kr" else "US" if ctry == "us" else None

        if lang == TranslateCountry.KO:
            # 뉴스 데이터
            condition["lang"] = "ko-KR"

            news_name = "kr_name"
            disclosure_name = "ko_name"
        else:
            # 뉴스 데이터
            condition["lang"] = "en-US"

            news_name = "en_name"
            disclosure_name = "en_name"

        news_condition = condition.copy()
        disclosure_condition = condition.copy()

        news_condition["is_related"] = True

        change_rate_column = "change_rt"

        join_info = lambda table: JoinInfo(  # noqa: E731
            primary_table=table,
            secondary_table="stock_trend",
            primary_column="ticker",
            secondary_column="ticker",
            columns=["current_price", change_rate_column],
            is_outer=True,
        )

        from concurrent.futures import ThreadPoolExecutor

        def fetch_data(table, columns, condition):
            return pd.DataFrame(
                self.db._select(
                    table=table,
                    columns=columns,
                    order="date",
                    ascending=False,
                    limit=100,
                    join_info=join_info(table),
                    **condition,
                )
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            news_future = executor.submit(
                fetch_data,
                "news_analysis",
                [
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
                news_condition,
            )

            disclosure_future = executor.submit(
                fetch_data,
                "disclosure_information",
                [
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
                disclosure_condition,
            )

            df_news = news_future.result()
            df_disclosure = disclosure_future.result()

        news_data = [] if df_news.empty else self._process_price_data(df=self._process_dataframe_news(df_news), lang=lang)

        disclosure_data = (
            []
            if df_disclosure.empty
            else self._process_price_data(
                self._process_dataframe_disclosure(df_disclosure), lang=lang, is_disclosure=True
            )
        )

        return news_data, disclosure_data

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

    def top_stories(self, request: Request, lang: TranslateCountry | None = None):
        viewed_stories = set()
        if request.cookies.get("viewed_stories"):
            cookie_data = request.cookies.get("viewed_stories", "[]")
            try:
                viewed_stories = set(json.loads(cookie_data))
            except json.JSONDecodeError:
                viewed_stories = set()

        if lang is None:
            lang = TranslateCountry.KO

        current_datetime = now_utc()
        before_24_hours = current_datetime - timedelta(hours=24)

        query_us = f"""
            SELECT st.ticker, st.volume_change_rt, st.current_price, st.change_rt
            FROM stock_trend st
            JOIN (
                SELECT DISTINCT ticker
                FROM news_analysis
                WHERE date >= '{before_24_hours}'
            ) na ON st.ticker = na.ticker
            WHERE ctry = 'US'
            ORDER BY st.volume_change_rt DESC
            LIMIT 6
        """
        top_stories_data_us = self.db._execute(text(query_us))
        query_kr = f"""
            SELECT st.ticker, st.volume_change_rt, st.current_price, st.change_rt
            FROM stock_trend st
            JOIN (
                SELECT DISTINCT ticker
                FROM news_analysis
                WHERE date >= '{before_24_hours}'
            ) na ON st.ticker = na.ticker
            WHERE ctry = 'KR'
            ORDER BY st.volume_change_rt DESC
            LIMIT 5
        """
        top_stories_data_kr = self.db._execute(text(query_kr))

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

        condition = {"is_exist": True, "date__gte": before_24_hours, "ticker__in": top_stories_tickers}

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
            self.db._select(
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

            print(f"df_disclosure: {df_disclosure}")

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

        # 종목 현재가 정보 추가 (미리 가져온 데이터 사용)
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

            mask = (total_df["current_price"] != 0) & (total_df["that_time_price"] != 0)
            total_df["price_impact"] = 0.0
            total_df.loc[mask, "price_impact"] = (
                (total_df.loc[mask, "current_price"] - total_df.loc[mask, "that_time_price"])
                / total_df.loc[mask, "that_time_price"]
                * 100
            ).round(2)
            total_df = NewsService._convert_to_kst(total_df)

        # 결과 생성
        result = []
        for ticker in unique_tickers:
            ticker_news = total_df[total_df["ticker"] == ticker]
            if ticker_news.empty:
                continue

            news_items = []
            ticker_has_unviewed = False
            for _, row in ticker_news.iterrows():
                if len(news_items) >= 30:
                    break
                news_key = f'{ticker}_{row["type"]}_{row["id"]}'
                is_viewed = news_key in viewed_stories

                if not is_viewed:
                    ticker_has_unviewed = True

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
                        is_viewed=is_viewed,
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
                    is_viewed=not ticker_has_unviewed,
                )
            )
        result.sort(key=lambda x: x.is_viewed, reverse=False)

        return result

    def mark_story_as_viewed(self, ticker: str, type: str, id: int, request: Request, response: Response) -> bool:
        # 기존 쿠키 확인
        current_viewed = request.cookies.get("viewed_stories", "[]")
        viewed_list = json.loads(current_viewed)

        # 새로운 조회 기록 추가
        story_key = f"{ticker}_{type}_{id}"
        if story_key not in viewed_list:
            viewed_list.append(story_key)

            if len(viewed_list) > 100:  # 최대 100개 기록 유지
                viewed_list.pop(0)

        # 쿠키 업데이트
        response.set_cookie(
            key="viewed_stories",
            value=json.dumps(viewed_list),
            max_age=86400,  # 24시간
            httponly=True,
            # secure=True,
            samesite="lax",
        )

        return True

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

        if end_date:
            end_date = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")

        ctry = check_ticker_country_len_2(ticker)

        stock_info_columns = ["ticker", "en_name", "is_pub"]
        stock_info = self.db._select(
            table="stock_information",
            columns=stock_info_columns,
            **{"ticker": ticker},
        )
        duplicate_stock_info = self.db._select(
            table="stock_information",
            columns=["ticker"],
            **{"en_name": stock_info[0][1]},
        )
        condition = {
            "date__gte": utc_start_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "date__lt": utc_end_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "is_exist": True,
            "is_related": True,
            "lang": lang,
        }
        if len(duplicate_stock_info) == 2:
            unique_tickers = [info[0] for info in duplicate_stock_info]
            condition["ticker__in"] = unique_tickers
        else:
            condition["ticker"] = ticker

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

        # df_price = pd.DataFrame(
        #     self.db._select(
        #         table="stock_trend",
        #         columns=["ticker", "current_price"],
        #         **{"ticker": ticker},
        #     )
        # )

        df_news["price_impact"] = 0.0
        # if not df_price.empty:
        #     df_price["current_price"] = df_price["current_price"].fillna(0.0)

        #     df_news = pd.merge(df_news, df_price, on="ticker", how="left")

        #     df_news["price_impact"] = (
        #         (df_news["current_price"] - df_news["that_time_price"]) / df_news["that_time_price"] * 100
        #     )
        #     df_news["price_impact"] = round(df_news["price_impact"].replace([np.inf, -np.inf, np.nan], 0), 2)

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
                    price_impact=row["price_impact"],
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


def get_news_service() -> NewsService:
    return NewsService()
