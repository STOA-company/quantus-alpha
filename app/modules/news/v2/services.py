import asyncio
import math
import re
from datetime import datetime, time, timedelta
from typing import List, Optional, Union

import pandas as pd
import pytz
from fastapi import Request, Response

from app.cache.story_view import get_story_view_cache
from app.common.constants import KST, UTC
from app.core.exception.custom import DataNotFoundException
from app.database.crud import database
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from app.modules.disclosure.mapping import (
    CATEGORY_TYPE_MAPPING_EN,
    DOCUMENT_TYPE_MAPPING,
    DOCUMENT_TYPE_MAPPING_EN,
    FORM_TYPE_MAPPING,
)
from app.modules.news.v2.schemas import (
    DisclosureRenewalItem,
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
    def _count_emotion(df: pd.DataFrame) -> dict:
        return df["emotion"].value_counts().to_dict()

    @staticmethod
    def _convert_to_kst(df: pd.DataFrame) -> pd.DataFrame:
        dates = pd.to_datetime(df["date"])
        if dates.dt.tz is None:
            df["date"] = dates.dt.tz_localize(UTC).dt.tz_convert(KST)
        else:
            df["date"] = dates.dt.tz_convert(KST)
        return df

    @staticmethod
    def _process_dataframe_disclosure(df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 전처리 및 필터링"""
        df = df.dropna(subset=["emotion"]).sort_values(by=["date"], ascending=[False])
        df = NewsService._convert_to_kst(df)

        df["emotion"] = df["emotion"].str.lower()
        df["ctry"] = df["ctry"].str.lower()

        return df

    def remove_parentheses(self, text):
        if not text:  # None이나 빈 문자열 체크
            return text
        # \(.*?\)$ : 마지막에 있는 괄호와 그 내용을 매칭
        # .*? : 괄호 안의 모든 문자 (non-greedy)
        # $ : 문자열의 끝
        cleaned_text = re.sub(r"\(.*?\)$", "", text).strip()
        return cleaned_text

    def news_detail(
        self,
        ticker: str,
        date: str = None,
        end_date: str = None,
        page: int = 1,
        size: int = 6,
        lang: TranslateCountry | None = None,
        user: AlphafinderUser | None = None,  # noqa
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
                    "kr_name",
                    "ctry",
                    "date",
                    "title",
                    "summary",
                    "impact_reason",
                    "key_points",
                    "emotion",
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

        # user_level = user.subscription_level if user else 1 # TODO :: 유저 마이그레이션 완료 후 주석 해제
        # # 최근 10개를 제외한 데이터 마스킹
        # if user_level == 1:
        #     df_news = self.mask_fields(df=df_news, recent_news_id=recent_news_id)

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

        data = []
        for _, row in df_news.iterrows():
            data.append(
                NewsDetailItemV2(
                    id=row["id"],
                    ctry=row["ctry"].lower(),
                    name=row["kr_name"],
                    ticker=row["ticker"],
                    date=row["date"],
                    title=row["title"],
                    summary=row["summary"],
                    impact_reason=row["impact_reason"],
                    key_points=row["key_points"],
                    emotion=row["emotion"],
                )
            )
        return data, total_count, total_page, offset, emotion_count, ctry

    def get_news(
        self, ctry: str = None, lang: TranslateCountry | None = None, tickers: Optional[List[str]] = None
    ) -> List[NewsRenewalItem]:
        if lang is None:
            lang = TranslateCountry.KO

        news_condition = {"is_exist": True}
        if tickers:
            news_condition["ticker__in"] = tickers
        if ctry:
            news_condition["ctry"] = "KR" if ctry == "kr" else "US" if ctry == "us" else None

        if lang == TranslateCountry.KO:
            news_condition["lang"] = "ko-KR"
            news_name = "kr_name"
        else:
            news_condition["lang"] = "en-US"
            news_name = "en_name"

        news_condition["is_related"] = True

        # 현재 시간에 5분을 더한 시간까지 허용
        current_time = datetime.now(UTC)
        allowed_time = current_time + timedelta(minutes=5)
        news_condition["date__lte"] = allowed_time

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
                ],
                order="date",
                ascending=False,
                limit=100,
                **news_condition,
            )
        )

        if df_news.empty:
            ticker = ",".join(tickers) if tickers else None
            raise DataNotFoundException(ticker=ticker, data_type="news")

        df_news = df_news.dropna(subset=["emotion"]).sort_values(by=["date"], ascending=[False])
        df_news = df_news[df_news["title"].str.strip() != ""]  # titles가 "" 인 경우 행 삭제
        df_news = NewsService._convert_to_kst(df_news)

        news_data = [] if df_news.empty else self._process_price_data(df=df_news, lang=lang)

        return news_data

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

    def get_disclosure(
        self, ctry: str = None, lang: TranslateCountry | None = None, tickers: Optional[List[str]] = None
    ) -> List[DisclosureRenewalItem]:
        if lang is None:
            lang = TranslateCountry.KO

        disclosure_condition = {"is_exist": True}
        if tickers:
            disclosure_condition["ticker__in"] = tickers
        if ctry:
            disclosure_condition["ctry"] = "KR" if ctry == "kr" else "US" if ctry == "us" else None

        if lang == TranslateCountry.KO:
            disclosure_condition["lang"] = "ko-KR"
            disclosure_name = "ko_name"
        else:
            disclosure_condition["lang"] = "en-US"
            disclosure_name = "en_name"

        # 현재 시간에 5분을 더한 시간까지 허용
        current_time = datetime.now(UTC)
        allowed_time = current_time + timedelta(minutes=5)
        disclosure_condition["date__lte"] = allowed_time

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
                    # "current_price",
                    # "change_rt",
                ],
                order="date",
                ascending=False,
                limit=100,
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

    def _process_price_data(
        self, df: pd.DataFrame, lang: TranslateCountry, is_disclosure: bool = False
    ) -> List[Union[NewsRenewalItem, DisclosureRenewalItem]]:
        if df.empty:
            return []

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
                ticker=row["ticker"],
            )
            for _, row in df.iterrows()
        ]

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

        top_stories_tickers = tickers

        # 해당 티커의 가격 데이터 조회 (비동기)
        price_data = await self.db._select_async(
            table="stock_trend", columns=["ticker", "current_price", "change_rt", "ctry"], ticker__in=tickers
        )

        ticker_to_price_data = {}
        for row in price_data:
            ticker = row[0]
            ticker_to_price_data[ticker] = {"current_price": row[1], "change_rt": row[2]}

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

        # 뉴스 데이터 수집 (비동기)
        news_data = await self.db._select_async(
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
        df_news = pd.DataFrame(news_data)
        if not df_news.empty:
            df_news = df_news.dropna(subset=["emotion"]).sort_values(by=["date"], ascending=[False])
            df_news = df_news[df_news["title"].str.strip() != ""]  # titles가 "" 인 경우 행 삭제
            df_news = NewsService._convert_to_kst(df_news)
            df_news["type"] = "news"
            if lang == TranslateCountry.KO:
                df_news = df_news.rename(columns={"kr_name": "ko_name"})

        # 공시 데이터 수집 (비동기)
        disclosure_data = await self.db._select_async(
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
        df_disclosure = pd.DataFrame(disclosure_data)
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

            total_df["price_impact"] = 0.0
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
        # Update the viewed status using Redis
        result = self.check_stories_viewed_status(result, request, user)

        return result

    def check_stories_viewed_status(
        self, stories_data: List[TopStoriesResponse], request: Request, user: Optional[AlphafinderUser] = None
    ) -> List[TopStoriesResponse]:
        """
        현재 사용자가 스토리를 조회했는지 확인하고 상태를 업데이트합니다.
        top_stories 응답에서 스토리의 조회 상태를 업데이트하는데 사용됩니다.

        Args:
            stories_data: TopStoriesResponse 객체 리스트
            request: FastAPI 요청 객체
            user: 선택적 인증된 사용자

        Returns:
            List[TopStoriesResponse]: 조회 상태가 업데이트된 스토리 데이터
        """
        story_cache = get_story_view_cache()

        # 인증된 사용자의 경우 사용자 ID를 사용
        if user:
            user_id = f"auth_{user['uid']}"
        else:
            # 익명 사용자의 경우 쿠키에서 ID를 가져옴 (존재하지 않으면 생성하지 않음)
            anonymous_id = request.cookies.get(story_cache.anonymous_cookie_name)
            if not anonymous_id:
                return stories_data  # 그대로 반환, 모두 미조회로 표시됨
            user_id = f"anon_{anonymous_id}"

        # 해당 사용자의 모든 조회된 스토리 가져오기
        viewed_stories = story_cache.get_viewed_stories(user_id)

        # 각 티커 그룹 내의 아이템에 대해 조회 상태 업데이트 및 정렬
        for story_group in stories_data:
            ticker = story_group.ticker
            has_unviewed = False

            # 먼저 모든 아이템의 조회 상태 업데이트
            for item in story_group.news:
                story_key = f"{ticker}_{item.type}_{item.id}"
                item.is_viewed = story_key in viewed_stories
                if not item.is_viewed:
                    has_unviewed = True

            # 아이템 정렬: 조회된 것 먼저, 그 다음 날짜순 (최신순)
            story_group.news.sort(key=lambda x: (-x.is_viewed, -x.date.timestamp()))

            # 그룹 레벨 조회 상태 업데이트
            story_group.is_viewed = not has_unviewed

        # 그룹 정렬: 미조회 그룹 먼저, 그 다음 가장 최근 스토리 날짜순
        stories_data.sort(
            key=lambda x: (
                x.is_viewed,  # 미조회 그룹 먼저
                -max([item.date.timestamp() for item in x.news] if x.news else [0]),  # 그 다음 가장 최근 스토리 날짜순
            )
        )

        return stories_data

    def etf_news_detail(
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

        holdings = self.db._select(
            table="etf_top_holdings",
            columns=["holding_ticker"],
            ticker=ticker,
        )

        if not holdings:
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
            user_id = f"auth_{user['uid']}"
        else:
            # For anonymous users, use a cookie-based ID
            user_id = f"anon_{story_cache.get_or_create_anonymous_id(request, response)}"

        # Mark the story as viewed in Redis
        story_cache.mark_story_as_viewed(user_id=user_id, ticker=ticker, story_type=type, story_id=id)

        return True


def get_news_service() -> NewsService:
    return NewsService()
