from datetime import datetime
import json
import math
import re
from typing import List, Tuple, Union

from fastapi import Request, Response

from app.core.exception.custom import DataNotFoundException
from app.modules.disclosure.mapping import document_type_mapping

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


class NewsService:
    def __init__(self):
        self.db = database

    @staticmethod
    def _process_dataframe_news(df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 전처리 및 필터링"""

        df = df.dropna(subset=["emotion"]).sort_values(by=["date"], ascending=[False])
        df = df[df["title"].str.strip() != ""]  # titles가 "" 인 경우 행 삭제

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

        df["emotion"] = df["emotion"].str.lower()
        df["ctry"] = np.where(df["ctry"] == "KR", "kr", np.where(df["ctry"] == "US", "us", df["ctry"]))

        df["key_points"] = df["key_points"].str.replace(r'[\[\]"]', "", regex=True)

        return df

    def get_renewal_data(self, ctry: str = None) -> Tuple[List[NewsRenewalItem], List[DisclosureRenewalItem]]:
        condition = {"is_exist": True}
        if ctry:
            condition["ctry"] = "KR" if ctry == "kr" else "US" if ctry == "us" else None

        join_info = lambda table: JoinInfo(  # noqa: E731
            primary_table=table,
            secondary_table="stock_trend",
            primary_column="ticker",
            secondary_column="ticker",
            columns=["current_price", "change_rt"],
            is_outer=True,
        )

        from concurrent.futures import ThreadPoolExecutor

        def fetch_data(table, columns):
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
                    "kr_name",
                    "en_name",
                    "ctry",
                    "date",
                    "title",
                    "summary",
                    "emotion",
                    "that_time_price",
                    "current_price",
                    "change_rt",
                ],
            )

            disclosure_future = executor.submit(
                fetch_data,
                "disclosure_information",
                [
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
                    "current_price",
                    "change_rt",
                ],
            )

            df_news = news_future.result()
            df_disclosure = disclosure_future.result()

        news_data = [] if df_news.empty else self._process_price_data(self._process_dataframe_news(df_news))

        disclosure_data = (
            []
            if df_disclosure.empty
            else self._process_price_data(self._process_dataframe_disclosure(df_disclosure), is_disclosure=True)
        )

        return news_data, disclosure_data

    def _process_price_data(
        self, df: pd.DataFrame, is_disclosure: bool = False
    ) -> List[Union[NewsRenewalItem, DisclosureRenewalItem]]:
        if df.empty:
            return []

        numeric_columns = ["that_time_price", "current_price", "change_rt"]
        df[numeric_columns] = df[numeric_columns].astype("float64").fillna(0)

        mask = (df["current_price"] == 0) & (df["that_time_price"] != 0)
        df.loc[mask, "that_time_price"] = 0

        df["price_impact"] = np.where(
            df["that_time_price"] != 0, (df["current_price"] - df["that_time_price"]) / df["that_time_price"] * 100, 0
        )

        df["price_impact"] = df["price_impact"].round(2).fillna(0)
        df["change_rate"] = df["change_rt"].round(2)

        if is_disclosure:
            df["ko_name"] = df["ko_name"].apply(self.remove_parentheses)
            return [
                DisclosureRenewalItem(
                    id=row["id"],
                    date=row["date"],
                    ctry=row["ctry"],
                    ticker=row["ticker"],
                    title=f"{row['ko_name']} {document_type_mapping.get(row['form_type'], row['form_type'])}",
                    summary=row["summary"],
                    impact_reason=row["impact_reason"],
                    key_points=row["key_points"],
                    emotion=row["emotion"],
                    name=row["ko_name"],
                    change_rate=row["change_rate"],
                    price_impact=row["price_impact"],
                    document_url=row["url"],
                )
                for _, row in df.iterrows()
            ]

        return [
            NewsRenewalItem(
                id=row["id"],
                date=row["date"],
                ctry=row["ctry"],
                title=row["title"],
                summary=row["summary"],
                emotion=row["emotion"],
                name=row["kr_name"],
                change_rate=row["change_rate"],
                price_impact=row["price_impact"],
                ticker=row["ticker"],
            )
            for _, row in df.iterrows()
        ]

    def top_stories(self, request: Request):
        viewed_stories = set()
        if request.cookies.get("viewed_stories"):
            cookie_data = request.cookies.get("viewed_stories", "[]")
            try:
                viewed_stories = set(json.loads(cookie_data))
            except json.JSONDecodeError:
                viewed_stories = set()

        condition = {"is_top_story": 1, "is_exist": True}
        # 뉴스 데이터 수집
        df_news = pd.DataFrame(
            self.db._select(
                table="news_analysis",
                columns=[
                    "id",
                    "ticker",
                    "kr_name",
                    "en_name",
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
                limit=100,
                **condition,
            )
        )
        if not df_news.empty:
            df_news = self._process_dataframe_news(df_news)
            df_news["type"] = "news"
            df_news = df_news.rename(columns={"kr_name": "ko_name"})

        # 공시 데이터 수집
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
                order="date",
                ascending=False,
                limit=100,
                **condition,
            )
        )
        if not df_disclosure.empty:
            df_disclosure = self._process_dataframe_disclosure(df_disclosure)
            df_disclosure["title"] = (
                df_disclosure["ko_name"]
                + " "
                + df_disclosure["form_type"].map(document_type_mapping).fillna(df_disclosure["form_type"])
            )
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

        # 종목 현재가 정보 수집
        unique_tickers = total_df["ticker"].unique().tolist()
        df_price = pd.DataFrame(
            self.db._select(
                table="stock_trend",
                columns=["ticker", "current_price", "change_1m"],
                **{"ticker__in": unique_tickers},
            )
        )
        total_df["price_impact"] = 0.0

        if not df_price.empty:
            total_df = pd.merge(total_df, df_price, on="ticker", how="left")
            total_df["current_price"] = total_df["current_price"].fillna(total_df["that_time_price"])
            total_df["change_1m"] = total_df["change_1m"].fillna(0.0)
            total_df["that_time_price"] = total_df["that_time_price"].fillna(0.0)

            mask = (total_df["current_price"] != 0) & (total_df["that_time_price"] != 0)
            total_df["price_impact"] = 0.0
            total_df.loc[mask, "price_impact"] = (
                (total_df.loc[mask, "current_price"] - total_df.loc[mask, "that_time_price"])
                / total_df.loc[mask, "that_time_price"]
                * 100
            ).round(2)

        # 결과 생성
        result = []
        for ticker in unique_tickers:
            ticker_news = total_df[total_df["ticker"] == ticker]
            if ticker_news.empty:
                continue

            news_items = []
            ticker_has_unviewed = False
            for _, row in ticker_news.iterrows():
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
                        title=self.remove_parentheses(row["title"]),
                        summary=row["summary"],
                        impact_reason=row["impact_reason"],
                        key_points=row["key_points"],
                        emotion=row["emotion"],
                        type=row["type"],
                        is_viewed=is_viewed,
                    )
                )
            ko_name = self.remove_parentheses(ticker_news.iloc[0]["ko_name"])
            result.append(
                TopStoriesResponse(
                    name=ko_name,
                    ticker=ticker,
                    logo_image="추후 반영",
                    ctry=ticker_news.iloc[0]["ctry"],
                    current_price=ticker_news.iloc[0]["current_price"]
                    if ticker_news.iloc[0].get("current_price")
                    else 0.0,
                    change_rate=ticker_news.iloc[0]["change_1m"] if ticker_news.iloc[0].get("change_1m") else 0.0,
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

    def news_detail_v2(self, ticker: str, date: str = None, page: int = 1, size: int = 6):
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
                **condition,
            )
        )
        if df_news.empty:
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
        df_news["that_time_price"] = df_news["that_time_price"].fillna(0.0)

        df_price = pd.DataFrame(
            self.db._select(
                table="stock_trend",
                columns=["ticker", "current_price"],
                **{"ticker": ticker},
            )
        )

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
