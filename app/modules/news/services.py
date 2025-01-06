from datetime import datetime
import math
from typing import List
from app.core.exception.custom import DataNotFoundException
from app.modules.disclosure.mapping import document_type_mapping

import numpy as np
import pandas as pd
from app.modules.news.schemas import (
    DisclosureRenewalItem,
    NewsDetailItem,
    NewsRenewalItem,
    TopStoriesItem,
    TopStoriesResponse,
)
from app.database.crud import database
from app.utils.ctry_utils import check_ticker_country_len_2


class NewsService:
    def __init__(self):
        self.db = database

    @staticmethod
    def _process_dataframe_news(df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 전처리 및 필터링"""

        df = df.dropna(subset=["emotion"]).sort_values(by=["date"], ascending=[False])
        df = df[df["title"].str.strip() != ""]  # titles가 "" 인 경우 행 삭제

        df["emotion"] = np.where(
            df["emotion"] == "긍정",
            "positive",
            np.where(df["emotion"] == "부정", "negative", np.where(df["emotion"] == "중립", "neutral", df["emotion"])),
        )
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

        df["emotion"] = np.where(
            df["emotion"] == "POSITIVE",
            "positive",
            np.where(
                df["emotion"] == "NEGATIVE", "negative", np.where(df["emotion"] == "NEUTRAL", "neutral", df["emotion"])
            ),
        )
        df["ctry"] = np.where(df["ctry"] == "KR", "kr", np.where(df["ctry"] == "US", "us", df["ctry"]))

        df["key_points"] = df["key_points"].str.replace(r'[\[\]"]', "", regex=True)

        return df

    def news_main(self, ctry: str = None) -> List[NewsRenewalItem]:
        condition = {"is_exist": True}
        if ctry:
            if ctry == "kr":
                condition["ctry"] = "KR"
            elif ctry == "us":
                condition["ctry"] = "US"

        df_news = pd.DataFrame(
            self.db._select(
                table="news_information",
                columns=[
                    "id",
                    "ticker",
                    "ko_name",
                    "en_name",
                    "ctry",
                    "date",
                    "title",
                    "summary",
                    "emotion",
                    "that_time_price",
                ],
                order="date",
                ascending=False,
                limit=100,
                **condition,
            )
        )
        df_news = self._process_dataframe_news(df_news)
        unique_ticker = df_news["ticker"].unique().tolist()

        df_price = pd.DataFrame(
            self.db._select(
                table="stock_trend",
                columns=["ticker", "current_price", "change_rt"],
                **dict(ticker__in=unique_ticker),
            )
        )
        df_news["price_impact"] = 0.0
        df_news["change_rate"] = 0.0
        if not df_price.empty:
            df_news = pd.merge(df_news, df_price, on="ticker", how="left")

            df_news["that_time_price"] = df_news["that_time_price"].fillna(0)
            df_news["current_price"] = df_news["current_price"].fillna(0)
            df_news["change_rt"] = df_news["change_rt"].fillna(0)

            mask = (df_news["current_price"] == 0) & (df_news["that_time_price"] != 0)
            df_news.loc[mask, "that_time_price"] = 0

            df_news["price_impact"] = round(
                (df_news["current_price"] - df_news["that_time_price"]) / df_news["that_time_price"] * 100,
                2,
            )
            df_news["change_rate"] = round(df_news["change_rt"], 2)

        df_news["price_impact"] = round(df_news["price_impact"].replace([np.inf, -np.inf, np.nan], 0), 2)
        data = []
        for _, row in df_news.iterrows():
            id = row["id"]
            date = row["date"]
            ctry = row["ctry"]
            title = row["title"]
            summary = row["summary"]
            emotion = row["emotion"]
            name = row["ko_name"]
            change_rate = row["change_rate"] if row.get("change_rate") else 0.00
            price_impact = row["price_impact"] if row.get("price_impact") else 0.00
            ticker = row["ticker"]
            data.append(
                NewsRenewalItem(
                    id=id,
                    date=date,
                    ctry=ctry,
                    title=title,
                    summary=summary,
                    emotion=emotion,
                    name=name,
                    change_rate=change_rate,
                    price_impact=price_impact,
                    ticker=ticker,
                )
            )

        return data

    def disclosure_main(self, ctry: str = None) -> List[DisclosureRenewalItem]:
        condition = {"is_exist": True}
        if ctry:
            if ctry == "kr":
                condition["ctry"] = "KR"
            elif ctry == "us":
                condition["ctry"] = "US"

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

        df_disclosure = self._process_dataframe_disclosure(df_disclosure)
        unique_ticker = df_disclosure["ticker"].unique().tolist()

        df_price = pd.DataFrame(
            self.db._select(
                table="stock_trend",
                columns=["ticker", "current_price", "change_rt"],
                order="last_updated",
                ascending=False,
                **dict(ticker__in=unique_ticker),
            )
        )
        df_disclosure["price_impact"] = 0.0
        df_disclosure["change_rate"] = 0.0
        if not df_price.empty:
            df_disclosure = pd.merge(df_disclosure, df_price, on="ticker", how="left")

            df_disclosure["that_time_price"] = df_disclosure["that_time_price"].fillna(0)
            df_disclosure["current_price"] = df_disclosure["current_price"].fillna(0)
            df_disclosure["change_rt"] = df_disclosure["change_rt"].fillna(0)

            mask = (df_disclosure["current_price"] == 0) & (df_disclosure["that_time_price"] != 0)
            df_disclosure.loc[mask, "that_time_price"] = 0

            df_disclosure["price_impact"] = round(
                (df_disclosure["current_price"] - df_disclosure["that_time_price"])
                / df_disclosure["that_time_price"]
                * 100,
                2,
            )
            df_disclosure["change_rate"] = round(df_disclosure["change_rt"], 2)

        df_disclosure["price_impact"] = round(df_disclosure["price_impact"].replace([np.inf, -np.inf, np.nan], 0), 2)

        data = []
        for _, row in df_disclosure.iterrows():
            id = row["id"]
            date = row["date"]
            ctry = row["ctry"]
            ticker = row["ticker"]
            title = row["ko_name"] + " " + document_type_mapping.get(row.form_type, row.form_type)
            summary = row["summary"]
            impact_reason = row["impact_reason"]
            key_points = row["key_points"]
            emotion = row["emotion"]
            name = row["ko_name"]
            change_rate = row["change_rate"] if row.get("change_rate") else 0.00
            price_impact = row["price_impact"] if row.get("price_impact") else 0.00
            document_url = row["url"]
            data.append(
                DisclosureRenewalItem(
                    id=id,
                    date=date,
                    ctry=ctry,
                    title=title,
                    summary=summary,
                    impact_reason=impact_reason,
                    key_points=key_points,
                    emotion=emotion,
                    name=name,
                    change_rate=change_rate,
                    price_impact=price_impact,
                    document_url=document_url,
                    ticker=ticker,
                )
            )

        return data

    def top_stories(self):
        condition = {"is_top_story": 1}
        # 뉴스 데이터 수집
        df_news = pd.DataFrame(
            self.db._select(
                table="news_information",
                columns=[
                    "ticker",
                    "ko_name",
                    "en_name",
                    "ctry",
                    "date",
                    "title",
                    "summary",
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

        # 공시 데이터 수집
        df_disclosure = pd.DataFrame(
            self.db._select(
                table="disclosure_information",
                columns=[
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
                + df_disclosure.apply(lambda x: document_type_mapping.get(x.form_type, x.form_type), axis=1)
            )
            df_disclosure["summary"] = df_disclosure.apply(
                lambda x: f"{x.summary} {x.key_points}" if x.key_points else x.summary, axis=1
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

        print(f"total_df: {total_df}")

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

            def calculate_price_impact(row):
                if row["current_price"] == 0 or row["that_time_price"] == 0:
                    return 0
                return round((row["current_price"] - row["that_time_price"]) / row["that_time_price"] * 100, 2)

            total_df["price_impact"] = total_df.apply(calculate_price_impact, axis=1)

            # 무한값과 NaN을 0으로 대체
            total_df["price_impact"] = total_df["price_impact"].replace([np.inf, -np.inf, np.nan], 0)

        # 결과 생성
        result = []
        for ticker in unique_tickers:
            ticker_news = total_df[total_df["ticker"] == ticker]
            if ticker_news.empty:
                continue

            news_items = []
            for _, row in ticker_news.iterrows():
                price_impact = float(row["price_impact"]) if pd.notnull(row["price_impact"]) else 0.0
                news_items.append(
                    TopStoriesItem(
                        price_impact=price_impact,
                        date=row["date"],
                        title=row["title"],
                        summary=row["summary"],
                        emotion=row["emotion"],
                        type=row["type"],
                    )
                )

            result.append(
                TopStoriesResponse(
                    name=ticker_news.iloc[0]["ko_name"],
                    ticker=ticker,
                    logo_image="추후 반영",
                    ctry=ticker_news.iloc[0]["ctry"],
                    current_price=ticker_news.iloc[0]["current_price"]
                    if ticker_news.iloc[0].get("current_price")
                    else 0.0,
                    change_rate=ticker_news.iloc[0]["change_1m"] if ticker_news.iloc[0].get("change_1m") else 0.0,
                    items_count=len(news_items),
                    news=news_items,
                )
            )

        return result

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
                    summary=row["summary"],
                    emotion=row["emotion"],
                    price_impact=row["price_impact"],
                )
            )
        return data, total_count, total_page, offset, emotion_count, ctry


def get_news_service() -> NewsService:
    return NewsService()
