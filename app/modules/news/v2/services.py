import math
from datetime import datetime, time, timedelta

import pandas as pd
import pytz

from app.common.constants import UTC
from app.core.exception.custom import DataNotFoundException
from app.database.crud import database
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from app.modules.news.v2.schemas import (
    NewsDetailItemV2,
)
from app.utils.ctry_utils import check_ticker_country_len_2


class NewsService:
    def __init__(self):
        self.db = database

    @staticmethod
    def _count_emotion(df: pd.DataFrame) -> dict:
        return df["emotion"].value_counts().to_dict()

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
        df_news["that_time_price"] = df_news["that_time_price"].fillna(0.0)

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
                )
            )
        return data, total_count, total_page, offset, emotion_count, ctry


def get_news_service() -> NewsService:
    return NewsService()
