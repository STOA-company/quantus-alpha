from functools import lru_cache
from typing import Dict, Optional, List
import pytz
from datetime import datetime
import pandas as pd
import numpy as np
from app.core.exception.custom import DataNotFoundException
from app.modules.common.utils import check_ticker_contry_len_2
from app.modules.news.schemas import NewsItem
from quantus_aws.common.configs import s3_client

KST_TIMEZONE = pytz.timezone("Asia/Seoul")
NEWS_CONTRY_MAP = {
    "kr": "KR",
    "us": "US",
    "jp": "JP",
    "hk": "HK",
}


class NewsService:
    def __init__(self):
        self._bucket_name = "quantus-news"

    async def _fetch_s3_data(self, date_str: str, country_path: str) -> Optional[bytes]:
        """S3에서 데이터를 가져오는 내부 메서드"""
        try:
            file_path = f"{country_path}/{date_str}.parquet"
            response = s3_client.get_object(Bucket=self._bucket_name, Key=file_path)
            return response["Body"].read()
        except Exception:
            return None

    @staticmethod
    def _process_dataframe(df: pd.DataFrame, ticker: Optional[str] = None) -> pd.DataFrame:
        """DataFrame 전처리 및 필터링"""
        if ticker:
            df = df[df["Code"] == ticker]

        df = df.dropna(subset=["emotion"]).sort_values(by=["date"], ascending=[False])

        df["emotion"] = np.where(
            df["emotion"] == "긍정",
            "positive",
            np.where(df["emotion"] == "부정", "negative", np.where(df["emotion"] == "중립", "neutral", df["emotion"])),
        )

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

    @staticmethod
    def _create_news_items(df: pd.DataFrame) -> List[NewsItem]:
        """DataFrame을 NewsItem 리스트로 변환"""
        return [
            NewsItem(
                date=pd.to_datetime(row["date"]) if not isinstance(row["date"], datetime) else row["date"],
                title=row["titles"],
                summary=row["summary"] if pd.notna(row["summary"]) else None,
                emotion=row["emotion"] if pd.notna(row["emotion"]) else None,
            )
            for _, row in df.iterrows()
        ]

    @staticmethod
    @lru_cache(maxsize=1)
    def _get_current_date() -> str:
        """현재 KST 날짜를 가져오는 캐시된 메서드"""
        return datetime.now(KST_TIMEZONE).strftime("%Y%m%d")

    @staticmethod
    def _ticker_preprocess(ticker: str, ctry: str) -> str:
        if ctry == "us":
            return ticker
        elif ctry == "kr":
            return ticker[1:]
        elif ctry == "jp":
            return ticker[1:]
        elif ctry == "hk":
            return ticker[2:]

    async def get_news(
        self, page: int, size: int, ticker: Optional[str] = None, date: Optional[str] = None
    ) -> Dict[str, any]:
        """뉴스 데이터 조회"""
        if page < 1:
            raise ValueError("Page number must be greater than 0")
        if size < 1:
            raise ValueError("Page size must be greater than 0")

        ctry = check_ticker_contry_len_2(ticker)

        # 티커 전처리
        if ticker:
            ticker = self._ticker_preprocess(ticker, ctry)

        # 날짜 및 경로 설정
        date_str = date or self._get_current_date()
        country_path = f"merged_data/{NEWS_CONTRY_MAP[ctry]}"

        # S3 데이터 가져오기
        s3_data = await self._fetch_s3_data(date_str, country_path)
        if s3_data is None:
            raise DataNotFoundException(ticker=ticker or "all", data_type="news")

        # DataFrame 처리
        df = pd.read_parquet(pd.io.common.BytesIO(s3_data))
        df = self._process_dataframe(df, ticker)

        # 감정 카운트 및 페이지네이션 처리
        emotion_counts = self._count_emotions(df)
        total_records = len(df)
        start_idx = (page - 1) * size
        df_paged = df.iloc[start_idx : start_idx + size]
        news_items = self._create_news_items(df_paged)

        # 결과 반환
        return {
            "total_count": total_records,
            "total_pages": (total_records + size - 1) // size,
            "current_page": page,
            "offset": start_idx,
            "size": size,
            "data": news_items,
            **emotion_counts,
        }


def get_news_service() -> NewsService:
    return NewsService()
