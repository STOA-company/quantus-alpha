from functools import lru_cache
from typing import Dict, Optional, List
import pytz
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from app.core.exception.custom import DataNotFoundException
from app.modules.common.utils import check_ticker_country_len_2, check_ticker_country_len_3
from app.modules.news.schemas import LatestNewsResponse, NewsItem
from quantus_aws.common.configs import s3_client
from app.database.crud import database
from app.core.logging.config import get_logger


KST_TIMEZONE = pytz.timezone("Asia/Seoul")
NEWS_CONTRY_MAP = {
    "kr": "KR",
    "us": "US",
    "jp": "JP",
    "hk": "HK",
}

logger = get_logger(__name__)


class NewsService:
    def __init__(self):
        self._bucket_name = "quantus-news"
        self.db = database

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

    def _create_news_items(self, df: pd.DataFrame, include_stock_info: bool = False) -> List[NewsItem]:
        """DataFrame을 NewsItem 리스트로 변환"""
        result = []
        for _, row in df.iterrows():
            news_item = {
                "date": pd.to_datetime(row["date"]) if not isinstance(row["date"], datetime) else row["date"],
                "title": row["titles"],
                "summary": row["summary"] if pd.notna(row["summary"]) else None,
                "emotion": row["emotion"].lower() if pd.notna(row["emotion"]) else None,
                "name": None,
                "change_rate": None,
            }

            if include_stock_info:
                news_item.update(
                    {
                        "name": row["Name"] if pd.notna(row["Name"]) else "None",
                        "change_rate": float(row["change_rate"]) if pd.notna(row["change_rate"]) else 0.00,
                    }
                )

            # NewsItem 객체로 변환
            result.append(NewsItem(**news_item))

        return result

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

    @staticmethod
    def get_current_market_country() -> str:
        """
        현재 시간 기준으로 활성화된 시장 국가 반환
        한국 시간 07:00-19:00 -> 한국 시장
        한국 시간 19:00-07:00 -> 미국 시장
        """
        current_time = datetime.now(KST_TIMEZONE)
        current_hour = current_time.hour

        if 7 <= current_hour < 19:
            return "kr"
        else:
            return "us"

    @staticmethod
    def _format_date(date_str):  # TODO: 홈 - 뉴스데이터 로직 수정 필요
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        # 하루 전 날짜로 변경
        prev_day = date_obj - timedelta(days=1)
        return prev_day.strftime("%Y-%m-%d")

    def _get_stock_info(self, df: pd.DataFrame, ctry: str, date_str: str) -> pd.DataFrame:
        """주식 정보(종목명, 가격변화율) 조회"""
        table_name = f"stock_{ctry}_1d"

        tickers = df["Code"].unique().tolist()
        logger.debug(f"Querying tickers: {tickers}")

        if ctry == "kr":
            tickers = [f"A{ticker}" for ticker in tickers]

        stock_data = self.db._select(
            table=table_name, columns=["Date", "Ticker", "Open", "Close", "Name"], Date=date_str, Ticker__in=tickers
        )

        logger.debug(f"stock_data: {stock_data}")

        if not stock_data:
            logger.warning(f"No stock data found for tickers: {tickers} on date: {date_str}")
            # DataFrame에 새 컬럼 추가
            df_copy = df.copy()
            df_copy["Name"] = "None"
            df_copy["change_rate"] = 0.00
            return df_copy

        # 결과를 리스트 형태로 변환
        stock_data_list = [
            {"Date": row[0], "Ticker": row[1], "Open": float(row[2]), "Close": float(row[3]), "Name": str(row[4])}
            for row in stock_data
        ]

        # 리스트를 DataFrame으로 변환
        stock_df = pd.DataFrame(stock_data_list)

        stock_df["change_rate"] = round((stock_df["Close"] - stock_df["Open"]) / stock_df["Open"] * 100, 2)

        if ctry == "kr":
            stock_df["Ticker"] = stock_df["Ticker"].str[1:]

        # 원본 DataFrame 복사 후 병합
        df_copy = df.copy()
        merged_df = pd.merge(
            df_copy, stock_df[["Ticker", "change_rate"]], left_on=["Code"], right_on=["Ticker"], how="left"
        ).drop(columns=["Ticker"])

        # 누락된 데이터 처리
        merged_df["Name"] = merged_df["Name"].fillna("None")
        merged_df["change_rate"] = merged_df["change_rate"].fillna(0.00)

        return merged_df

    async def get_news(
        self, page: int, size: int, ticker: Optional[str] = None, date: Optional[str] = None
    ) -> Dict[str, any]:
        """뉴스 데이터 조회"""
        if page < 1:
            raise ValueError("Page number must be greater than 0")
        if size < 1:
            raise ValueError("Page size must be greater than 0")
        # 시간대에 맞춘 ctry 기본값 설정
        ctry = self.get_current_market_country()

        if ticker:
            ctry = check_ticker_country_len_2(ticker)
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

        if not ticker:
            formatted_date = self._format_date(date_str)
            df_paged = self._get_stock_info(df_paged, ctry, formatted_date)
            news_items = self._create_news_items(df_paged, include_stock_info=True)
        else:
            news_items = self._create_news_items(df_paged, include_stock_info=False)
        # 결과 반환
        return {
            "total_count": total_records,
            "total_pages": (total_records + size - 1) // size,
            "current_page": page,
            "offset": start_idx,
            "size": size,
            "data": news_items,
            "ctry": ctry,
            **emotion_counts,
        }

    async def get_latest_news(self, ticker: str) -> LatestNewsResponse:
        """최신 뉴스와 공시 데이터 중 최신 데이터 조회"""
        try:
            # 1. 공시 데이터 조회
            disclosure_info = await self._get_disclosure_data(ticker)

            # 2. 뉴스 데이터 조회
            news_info = await self._get_news_data(ticker)

            # 3. 둘 다 없으면 에러
            if not disclosure_info and not news_info:
                raise DataNotFoundException(ticker=ticker, data_type="news and disclosure")

            # 4. 날짜 비교하여 최신 데이터 선택
            result = self._select_latest_data(disclosure_info, news_info)

            return LatestNewsResponse(**result)

        except Exception as e:
            logger.error(f"Error in get_latest_news for ticker {ticker}: {str(e)}")
            raise

    def _parse_key_points(self, key_points: list) -> str:
        """key_points 리스트를 파싱하여 작은따옴표만 큰따옴표로 변경"""
        try:
            # 리스트의 각 항목을 큰따옴표로 감싸기
            quoted_items = [f"{item}" for item in key_points]
            # 대괄호로 감싸서 반환
            return "[" + " ".join(quoted_items) + "]"
        except Exception as e:
            logger.error(f"Error parsing key points: {str(e)}")
            return str(key_points)

    async def _get_disclosure_data(self, ticker: str) -> Optional[Dict]:
        """공시 데이터 조회 및 분석 데이터 함께 반환"""
        try:
            # 공시 기본 데이터 조회
            year = datetime.now().strftime("%Y")
            ctry = check_ticker_country_len_3(ticker)

            disclosure_data = self.db._select(
                table=f"{ctry}_disclosure",
                columns=["filing_id", "filing_date"],
                order="filing_date",
                ascending=False,
                limit=1,
                ticker=ticker,
                filing_date__like=f"{year}%",
                ai_processed=1,
            )

            if not disclosure_data:
                return None

            # 분석 데이터 조회
            analysis_data = self.db._select(
                table=f"{ctry}_disclosure_analysis",
                columns=["ai_summary", "market_impact", "impact_reason", "key_points", "translated"],
                filing_id=disclosure_data[0][0],
            )

            if not analysis_data:
                return None

            analysis_data = list(analysis_data[0])
            if analysis_data[-1]:
                translated_data = self.db._select(
                    table=f"{ctry}_disclosure_analysis_translation",
                    columns=["ai_summary", "key_points"],
                    filing_id=disclosure_data[0][0],
                )
                analysis_data[0] = translated_data[0][0]
                analysis_data[3] = translated_data[0][1]
            print(f"analysis_data::::: {analysis_data[3]}")
            key_points_parsed = self._parse_key_points(analysis_data[3])
            content = f"{analysis_data[0]} {key_points_parsed}"
            print(f"content::::: {content}")
            return {"date": disclosure_data[0][1], "content": content, "type": "disclosure"}

        except Exception as e:
            logger.error(f"Error fetching disclosure data: {str(e)}")
            return None

    async def _get_news_data(self, ticker: str) -> Optional[Dict]:
        """뉴스 데이터 조회"""
        try:
            ctry = check_ticker_country_len_2(ticker)
            processed_ticker = self._ticker_preprocess(ticker, ctry)

            # S3 데이터 조회
            s3_data = await self._fetch_s3_data(self._get_current_date(), f"merged_data/{NEWS_CONTRY_MAP[ctry]}")

            if not s3_data:
                return None

            # DataFrame 처리
            df = pd.read_parquet(pd.io.common.BytesIO(s3_data))
            df = self._process_dataframe(df, processed_ticker)

            if df.empty:
                return None

            latest_news = df.iloc[0]
            return {"date": latest_news["date"], "content": latest_news["summary"], "type": "news"}

        except Exception as e:
            logger.error(f"Error fetching news data: {str(e)}")
            return None

    def _select_latest_data(self, disclosure_info: Optional[Dict], news_info: Optional[Dict]) -> Dict:
        """두 데이터 중 최신 데이터 선택"""
        if not disclosure_info:
            if news_info:
                # 뉴스 데이터의 content 처리
                news_info["content"] = self._parse_news_content(news_info["content"])
            return news_info
        if not news_info:
            return disclosure_info

        disclosure_date = pd.to_datetime(disclosure_info["date"])
        news_date = pd.to_datetime(news_info["date"])

        result = disclosure_info if disclosure_date > news_date else news_info

        # 뉴스인 경우 content 처리
        if result["type"] == "news":
            result["content"] = self._parse_news_content(result["content"])

        return result

    def _parse_news_content(self, content: str) -> str:
        """뉴스 content에서 기사 요약 부분만 추출하고 정리"""
        try:
            # "기사 요약" 섹션 추출
            if "**기사 요약**" in content:
                summary_section = content.split("**기사 요약**")[1].split("**주가에")[0]

                # 불필요한 문자 제거
                cleaned_content = summary_section.replace("\n", "")
                # 연속된 공백을 하나로
                cleaned_content = " ".join(cleaned_content.split())
                return cleaned_content.strip(" :")
            return content
        except Exception as e:
            logger.error(f"Error parsing news content: {str(e)}")
            return content


def get_news_service() -> NewsService:
    return NewsService()
