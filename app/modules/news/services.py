from functools import lru_cache
from typing import Dict, Optional, List
import pytz
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import text
from app.core.exception.custom import DataNotFoundException
from app.modules.common.utils import check_ticker_country_len_2, check_ticker_country_len_3
from app.modules.news.schemas import LatestNewsResponse, NewsItem, TopStoriesResponse
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

    def _fetch_s3_data(self, date_str: str, country_path: str) -> Optional[bytes]:
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

        kr_columns = ["Date", "Ticker", "Open", "Close", "Name"]
        us_columns = ["Date", "Ticker", "Open", "Close"]

        if ctry == "kr":
            db_columns = kr_columns
        else:
            db_columns = us_columns

        while True:
            stock_data = self.db._select(table=table_name, columns=db_columns, Date=date_str, Ticker__in=tickers)
            if stock_data:
                break
            else:
                # 문자열 날짜를 datetime으로 변환하여 계산
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                date_obj = date_obj - timedelta(days=1)
                date_str = date_obj.strftime("%Y-%m-%d")

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
            {
                "Date": row[0],
                "Ticker": row[1],
                "Open": float(row[2]),
                "Close": float(row[3]),
                "Name": str(row[4] if ctry == "kr" else None),
            }
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

    def get_news(self, page: int, size: int, ticker: Optional[str] = None, date: Optional[str] = None) -> Dict[str, any]:
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
        s3_data = self._fetch_s3_data(date_str, country_path)
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

    def get_latest_news(self, ticker: str) -> LatestNewsResponse:
        """최신 뉴스와 공시 데이터 중 최신 데이터 조회"""
        try:
            # 1. 공시 데이터 조회
            disclosure_info = self._get_disclosure_data(ticker)

            # 2. 뉴스 데이터 조회
            news_info = self._get_news_data(ticker)

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

    def _get_disclosure_data(self, ticker: str) -> Optional[Dict]:
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
            key_points_parsed = self._parse_key_points(analysis_data[3])
            content = f"{analysis_data[0]} {key_points_parsed}"
            return {"date": disclosure_data[0][1], "content": content, "type": "disclosure"}

        except Exception as e:
            logger.error(f"Error fetching disclosure data: {str(e)}")
            return None

    def _get_news_data(self, ticker: str) -> Optional[Dict]:
        """뉴스 데이터 조회"""
        try:
            ctry = check_ticker_country_len_2(ticker)
            processed_ticker = self._ticker_preprocess(ticker, ctry)

            # S3 데이터 조회
            s3_data = self._fetch_s3_data(self._get_current_date(), f"merged_data/{NEWS_CONTRY_MAP[ctry]}")

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

        # 1. 최신 뉴스 1주치 데이터 조회
        # 2. 최신 공시 1주치 데이터 조회
        # 3. 뉴스는 date, 공시는 filing_date로 outer 병합
        # 4. response에 보내질 ticker를 넣을 빈리스트 생성
        # 5. 빈리스트에 최신부터 유니크한 티커 11개 추가
        # 6. 최신부터 11개의 종목 조회 11개가 안되면 11개 될때까지 ticker 조회
        # 7. 해당 종목의 2주치 뉴스, 공시 데이터 ticker별로 분류
        # 8. 해당 종목의 주가 데이터 조회
        # 9. 각 종목별 뉴스, 공시 데이터와 가격 데이터 병합

    def _get_news_data_from_s3(self, date_str: str, country: str) -> pd.DataFrame:
        s3_data = self._fetch_s3_data(date_str, f"merged_data/{country}")
        if s3_data is None:
            raise DataNotFoundException(ticker="all", data_type="news")
        news_df = pd.read_parquet(pd.io.common.BytesIO(s3_data))
        news_df = self._process_dataframe(news_df)
        news_df = news_df.rename(columns={"titles": "title"})
        return news_df

    def _get_max_dates_kr_us(self) -> dict:
        """한국과 미국 주가 데이터의 가장 최신 날짜를 한 번에 조회"""
        query = text("""
            SELECT country, max_date FROM (
                SELECT 'kr' as country, MAX(Date) as max_date FROM stock_kr_1d
                UNION ALL
                SELECT 'us' as country, MAX(Date) as max_date FROM stock_us_1d
            ) as dates
        """)
        result = self.db._execute(query).fetchall()

        return {row[0]: row[1].strftime("%Y-%m-%d") for row in result}

    def get_top_stories(self) -> TopStoriesResponse:
        # # 시장 및 날짜 설정
        # current_date = datetime.now(KST_TIMEZONE) - timedelta(days=1)
        # date_str = current_date.strftime("%Y%m%d")
        # db_date = current_date.strftime("%Y-%m-%d")
        # max_date = self._get_max_dates_kr_us()  # db hit
        # kr_max_date = max_date['kr']
        # us_max_date = max_date['us']
        # # 1. S3에서 뉴스 데이터 조회
        # kor_today_news = self._get_news_data_from_s3(date_str, "KR") # s3 hit
        # us_today_news = self._get_news_data_from_s3(date_str, "US") # s3 hit

        # us_today_disclosure_join_info = JoinInfo(
        #     primary_table="usa_disclosure",
        #     secondary_table="usa_disclosure_analysis",
        #     primary_column="filing_id",
        #     secondary_column="filing_id",
        #     columns=["filing_id", "ai_summary", "market_impact", "key_points"],
        #     is_outer=True,
        # )
        # us_today_disclosure = self.db._select(  # db hit
        #     table="usa_disclosure",
        #     columns=["filing_id", "company_name", "form_type", "ticker", "filing_date"]
        #     + ["filing_id", "ai_summary", "market_impact", "key_points"],
        #     order="filing_date",
        #     ascending=False,
        #     join_info=us_today_disclosure_join_info,
        #     filing_date=db_date,
        #     ai_processed=1,
        # )

        # news_df = pd.concat([kor_today_news, us_today_news], ignore_index=True)
        # news_df = news_df[["Code", "Name", "date", "title", "summary", "emotion"]]
        # news_df["type"] = "news"

        # total_df = news_df

        # if us_today_disclosure:
        #     df_us_disclosure = pd.DataFrame(us_today_disclosure)
        #     df_us_disclosure = df_us_disclosure.rename(
        #         columns={"filing_date": "date", "ticker": "Code", "company_name": "Name"}
        #     )
        #     df_us_disclosure["type"] = "disclosure"
        #     df_us_disclosure["emotion"] = df_us_disclosure["market_impact"]
        #     df_us_disclosure["summary"] = df_us_disclosure.apply(
        #         lambda row: row["ai_summary"] + " " + self._parse_key_points(row["key_points"])
        #         if pd.notna(row["key_points"])
        #         else row["ai_summary"],
        #         axis=1,
        #     )

        #     total_df = pd.concat([news_df, df_us_disclosure], ignore_index=True)

        # total_df = total_df.sort_values(by="date", ascending=False)
        # top_tickers = []
        # seen_tickers = set()

        # for _, row in total_df.iterrows():
        #     ticker = row["Code"]
        #     if ticker not in seen_tickers:
        #         seen_tickers.add(ticker)
        #         top_tickers.append(ticker)
        #         if len(top_tickers) == 11:
        #             break

        # kr_tickers = [f"A{ticker}" for ticker in top_tickers if str(ticker).isdigit()]
        # us_tickers = [ticker for ticker in top_tickers if not str(ticker).isdigit()]

        # result_data = []

        # if kr_tickers:
        #     kr_stock_data = self.db._select( # db hit
        #         table="stock_kr_1d", columns=["Date", "Ticker", "Open", "Close"], Date=kr_max_date, Ticker__in=kr_tickers
        #     )

        #     if kr_stock_data:
        #         for stock in kr_stock_data:
        #             ticker = stock[1][1:]  # Remove 'A' prefix
        #             ticker_news = total_df[total_df["Code"] == ticker]

        #             if not ticker_news.empty:
        #                 stock_info = {
        #                     "name": ticker_news.iloc[0]["Name"],
        #                     "ticker": f"A{ticker}",
        #                     "logo_image": "추후 반영",
        #                     "ctry": "kr",
        #                     "current_price": float(stock[3]),
        #                     "change_rate": round(((float(stock[3]) - float(stock[2])) / float(stock[2])) * 100, 2),
        #                     "items_count": len(ticker_news),
        #                     "news": [
        #                         TopStoriesItem(
        #                             date=row["date"],
        #                             title=row["title"],
        #                             summary=(
        #                                 self._parse_news_content(row["summary"])
        #                                 if pd.notna(row["summary"]) and row["type"] == "news"
        #                                 else (row["summary"] if pd.notna(row["summary"]) else None)
        #                             ),
        #                             emotion=row["emotion"].lower() if pd.notna(row["emotion"]) else None,
        #                             type=row["type"],
        #                         )
        #                         for _, row in ticker_news.iterrows()
        #                     ],
        #                 }
        #                 result_data.append(TopStoriesResponse(**stock_info))

        # # 미국 주식 데이터 처리
        # if us_tickers:
        #     us_stock_data = self.db._select(
        #         table="stock_us_1d", columns=["Date", "Ticker", "Open", "Close"], Date=us_max_date, Ticker__in=us_tickers
        #     )

        #     if us_stock_data:
        #         for stock in us_stock_data:
        #             ticker = stock[1]
        #             ticker_news = total_df[total_df["Code"] == ticker]

        #             if not ticker_news.empty:
        #                 stock_info = {
        #                     "name": ticker_news.iloc[0]["Name"],
        #                     "ticker": ticker,
        #                     "logo_image": "추후 반영",
        #                     "ctry": "us",
        #                     "current_price": float(stock[3]),
        #                     "change_rate": round(((float(stock[3]) - float(stock[2])) / float(stock[2])) * 100, 2),
        #                     "items_count": len(ticker_news),
        #                     "news": [
        #                         TopStoriesItem(
        #                             date=row["date"],
        #                             title=row["title"],
        #                             summary=(
        #                                 self._parse_news_content(row["summary"])
        #                                 if pd.notna(row["summary"]) and row["type"] == "news"
        #                                 else (row["summary"] if pd.notna(row["summary"]) else None)
        #                             ),
        #                             emotion=row["emotion"].lower() if pd.notna(row["emotion"]) else None,
        #                             type=row["type"],
        #                         )
        #                         for _, row in ticker_news.iterrows()
        #                     ],
        #                 }
        #                 result_data.append(TopStoriesResponse(**stock_info))

        # if not result_data:
        #     raise DataNotFoundException(ticker="all", data_type="top_stories")

        result_data = [
            {
                "name": "신세계",
                "ticker": "A004170",
                "logo_image": "추후 반영",
                "ctry": "kr",
                "current_price": 134900,
                "change_rate": 0.52,
                "items_count": 9,
                "news": [
                    {
                        "date": "2024-12-25T09:24:00",
                        "title": "'서울 대표 명소' 신세계스퀘어… 오는 31일 밤 새해 카운트다운 외친...",
                        "summary": "- 신세계백화점은 신세계스퀘어에서 '2025 카운트다운 쇼 Light Now' 축제를 진행한다고 발표했음.- 카운트다운 행사는 31일 밤 11시에 신세계백화점 본점 앞에서 열릴 예정임.- 신세계스퀘어는 명동스퀘어 프로젝트의 일환으로 조성된 초대형 사이니지로, 서울의 새해맞이 명소로 자리 잡을 계획임.- 축제는 서울중앙우체국 광장과 신세계백화점 본관 앞 분수 광장에서 진행됨.- 신세계백화점은 K-콘텐츠와 헤리티지를 결합하여 고객들에게 특별한 경험을 제공할 예정임.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:01:00",
                        "title": "새해 카운트다운 행사 인기에...근처 주차장 수요 급증",
                        "summary": "- 새해 카운트다운 행사 인기로 행사장 인근 주차장 수요가 급증했음.- 최근 3년간 주요 행사장 인근 주차장 거래액이 연평균 110% 증가했음.- 동대문디자인플라자 인근 주차권 거래건수가 전년 대비 436% 증가했음.- 신세계백화점 본점 인근 주차장 거래건수가 전년 대비 70% 이상 증가했음.- 엔데믹 이후 오프라인 새해 맞이 행사 참여가 증가하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:00:00",
                        "title": '[생활경제24]"불황 넘자"…효율화·복합화 나선 유통업계',
                        "summary": "- 2024년 오프라인 유통업계는 소비 위축으로 어려움을 겪고 있으며, 신세계와 롯데는 효율화 및 복합화 전략을 추진하고 있음.- 신세계는 이마트와 SSM의 합병을 통해 운영 효율성을 높이고, 희망퇴직을 통해 인력 구조조정을 진행했음.- 백화점업계는 신규 출점 없이 기존 점포의 리뉴얼을 통해 쇼핑몰화하고 있으며, 신세계는 경기점과 죽전점을 리뉴얼하여 새로운 브랜드로 전환했음.- 신세계그룹의 정용진 회장과 정유경 회장이 승진하며 경영 책임이 더욱 커졌음.- 신세계는 연간 흑자 전환을 목표로 하고 있으며, 계열사 지원을 통해 위기 상황을 극복하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:01:00",
                        "title": "“정용진의 No Brand아니고 ‘NOBLAND’입니다”... 매번 나...",
                        "summary": "- 노브랜드 주가는 12월 23일 장 중 9730원까지 상승했으나, 이후 하락하여 7990원으로 마감했음.- 주가 급등은 노브랜드를 신세계의 PB 브랜드 '노브랜드'로 착각한 투자자들 때문으로 분석됨.- 노브랜드는 이마트와 관련 없는 북미 의류 수출 회사임.- 유사한 이름으로 인한 주가 급등락 사례가 올해에도 여러 차례 발생했음.- 개인 투자자들이 사명만 보고 투자하는 경향이 반복되고 있음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:01:00",
                        "title": "유통街 설 선물세트, 초고가 사라졌다... ‘알뜰 구성’ 경쟁",
                        "summary": "- 유통업계가 명절 선물세트에서 고가 선물 경쟁을 중단하고 알뜰 구성으로 전환했음.- 내년 설 선물 세트는 5만원 이하의 가성비 높은 상품이 주를 이루고 있음.- 신세계백화점은 직거래 비중을 확대하여 가격을 낮추고 실속형과 프리미엄 라인을 세분화했음.- 소비자 심리가 급속히 얼어붙고 있으며, 불황형 소비가 뚜렷해지고 있음.- 전문가들은 필수 소비재를 제외한 비필수품 구매가 줄어들 것으로 예상하고 있음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T05:03:00",
                        "title": "임원 줄이고 휴가 늘리고… ‘불확실성 시대’ 기업 생존법",
                        "summary": "- 대기업 임원 승진자가 지난해보다 9.6% 감소했음.- 사장단 이상 고위급 승진자는 지난해 43명에서 올해 24명으로 줄어들었음.- 신세계 회장 정유경이 승진했으며, 부회장 승진자는 4명으로 감소했음.- 대기업들은 연말 휴가를 권장하며 비용 절감을 도모하고 있음.- 경기가 좋지 않은 상황에서 송년회 대신 조용한 분위기를 선호하고 있음.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T00:01:00",
                        "title": "다음은 누가 트럼프 만나나… 정의선·신동빈·손경식 거론",
                        "summary": "- 정용진 신세계그룹 회장이 도널드 트럼프 미국 대통령 당선인과 회동한 이후, 다음 회동 주자로 정의선 현대차그룹 회장, 신동빈 롯데그룹 회장, 손경식 CJ그룹 회장이 거론되고 있음.- 그러나 윤석열 대통령 탄핵정국으로 외교·통상 공백이 커지면서 회동 가능성은 낮다는 의견이 지배적임.- 신동빈 회장은 2019년 트럼프와의 단독 회동 이력이 있으며, 롯데그룹은 미국에서 사업을 적극 추진하고 있음.- 현대차그룹은 대미 사업을 활발히 진행하고 있으나, 관세 문제로 정의선 회장과의 회동은 어려울 것으로 보임.- CJ그룹은 대미 사업 확장을 위해 트럼프와의 네트워크를 원하고 있으나, 민간 가교론의 성과는 불확실함.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T21:11:00",
                        "title": "“여긴 크리스마스의 악몽이다”…차갑게 식은 내수경기, 백화점도 폐...",
                        "summary": "- 12월 소비자심리지수가 코로나19 팬데믹 이후 최대폭으로 하락하여 88.4를 기록했음.- 유통업계는 연말 대목에도 불구하고 매출 하락을 우려하고 있으며, 백화점 2곳이 폐업 절차에 들어갔음.- 면세점 매출이 전년 대비 20% 감소했으며, 외국인 방문객 수가 줄어들고 있음.- 저가 전문점인 다이소는 오히려 매출 상승세를 이어가고 있음.- 정치적 불확실성과 경제적 어려움이 소비심리에 부정적인 영향을 미치고 있음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T20:07:00",
                        "title": "신세계免, 아웃도어 슈즈 브랜드 ‘킨’ 단독 매장",
                        "summary": "- 신세계면세점 명동점에 글로벌 어반 아웃도어 슈즈 브랜드 ‘킨(KEEN)’의 단독 매장이 오픈했음.- 킨은 2003년 미국 포틀랜드에서 시작된 브랜드로, 이번 매장은 시내면세점 중 처음으로 개설됨.- 매장에는 다양한 제품이 진열되어 고객들에게 선보이고 있음.- 신세계면세점의 브랜드 포트폴리오 확장을 나타내는 사례임.- 아웃도어 시장의 성장 가능성을 반영하는 전략으로 해석될 수 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                ],
            },
            {
                "name": "현대차",
                "ticker": "A005380",
                "logo_image": "추후 반영",
                "ctry": "kr",
                "current_price": 216500,
                "change_rate": 0.7,
                "items_count": 9,
                "news": [
                    {
                        "date": "2024-12-25T09:17:00",
                        "title": "테슬라 주가 7% 넘게 뛰자 “투자자들 현대차로부터 크리스마스 선물.....",
                        "summary": "- 테슬라 주가가 7.35% 상승하여 462.25달러로 마감했음.- 현대차가 테슬라 충전기 어댑터를 무료로 배포한다고 발표했음.- 이 어댑터를 통해 현대차 전기차 고객들이 테슬라 슈퍼차저에서 충전할 수 있게 됨.- 현대차는 제네시스 브랜드도 NACS 어댑터 무료 제공 프로그램에 참여할 계획임.- 이로 인해 테슬라의 충전소 운영 수익에 긍정적인 영향을 미칠 것으로 예상됨.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T09:11:00",
                        "title": "현대차그룹, 美IIHS 충돌평가 ‘가장 안전한 차’ 최다선정…“고객안....",
                        "summary": "- 현대차그룹이 미국 IIHS의 충돌평가에서 2024 IIHS 톱 세이프티 픽(TSP) 등급에 K4가 최종후보에 올랐음.- 현대차그룹은 총 22개 차종이 TSP+ 또는 TSP 등급을 받아 글로벌 자동차 그룹 중 최다 선정임.- 현대차는 대중 브랜드 중 2위, 기아는 6위, 제네시스는 고급 브랜드 1위를 차지했음.- IIHS의 충돌 평가 기준이 강화되어, 현대차그룹의 안전성이 더욱 입증되었음.- 현대차그룹은 고객의 안전을 최우선으로 두고 지속적으로 노력할 것임.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:30:00",
                        "title": "‘작거나 혹은 크거나’…데뷔 앞둔 신형 SUV, ‘극과 극’ 차체 크기...",
                        "summary": "- 현대차와 기아가 다양한 크기의 SUV 모델을 출시하며 SUV 시장에서의 영향력을 확대하고 있음.- 기아는 4m 이하의 콤팩트 SUV '시로스'를 인도에서 세계 최초로 공개했으며, 차체 크기가 작지만 실내 공간은 넉넉함.- 현대차는 신형 팰리세이드를 출시하며, 전장과 휠베이스가 증가하여 넓은 화물 수납공간을 제공함.- 현대차의 대형 전기 SUV '아이오닉 9'은 동급 최대의 실내 공간과 500km 이상의 주행 가능 거리를 자랑함.- 두 회사 모두 SUV 시장에서 다양한 소비층을 겨냥한 전략을 추진하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:01:00",
                        "title": "'슈퍼앱' 꿈꾸던 티맵은 군살 빼기중…이제 경쟁상대는 현대차?",
                        "summary": "- 티맵모빌리티가 슈퍼앱으로의 변화를 포기하고 AI 기반 모빌리티 데이터 기업으로 전환하고 있음.- 올해 들어 여러 부가 서비스를 종료하고, 우버와의 합작법인 지분을 매각하는 등 사업 부문 정리를 진행하고 있음.- 티맵은 수익성 개선을 위해 부수적인 사업을 정리하고, 데이터 기반 맞춤형 서비스에 집중할 계획임.- 현대차와 같은 완성차 업체들이 티맵의 경쟁상대가 될 것으로 예상됨.- SK그룹의 경영 리밸런싱과 최태원 회장의 경영 기조가 티맵의 방향 전환에 영향을 미쳤음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:01:00",
                        "title": "‘회장님차=세단’ 공식을 깨는 SUV, 압도적 넓이와 편안함 발군 [시...",
                        "summary": "- 제네시스 GV80은 SUV 시장에서 프리미엄 모델로 자리잡고 있으며, '사장님차는 세단'이라는 공식을 깨뜨렸음.- 뛰어난 서스펜션과 편의 기능으로 편안한 주행 경험을 제공하며, 다양한 안전 기능이 탑재되어 있음.- 차량 디자인은 '역동적인 우아함'을 지향하며, 고급스러운 내부 공간과 편안한 2열 좌석이 특징임.- 2.5와 3.5 가솔린 터보 엔진 옵션이 있으며, 각각 304ps와 380ps의 출력을 자랑함.- 차량 가격은 6945만원부터 시작하며, 가족의 안전을 고려한 패밀리카로도 적합함.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:00:00",
                        "title": '혼다·닛산 결합 시너지? "글쎄" 회의론에도...국내 완성차 셈법은 복...',
                        "summary": "- 혼다와 닛산의 합병 추진이 세계 자동차 업계에 큰 변화를 예고하고 있음.- 두 회사의 합병 효과는 제한적일 것이라는 회의론이 지배적임.- 합병 후 규모의 경제를 통해 원가 절감과 기술 개발 속도를 높일 가능성이 있음.- 현대차는 치열한 경쟁 속에서 대처 능력을 키워야 할 필요성이 커짐.- 혼다는 대규모 자사주 매입 계획을 발표하며 주가가 급등했음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:00:00",
                        "title": "한국은 이제 '초고령사회'…\"'계속고용' 논의, 발상 전환 필요\"",
                        "summary": "- 한국이 초고령사회에 진입하며 65세 이상 인구가 전체의 20%를 차지했음.- 고령 인력 활용 방안에 대한 사회적 논의가 필요하다는 목소리가 커지고 있음.- 노동계는 법적 정년 연장을 요구하고, 경영계는 재고용을 선호하고 있음.- 현대차는 정년에 도달한 기술직 근로자들을 재고용하여 추가 근무를 허용했음.- 고용부는 청년 일자리 감소 우려로 일률적 정년연장에 회의적임.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:51:00",
                        "title": "테슬라 7.4% 급등 마감…성탄 전날 거래 단축 속에서 메가캡 랠리",
                        "summary": "- 테슬라 주가가 7.4% 급등하여 462.28달러로 거래를 마쳤음.- 크리스마스 휴장을 앞두고 대형 기술성장 종목들이 상승장을 주도했음.- 테슬라는 올해 들어 86% 상승했음.- 혼다와 닛산의 합병 발표가 있었으나, 테슬라는 여전히 상승세를 유지했음.- 혼다와 닛산의 합병이 현대차의 세계 3위 자동차 메이커 자리를 위협할 수 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:02:00",
                        "title": "현대차 울산공장에서 삼현주의 쇠락을 본다 [전문가 리포트]",
                        "summary": "- 현대차 울산공장에서 전기차 전용라인 기공식이 열렸으며, 2025년 완공 예정임.- 현대차는 2023년 생산직 공개채용을 재개했으며, 2024년부터 매년 700명가량 채용하기로 결정했음.- 울산공장의 연구개발 기능이 축소되고 생산기지로의 역할이 강화되고 있음.- 엔지니어들이 울산공장을 떠나고 있으며, 대졸 일자리는 수도권에 집중되고 있음.- 지역 산업 생태계의 불균형이 심화되고 있다는 우려가 제기되고 있음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                ],
            },
            {
                "name": "삼성전자",
                "ticker": "A005930",
                "logo_image": "추후 반영",
                "ctry": "kr",
                "current_price": 54400,
                "change_rate": 1.3,
                "items_count": 8,
                "news": [
                    {
                        "date": "2024-12-25T09:22:00",
                        "title": "삼성전자, CES 2025서 '가정용 히트펌프 EHS' 공개",
                        "summary": "- 삼성전자가 CES 2025에서 '가정용 히트펌프 EHS'를 공개할 예정임.- EHS는 공기열과 전기를 이용해 온수를 생성하며, 화석연료 보일러보다 효율이 높음.- 내년에는 미국 시장으로도 확대할 계획이며, 200L 전용 물탱크가 탑재된 모델과 벽걸이형 모델을 선보임.- 'AI 홈' 기능을 통해 제품 제어와 에너지 관리가 용이함.- 에너지 효율이 높은 '모노 R32 HT 콰이어트' 실외기는 지구온난화지수(GWP)가 낮은 냉매를 사용함.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T09:11:00",
                        "title": "5G-A 도입 속도 낸다...AI 투자도 본격화",
                        "summary": "- 한국의 5G 시장은 성숙기에 접어들었으며, 가입자 도입률이 70%를 초과했음.- 통신사들의 설비 투자(CAPEX)는 감소 추세에 있으며, 안정적인 수익 창출이 예상됨.- 5G-어드밴스드 도입이 2025년 통신업의 주요 키워드로 부각되고 있음.- 통신사들은 AI 기업으로의 전환을 추진하며, AI 투자 확대가 주요 과제로 떠오르고 있음.- 정부는 5G 주파수 추가공급과 LTE, 3G 주파수 재할당 논의를 진행할 예정임.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:14:00",
                        "title": '"AI 반도체 수요 급증…엔비디아 올라탄 SK하이닉스가 간다" [202...',
                        "summary": "- 내년 메모리 반도체 수요는 전반적으로 증가할 것으로 예상되며, 특히 AI 디바이스와 데이터센터 수요가 급증할 것임.- 중국 메모리 기업의 공세로 가격 하락이 우려되지만, 국내 메모리 업체들은 고부가 제품 비중을 늘려 영향을 최소화할 전략을 시행할 것임.- SK하이닉스는 엔비디아와의 협업을 통해 HBM 시장에서 선점 효과를 유지할 것으로 전망되며, HBM3E 12단의 수율이 안정화되고 있음.- 트럼프 행정부의 정책 리스크가 반도체 업황 개선에 불확실성을 더할 것으로 분석됨.- 삼성전자는 DDR4 비중을 줄이고 DDR5 비중을 늘리고 있어 수익성 하락 영향은 제한적일 것으로 보임.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:01:00",
                        "title": "연말연시 '계엄 블랙리스트' 이런 문자 주의하세요",
                        "summary": "- 연말연시에는 건강검진, 연말정산 등을 사칭한 피싱과 스팸 공격이 증가함.- 비상계엄 상황을 악용한 사이버 공격에도 주의가 필요함.- '안심마크' 서비스는 삼성전자 단말 이용자에게 제공되며, 피싱 문자 확인에 도움을 줌.- 카카오톡을 통한 스미싱 확인 서비스도 제공되고 있음.- [국외발신] 표시가 있는 메시지에 대한 경계가 필요함.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:02:00",
                        "title": "2024년 대만 증시 ‘뛰는’동안 코스피는 ‘뒷걸음’ 外 [한강로 경제...",
                        "summary": "- 코스피는 올해 아시아 주요국 증시 중 유일하게 5% 이상 하락했음.- 삼성전자의 주가는 AI 분야 경쟁에서 뒤처지며 지난해 말 대비 30.7% 하락했음.- 정치적 불확실성과 소비자 심리 악화가 코스피 부진에 영향을 미쳤음.- 대만 증시는 TSMC의 주가 상승으로 28.94% 상승하며 가장 큰 상승률을 기록했음.- 한국 정부의 밸류업 정책이 주가 부양에 실패한 것으로 평가됨.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:01:00",
                        "title": "수출株, 환율 초강세 속 수혜 차별화…트럼프 변수도 상존",
                        "summary": "- 원·달러 환율이 1456.4원으로 상승하며 수출주에 대한 관심이 높아졌음.- 반도체 업황 하락으로 삼성전자의 주가는 지지부진한 흐름을 보이고 있음.- 자동차와 조선업은 긍정적인 분위기를 보이며 주가 상승세를 기록하고 있음.- 트럼프 2기 정부 출범이 수출에 미치는 영향에 대한 우려가 존재함.- 반도체 EBSI가 기준점 100을 크게 밑돌며 수출 전망이 부정적임.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:00:00",
                        "title": "전 세계 판매 1위 한국 TV…中 소비자들만 유독 '외면'",
                        "summary": "- 삼성전자와 LG전자의 TV 제품이 전 세계에서 인기를 끌고 있으나, 중국 시장에서는 소비자들의 외면을 받고 있음.- 중국 TV 시장에서 자국 브랜드의 점유율이 96.5%에 달하며, 삼성전자는 1% 남짓의 점유율을 기록하고 있음.- 중국 정부의 보조금 정책으로 인해 중국 브랜드의 판매가 증가하고 있으며, 외산 브랜드는 어려움을 겪고 있음.- 내년에는 LCD 패널 가격 인상이 예상되어 원가 부담이 우려됨.- 삼성전자는 중국 시장 공략을 위해 노력하고 있으나, 여전히 낮은 점유율에 머물고 있음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:03:00",
                        "title": "“해외 바이어 만날 때 필수품이네”…더 진화하는 갤럭시S25, 기능 미...",
                        "summary": "- 삼성전자가 다음달 갤럭시 S25 시리즈를 공개할 예정이며, AI 기능이 강화된 스마트폰임.- AI 스마트폰은 통화 내용을 자동으로 녹음하고 텍스트로 변환하는 기능을 제공함.- 새로운 운영체제 '원유아이(One UI) 7'의 베타 테스트를 통해 AI 기능이 개선되었음.- 퀄컴의 '스냅드래곤8 엘리트' 칩셋을 탑재하여 성능이 대폭 향상됨.- 일부 모델에는 16GB RAM이 탑재될 것으로 예상되어 멀티태스킹 성능이 개선될 것임.",
                        "emotion": "positive",
                        "type": "news",
                    },
                ],
            },
            {
                "name": "GS건설",
                "ticker": "A006360",
                "logo_image": "추후 반영",
                "ctry": "kr",
                "current_price": 18100,
                "change_rate": -0.49,
                "items_count": 10,
                "news": [
                    {
                        "date": "2024-12-25T09:29:00",
                        "title": '"설마 했는데 이럴 줄이야"...내년 아파트 값 또 치솟나?',
                        "summary": "- 내년 주요 건설사들의 민간 아파트 분양 물량이 15만 가구로, 2000년 이후 최저치를 기록할 전망임.- 아파트 공급 절벽으로 인한 부동산 시장의 쇼크 우려가 커지고 있음.- 25개 주요 시공사의 조사 결과, 내년도 분양 물량은 총 14만6130가구로 집계됨.- 수도권 분양 물량이 8만5천840가구로, 전체의 59%를 차지하며 수도권 쏠림 현상이 심화할 것으로 예상됨.- GS건설, 롯데건설, HDC현대산업개발의 일부 물량은 포함되지 않았으나, 추가해도 최저치임.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:52:00",
                        "title": "내년 건설사 분양 예정 15만 가구 밑으로…2000년 이후 최저",
                        "summary": "- 내년 주요 건설사들의 민간 아파트 분양 예정 물량이 15만 가구를 밑돌 것으로 나타났음.- 25개 주요 시공사의 내년도 분양 물량은 총 14만6130가구로, 2000년 이후 최저치임.- GS건설, 롯데건설, HDC현대산업개발의 일부 물량(1만1000여가구)은 통계에 포함되지 않았음.- 수도권에서 8만5840가구(59%), 지방에서 6만290가구(41%)가 분양될 예정임.- 분양 급감으로 인해 향후 주택 공급 시장에 쇼크를 줄 수 있는 상황임.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:13:00",
                        "title": "“우리 아파트도 브랜드 붙이자”…공공재개발에 대형건설사 참여 러...",
                        "summary": "- GS건설은 송파구 거여새마을 공공재개발 사업에서 삼성물산과 컨소시엄을 구성해 시공사로 선정되었음.- 전농9구역 공공재개발 사업에서 현대엔지니어링이 시공사로 선정되었으며, 공사비가 3.3㎡당 780만원으로 책정되었음.- 공공재개발 사업은 인허가 속도가 빠르고 사업자금 확보에 유리해 대형 건설사들의 참여가 증가하고 있음.- GS건설은 중화5구역에서도 시공을 맡을 가능성이 높아지고 있으며, 주민대표회의가 LH와 협의 중임.- 공공재개발의 공사비가 상승하면서 대형 건설사들의 참여가 긍정적으로 평가되고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:00:00",
                        "title": "건설사 연말 정기인사 키워드는 '조직 슬림화'·'젊은 조직'",
                        "summary": "- GS건설은 조직 슬림화와 젊은 조직을 목표로 임원 조직을 통합하고 구조를 단순화했음.- 기존 6개 사업본부를 3개로 줄이고, 임원 직급을 통합하여 수직적 계층을 축소했음.- 대우건설은 조직 개편을 통해 빠르고 합리적인 의사결정 체계를 구축하고, 젊은 인재를 배치하여 조직을 개선했음.- 현대건설은 1970년대생 대표이사를 선임하며 세대교체를 알렸음.- 건설업계 전반에 걸쳐 인원 감축과 조직 슬림화가 진행되고 있음.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:00:00",
                        "title": "‘산타랠리’ 기대감 사라졌다…한파 불어닥치는 건설업계",
                        "summary": "- 건설업계에서 '산타랠리' 기대감이 사라졌음. - 일부 건설사 주가는 1년 새 25% 이상 하락했음. - 부도업체 수는 지난해보다 40% 이상 증가했음. - 주택사업 경기 회복이 어려운 상황임. - 올해 부도난 건설업체 수는 30곳으로, 2019년 이후 가장 많았음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T05:03:00",
                        "title": "임원 줄이고 휴가 늘리고… ‘불확실성 시대’ 기업 생존법",
                        "summary": "- GS건설을 포함한 주요 대기업의 임원 승진자가 지난해보다 9.6% 감소했음.- 사장단 이상 고위급 승진자는 지난해 43명에서 올해 24명으로 줄어들었음.- 대기업들은 크리스마스 전후로 휴가를 권장하며 비용 절감을 도모하고 있음.- GS건설은 12월 25일부터 1월 1일까지 전체 휴무에 들어가며, 직원들에게 남은 연차를 소진하도록 권장하고 있음.- 불확실한 경제 상황 속에서 조직 효율화에 집중하는 경향이 나타나고 있음.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T19:51:00",
                        "title": "30대 그룹, 임원 승진 10% 줄어…사장단 이상 대폭 감소",
                        "summary": "- 국내 30대 그룹의 임원 승진자가 지난해보다 10% 감소했음.- GS건설의 임원 승진자는 지난해 19명에서 올해 9명으로 대폭 줄어 52.6% 감소했음.- 사장단 이상 고위직 승진자는 43명에서 24명으로 절반 가까이 줄어든 상황임.- 고금리와 경기침체로 인해 대부분의 그룹이 효율화에 집중하고 있음.- 10대 그룹 중 한화의 감소폭이 가장 크고, GS가 그 뒤를 이음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T18:54:00",
                        "title": '대기업 임원 승진 10%↓…"경기 침체에 슬림 경영 집중"',
                        "summary": "- GS건설을 포함한 국내 주요 대기업들이 임원 승진자 수를 작년보다 약 10% 줄였음.- GS건설의 임원 승진자는 19명에서 9명으로 감소했음.- 경기 둔화와 불확실성으로 인해 기업들이 슬림 경영에 집중하고 있음.- 고위직 승진자는 43명에서 24명으로 줄어들며, 사장 승진자도 감소했음.- 기업들은 희망퇴직, 복지 축소 등으로 경영 효율성을 높이려 하고 있음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T18:03:00",
                        "title": "외면받던 민간참여 공공주택…불황 속 대형사도 뛰어든다",
                        "summary": "- GS건설과 남광토건 컨소시엄이 남양주 왕숙 S-9블록 민간참여 공공주택사업에서 경쟁했으나, 남광토건 컨소시엄이 우선협상대상자로 선정되었음.- 민간참여 공공주택사업에 대한 관심이 높아지고 있으며, 정부의 공사비 인상 계획이 긍정적으로 작용하고 있음.- 대우건설 컨소시엄이 평택고덕 A-56블록에서 우선협상대상자로 선정되었으며, 사업비는 4012억원에 달함.- 공공주택사업은 미분양 우려가 적고, 공사비 인상 리스크가 줄어들어 경쟁이 치열해질 것으로 예상됨.- 정부의 공공 공사비 현실화 대책이 사업 리스크를 줄이고, 민간 참여를 촉진하고 있음.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T17:30:00",
                        "title": '"공공재개발 달라졌네"… 대형 건설사 러시',
                        "summary": "- GS건설은 서울 동대문구 중화5구역 공공재개발 사업에서 시공사로 선정될 가능성이 높음.- 공공재개발 사업은 과거 낮은 공사비로 인해 대형 건설사의 참여가 적었으나, 최근 공사비 인상으로 대형 건설사들이 참여하고 있음.- 전농9구역에서는 현대엔지니어링이 시공사로 선정되었으며, 공사비는 3.3㎡당 780만원으로 총 4400억원에 달함.- 공공재개발은 LH와 SH공사가 주민과 함께 시행하는 방식으로, 민간 재개발보다 사업성이 높아지고 있음.- GS건설은 송파구 거여새마을 사업에서도 시공사로 선정되었으며, 대형 건설사 참여에 대한 긍정적인 분위기가 형성되고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                ],
            },
            {
                "name": "고려아연",
                "ticker": "A010130",
                "logo_image": "추후 반영",
                "ctry": "kr",
                "current_price": 1134000,
                "change_rate": 0.53,
                "items_count": 9,
                "news": [
                    {
                        "date": "2024-12-25T09:30:00",
                        "title": "한달 남은 고려아연 임시주총...최윤범 '노림수'와 MBK '경우의수'",
                        "summary": "- 고려아연의 임시주총이 2025년 1월 23일로 예정되어 있으며, 집중투표제 도입과 이사수 상한 제한이 주요 안건으로 추가되었음.- 최윤범 회장은 영풍·MBK 측의 이사진 진입을 최소화하기 위한 전략으로 집중투표제를 도입하려 함.- 영풍·MBK는 이사수 상한 제한에 반대할 가능성이 높아, 이사 선임 방식에 대한 법적 논란이 존재함.- 임시주총 이후에도 경영권 분쟁이 지속될 것으로 보이며, 3월 정기주총에서 재대결이 불가피함.- 국민연금의 판단이 집중투표제 도입에 중요한 변수로 작용할 것으로 예상됨.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:33:00",
                        "title": '지분 밀리는데 "승리 확신한다"…최윤범, 손에 쥔 카드 두 장',
                        "summary": "- 최윤범 고려아연 회장이 내년 1월 23일 임시 주주총회를 앞두고 경영권 수성을 자신하고 있음.- 현재 지분율에서 MBK파트너스·영풍 연합에 밀린 상황이나, 집중투표제와 이사회 정원 제한을 통해 역공을 노리고 있음.- MBK 연합은 최 회장의 전략을 비판하며, 외국인 투자 의혹과 미공개정보 이용 논란에 대해 반박하고 있음.- 최 회장은 MBK의 외국인 경영진 문제를 부각시키며, 정부의 개입을 유도하고 있음.- 경영권 분쟁이 단순한 지분 경쟁을 넘어 복잡한 양상으로 전개되고 있음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T22:58:00",
                        "title": '고려아연 "조건부 집중투표청구는 합법·적법”… 영풍·MBK측 문제 제....',
                        "summary": "- 고려아연이 임시주주총회에서 집중투표제 도입을 위한 정관 변경을 추진하고 있음.- 영풍과 MBK파트너스 측이 법적 하자를 주장하자, 고려아연은 이를 반박하며 적법성을 강조했음.- 주주 제안은 정관 변경 6주 전인 12월 10일에 제출되었으며, 절차적으로 문제가 없다고 주장했음.- 법조계에서는 조건부 집중투표청구가 가능하다는 해석이 많음.- 고려아연은 유사한 사례들이 존재하며, 주주들에게 손해를 끼치지 않았다고 설명했음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T20:42:00",
                        "title": "사모펀드 도입 20년, 덩치 이렇게 커졌지만…국민 10명 중 6명 “기...",
                        "summary": "- MBK파트너스와 고려아연 간 경영권 분쟁이 진행 중이며, 여론조사 결과 10명 중 6명이 사모펀드의 단기차익 실현이 기업가치를 훼손할 것이라고 우려함.- 사모펀드에 대한 부정적인 인식이 여전히 우세하며, 응답자의 58.4%가 기업 인수합병이 산업 경쟁력에 부정적이라고 답변함.- 응답자의 60.5%는 MBK가 고려아연을 인수할 경우 기업가치가 하락할 것에 동의함.- 사모펀드의 책임성 강화와 기업 경영권 방어 수단의 필요성이 강조되고 있음.- 국내 사모펀드 시장 규모는 20년 만에 341배 성장하여 136조4000억 원에 달함.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T20:00:00",
                        "title": "고려아연 주총 표 대결 쟁점 떠오른 ‘집중투표제’",
                        "summary": "- 고려아연 주주총회에서 집중투표제가 쟁점으로 떠올랐음.- 고려아연 측은 소액주주 권리 강화를 위해 집중투표제를 도입하겠다고 주장했음.- MBK파트너스와 영풍은 집중투표제가 최 회장의 경영권 연장을 위한 꼼수라고 반발했음.- 집중투표제는 이사를 선임할 때 1주당 선임하고자 하는 이사 수만큼 의결권을 부여하는 제도임.- 최 회장 측은 현재 이사회에서 13명 중 다수를 차지하고 있으며, 집중투표제 도입이 유리할 수 있음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T19:06:00",
                        "title": "코스피 2,440대 약보합 마감…거래대금 13개월만 최소(종합2보)",
                        "summary": "- 24일 코스피는 약보합세로 마감하며 2,440선을 간신히 유지했음.- 외국인과 개인이 매도 우위를 보였으나 기관이 순매수 전환하며 지수 하락을 방어했음.- 거래대금은 6조7천407억원으로 지난해 11월 이후 가장 적었음.- 삼성전자는 외국인 순매수에 힘입어 상승했으나 SK하이닉스와 한미반도체는 하락했음.- 고려아연은 2.90% 상승하며 시가총액 상위 종목 중 하나로 부각되었음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T18:14:00",
                        "title": "거래소, 고려아연 불성실공시법인 지정 예고",
                        "summary": "- 한국거래소는 고려아연을 불성실공시법인으로 지정예고했다고 발표했음.- 두 번의 공시불이행이 원인으로, 금전대여 결정과 채무보증 결정에 대한 정정사실 공시가 지연되었음.- 고려아연은 유가증권시장공시위원회의 심의를 통해 불성실공시법인 지정 여부를 결정받게 됨.- 최근 1년 동안 고려아연이 받은 부과누계벌점은 7.5점임.- 이로 인해 회사의 신뢰도와 주가에 부정적인 영향을 미칠 가능성이 있음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T16:31:00",
                        "title": "결국 산타 랠리는 없었다…코스피, 외인·개인 이탈에 하락 마감",
                        "summary": "- 코스피는 외국인과 개인의 매도세로 하락 마감했음.- 고려아연은 시가총액 상위 종목 중 2%대 상승했음.- 고환율과 고금리, 정치 불확실성이 증시에 부담을 주고 있음.- 코스닥 지수는 소폭 상승했으나, 개인과 외국인의 매도세가 지속됨.- 시장의 관망 심리가 부각되며 종목 차별화가 진행되고 있음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T15:05:00",
                        "title": "성탄절 휴장 앞둔 코스피 숨고르기…환율 1455원 넘어동영상기사",
                        "summary": "- 성탄절을 앞두고 코스피가 숨 고르기 장세를 이어가고 있으며, 원달러 환율은 1455원대를 기록하고 있음.- 코스피는 외국인과 기관의 매도로 소폭 하락했으나, 개인 투자자는 순매수에 나섰음.- 고려아연은 임시주주총회에서 이사 수 제한 및 집중투표제 도입 등의 안건을 확정하며 장중 3% 넘게 상승했음.- 코스닥 지수는 상승세를 이어가며 680선 초반대에서 거래되고 있음.- 한국은행은 환율 상승이 금융기관 재무 건전성에 미치는 영향이 크지 않다고 진단했음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                ],
            },
            {
                "name": "NAVER",
                "ticker": "A035420",
                "logo_image": "추후 반영",
                "ctry": "kr",
                "current_price": 202500,
                "change_rate": 1,
                "items_count": 9,
                "news": [
                    {
                        "date": "2024-12-25T09:18:00",
                        "title": "네이버페이, 국내 최초 중국에서 위챗페이 `QR결제` 제공",
                        "summary": "- 네이버페이가 국내 최초로 중국 내 위챗페이 연동 QR결제 서비스를 시작했음.- 사용자들은 별도의 앱 설치 없이 네이버페이 앱을 통해 간편하게 결제할 수 있음.- 위챗페이 결제는 QR코드를 촬영하여 결제금액을 입력하는 방식으로 진행됨.- 네이버페이는 유니온페이와 협력하여 다양한 QR결제 서비스를 제공하고 있음.- 네이버페이 사용자들은 프로모션과 혜택을 통해 추가적인 이점을 누릴 수 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T09:01:00",
                        "title": "e스포츠로 맹추격하는 네이버 치지직 VS 글로벌로 격차 벌리는 SOOP",
                        "summary": "- 네이버는 내년에 '치지직' 이름을 단 자체 e스포츠 대회를 개최할 예정임.- e스포츠 대회는 리그 형식으로 진행될 가능성이 높음.- SOOP은 글로벌 경쟁력을 강화하며 e스포츠 생태계를 확장하고 있음.- 네이버는 파트너 스트리머를 초청해 대회 개최 계획을 발표했으나, 구체적인 내용은 미정임.- 업계에서는 네이버의 자본력이 e스포츠 대회에 큰 파급력을 미칠 것으로 예상하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T09:00:00",
                        "title": "드리미 X50 울트라 두 번째 티징영상 선보여",
                        "summary": "- 드리미(Dreame Technology)가 신제품 X50 Ultra에 대한 두 번째 티징 영상을 공개했음.- X50 Ultra는 Versa Lift™ 네비게이션 기술과 Pro-leap™ 기술을 탑재하여 청소 성능을 향상시켰음.- 두 가지 혁신 기술로 인해 로봇청소기 커뮤니티에서 긍정적인 반응이 이어지고 있음.- 드리미는 X50 Ultra의 사전 예약 이벤트를 진행하며, 선착순 50,000명에게 네이버 페이 포인트를 지급할 예정임.- 다양한 경품 이벤트도 마련되어 있어 소비자들의 기대감을 높이고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:57:00",
                        "title": "번개장터, 해외구매 후기 이벤트…최대 11만원 혜택",
                        "summary": "- 번개장터가 해외 상품 구매 후기 이벤트를 진행하며, 내년 2월 2일까지 참여할 수 있음.- '해외 탭'에서 구매한 상품 후기를 SNS에 작성하면 최대 11만1000원의 번개포인트를 지급함.- 후기는 네이버 블로그, 인스타그램, 틱톡 등 다양한 채널에서 작성 가능하며, 중복 참여가 가능함.- 베스트 후기에 선정된 5명에게는 추가로 5만원의 번개포인트가 지급될 예정임.- 번개장터는 일본 중고거래 플랫폼 '메루카리'와 파트너십을 체결하고 해외 상품 구매를 지원함.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:01:00",
                        "title": "'슈퍼앱' 꿈꾸던 티맵은 군살 빼기중…이제 경쟁상대는 현대차?",
                        "summary": "- 티맵모빌리티가 슈퍼앱으로의 변화를 포기하고 AI 기반 모빌리티 데이터 기업으로 전환하고 있음.- 올해 들어 블랙박스 녹화, HUD 서비스, 전기차 충전배달 제휴 서비스 등 다수의 서비스를 종료했음.- 우버와의 합작법인 UT 지분을 매각하고, 서울공항리무진 및 굿서비스 매각도 추진 중임.- 티맵은 수익성 중심으로 사업을 정리하며 맞춤형 B2C 및 B2B 서비스에 집중할 계획임.- 현대차와 같은 완성차 업체들이 티맵의 새로운 경쟁상대가 될 것으로 예상됨.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:03:00",
                        "title": "카톡으로 '메리 크리스마스' 보냈다가…\"이게 무슨 일\" 깜짝",
                        "summary": "- 네이버와 카카오는 크리스마스를 맞아 스페셜 로고와 배경 화면을 선보이며 분위기를 띄우고 있음.- 네이버는 크리스마스 관련 탭을 신설하고, 크리스마스 명소와 선물 추천 기능을 제공하고 있음.- 카카오톡에서는 크리스마스 관련 키워드를 입력하면 특별 이모티콘이 등장하는 이벤트를 진행하고 있음.- 카카오의 송금 기능에 'X-mas' 카드가 추가되어 크리스마스 트리 효과를 제공하고 있음.- 카카오는 크리스마스 인증샷 명소를 추천하는 테마 지도도 제공하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:00:00",
                        "title": "주가 오르다 '뚝', 오르다 또 '뚝'…외인 담아도 박스권 '뱅뱅' 네...",
                        "summary": "- 네이버(NAVER)는 12월에 외국인 순매수 3500억원을 기록했으나 주가는 20만~21만원대에서 지지부진함.- 최근 주가는 등락을 반복하며 52주 최고가 대비 86% 하락했음.- 커머스 앱 '네이버플러스 스토어' 출시와 넷플릭스와의 제휴가 기대되지만 AI 분야에서 경쟁력이 부족하다는 평가를 받고 있음.- 증권가는 네이버의 목표주가를 24만~29만원으로 상향 조정했음.- AI 서비스 개발에 대한 전략은 추진 중이나, 거대 기업들과의 경쟁에서 뒤처진 상황임.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:00:00",
                        "title": "'젊어진' 네이버, 중동에 AI 수출…\"내년 실적 사상 최대 예고\"",
                        "summary": "- 네이버는 올해 매출이 10조 원을 넘기고 내년 영업이익이 2조 원을 초과할 것으로 예상됨.- MZ세대(1030세대) 비중이 네이버 블로그 이용자의 64%에 달하며, 이는 검색 매출 증가에 기여할 것으로 보임.- 네이버는 사우디아라비아와 AI 협력 및 중동 총괄 법인 설립을 통해 AI 기술 수출을 추진하고 있음.- 내년부터 모든 서비스에 AI를 적용하는 '온 서비스 AI' 전략을 시행할 계획임.- 글로벌 AI 검색 서비스의 점유율 증가에 대한 우려가 존재함.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:02:00",
                        "title": "[디지털상공인] ‘힙지로 주역’ 커피한약방, 온라인 시장도 도전장",
                        "summary": "- 커피한약방은 독특한 인테리어와 수제 커피로 유명한 카페로, 최근 온라인 시장에 진출했음.- 코로나19로 인해 매출이 급감하면서 온라인 판매의 필요성을 느꼈음.- 현재 네이버 스마트스토어를 통해 원두와 드립백을 판매하고 있으며, 디저트도 추가할 예정임.- 오프라인 매장 이전과 해외 진출도 고려하고 있으며, 브랜드 정체성을 유지하고 싶어함.- SNS를 통한 입소문 마케팅으로 인기를 끌었음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                ],
            },
            {
                "name": "카카오",
                "ticker": "A035720",
                "logo_image": "추후 반영",
                "ctry": "kr",
                "current_price": 39500,
                "change_rate": -0.63,
                "items_count": 10,
                "news": [
                    {
                        "date": "2024-12-25T09:31:00",
                        "title": "형인우 대표 2대주주 합류 엔솔바이오, ‘기평 돌입’",
                        "summary": "- 엔솔바이오사이언스가 코스닥 상장에 본격 나서며 기술성평가를 신청했음.- 형인우 스마트앤그로스 대표가 2대주주로 합류하며 회사 가치가 높아졌음.- 엔솔바이오는 최근 기술수출 성과와 파이프라인 강화로 긍정적인 평가를 받고 있음.- AI 기반 펩타이드 발굴 플랫폼을 통해 지속 가능한 성장 기반을 마련했음.- 내년 1분기 예비상장심사를 신청할 계획임.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:30:00",
                        "title": "다가온 외국인 친구 생일…카카오 AI가 추천한 선물은?",
                        "summary": "- 카카오는 최근 'AI 쇼핑메이트' 베타 버전을 웹과 카카오톡 채널을 통해 출시했음.- 이 서비스는 친구의 생일 일정과 추천 선물을 AI가 알려주며, 결제까지 지원함.- AI는 사용자에게 맞춤형 선물 추천과 생일 축하 문구를 제공함.- 카카오는 쇼핑 특화 AI 기술을 개발하여 사용자 경험을 개선하고자 함.- 베타 기간 동안 사용자 의견을 반영하여 서비스 개선을 지속할 예정임.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:01:00",
                        "title": "'슈퍼앱' 꿈꾸던 티맵은 군살 빼기중…이제 경쟁상대는 현대차?",
                        "summary": "- 티맵모빌리티가 슈퍼앱으로의 변화를 포기하고 AI 기반 모빌리티 데이터 기업으로 전환하고 있음.- 올해 들어 블랙박스 녹화, HUD 서비스, 전기차 충전배달 제휴 서비스 등 다수의 서비스를 종료했음.- 우버와의 합작법인 UT 지분을 매각하고, 자회사 매각도 추진 중임.- 수익성 개선을 위해 부수적인 사업들을 정리하고, 데이터 기반 맞춤형 서비스에 집중할 계획임.- 현대차와 같은 완성차 업체들이 티맵의 새로운 경쟁상대가 될 것으로 예상됨.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:30:00",
                        "title": '카카오, 강도 높은 쇄신 작업 지속…"일상 속 AI 선보인다"',
                        "summary": "- 카카오는 올해 매출이 사상 최대치를 기록할 것으로 예상되지만, 영업이익은 소폭 감소할 전망임.- 강도 높은 쇄신 작업을 통해 AI 전담 조직을 개편하고, 내년 상반기 '카나나' 애플리케이션을 선보일 예정임.- 비핵심 사업 정리를 통해 경영 효율성을 높이고 있으며, AI 기술을 활용한 다양한 서비스 개발에 집중하고 있음.- 그러나 김범수 경영쇄신위원장의 사법 리스크가 사업 확장에 제약을 줄 가능성이 있음.- 내년은 AI 기술을 통한 성장 기회를 모색하는 동시에 사법 리스크 해결이 중요한 전환점이 될 것으로 보임.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:03:00",
                        "title": "카톡으로 '메리 크리스마스' 보냈다가…\"이게 무슨 일\" 깜짝",
                        "summary": "- 카카오는 크리스마스를 맞아 카카오톡에서 특별 이벤트를 진행하고 있음.- 채팅방에 '성탄절', '이브', '크리스마스' 등의 키워드를 입력하면 카카오프렌즈 캐릭터가 등장함.- 카카오톡 송금하기에 'X-mas' 카드가 신설되어 크리스마스트리가 나타나는 효과가 있음.- 카카오 선물하기에도 'X-mas' 탭이 신설되어 다양한 크리스마스 선물을 할인된 가격에 제공함.- 카카오는 크리스마스 관련 테마 지도를 통해 명소를 추천하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:02:00",
                        "title": "정신아 대표 “데이터센터는 카카오 심장과 같은 곳”",
                        "summary": "- 정신아 카카오 대표가 연말연시를 맞아 데이터센터 안산을 방문하여 인프라 현황을 점검했음.- 카카오톡의 일평균 수발신량은 100억건 이상이며, 연말에는 트래픽이 급증함.- 카카오는 서버 확충과 비상 인력 투입 등으로 서비스 안정성을 확보할 계획임.- 데이터센터는 연면적 4만7378㎡로, 10만대 이상의 서버를 보관할 수 있는 대규모 센터임.- 카카오는 체계적인 장애 대응과 모니터링 시스템을 통해 안정성을 높이고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T18:40:00",
                        "title": "“아! 깜빡 속았다” 카톡 친구인 줄 알았는데…이용자 ‘분노 폭발’",
                        "summary": "- 카카오는 카카오톡 프로필에 광고를 삽입하는 '프로필풀뷰' 광고를 도입했음.- 이용자들은 광고 삽입에 대해 불만을 표출하고 있으며, 광고 차단 방법이 공유되고 있음.- 카카오는 광고 매출 증가를 목표로 하며, 신규 광고 상품 출시를 예고했음.- 광고 매출은 올해 1분기부터 3분기까지 등락을 거듭하고 있음.- 카카오는 광고를 통해 브랜드 메시지를 효과적으로 전달할 수 있다고 강조했음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T18:29:00",
                        "title": "신년 카톡 대화량 평소 '3배'…정신아 대표, 데이터센터 점검 나서",
                        "summary": "- 정신아 카카오 대표가 연말연시를 앞두고 카카오 데이터센터 안산을 방문하여 서비스 안정성을 점검했음.- 카카오톡의 1초당 메시지 발신량은 평균 4만 5천 건으로, 연말연시에는 트래픽이 평소의 3배로 증가함.- 카카오는 서버 확충, 비상 인력 투입, 트래픽 분산 작업 등을 통해 서비스 안정성을 확보할 계획임.- 데이터센터는 카카오 서비스의 핵심으로, 임직원들에게 사명감을 강조하며 격려했음.- 카카오는 장애 대응 및 모니터링 시스템 강화를 위한 체계를 마련하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T18:17:00",
                        "title": '"새해 핫플은 여기" 도산공원에 라이언·춘식이 떴다',
                        "summary": "- 카카오는 24일 강남구와 협업해 도산공원에서 '라춘복배달' 행사를 진행한다고 발표했음.- 행사에서는 라이언과 춘식이 캐릭터를 활용한 테마 공간과 포토존이 설치될 예정임.- 방문객들은 새해 희망을 적을 수 있는 소원지를 제공받고, 소원 전시존도 마련됨.- 강남구는 행사 홍보를 위해 옥외 광고 캠페인을 진행하며, 주요 미디어 스크린에서 3D 영상이 송출될 예정임.- 행사는 2024년 2월 2일까지 6주간 이어질 예정임.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T18:00:00",
                        "title": "해 바뀔 때 카톡 트래픽 세 배 '쑥'... 과기부·카카오, 서비스 안...",
                        "summary": "- 카카오는 연말연시와 성탄절을 맞아 카카오톡 트래픽이 급증할 것으로 예상하고, 이에 대한 대비책을 마련했음.- 정신아 카카오 대표는 안산 데이터센터를 방문해 서비스 안정성을 점검했음.- 카카오톡의 메시지 발신량은 평시보다 세 배 이상 증가할 것으로 보이며, 최대 10배까지 트래픽을 처리할 수 있는 능력을 갖추고 있음.- 과학기술정보통신부는 사이버 위협에 대한 경고와 함께 주요 디지털 사업자와 핫라인을 운영해 장애 유무를 즉시 파악할 예정임.- 통신사와 IT사들은 연말을 맞아 자원 증설에 나서고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                ],
            },
            {
                "name": "하나금융지주",
                "ticker": "A086790",
                "logo_image": "추후 반영",
                "ctry": "kr",
                "current_price": 58900,
                "change_rate": 0.17,
                "items_count": 10,
                "news": [
                    {
                        "date": "2024-12-25T09:28:00",
                        "title": '"700만의 선택" 하나 트래블로그 환전금액 3조·절감수수료 1700억',
                        "summary": "- 하나금융그룹의 환전 서비스 '트래블로그' 가입자 수가 700만명을 돌파했음.- 누적 환전금액은 3조원, 고객들이 아낀 수수료는 약 1700억원에 달함.- 함영주 회장은 내년에도 최상의 손님 경험을 제공하겠다고 약속했음.- 트래블로그는 24시간 모바일 환전 서비스로, 다양한 혜택을 제공함.- 최근 카카오페이와 제휴하여 서비스 범위를 확대하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T09:06:00",
                        "title": "상장사 10곳 중 6곳, 목표주가 줄줄이 하향 조정",
                        "summary": "- 국내 증시 상장사 10곳 중 6곳의 목표주가가 하향 조정되었음.- 목표주가 하향 조정된 종목은 179개로 전체의 63.7%에 달했음.- 반도체 및 화장품 관련 기업들이 목표주가 하락의 주요 원인으로 지목되었음.- 목표주가가 가장 크게 하향 조정된 종목은 이수페타시스였음.- 반면, 엔터테인먼트 기업 디어유는 목표주가가 가장 많이 상승했음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:34:00",
                        "title": "'연말정산' 어떤 은행에서 준비하지?…\"여기에서 경품도 챙긴다\"",
                        "summary": "- 하나금융지주는 연말정산 미리보기 서비스를 제공하며 고객 유치를 위한 이벤트를 진행하고 있음.- 하나원큐 앱에서 연말정산 미리보기를 진행하는 고객에게 커피 쿠폰을 제공하는 이벤트를 실시하고 있음.- 주요 은행들은 민간 인증서를 통해 연말정산 서비스를 제공하고 있으며, 고객 몰이에 나서고 있음.- 개인형퇴직연금(IRP) 유치 경쟁도 벌어지고 있으며, 세액공제 혜택이 강조되고 있음.- 민간 인증서의 가입자 수가 디지털 전환의 척도로 평가되고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:35:00",
                        "title": "한때 1460원 위협, 환율 크리스마스 이브의 난",
                        "summary": "- 12월 24일 원‧달러 환율이 연중 최고치인 1456.4원을 기록하며 1460원대 위협을 받았음.- 코스피지수는 0.06% 하락한 2440.52포인트로 마감했으며, 코스닥지수는 0.13% 상승한 680.11포인트로 거래를 마쳤음.- 개인과 외국인 투자자는 순매도세를 보였고, 기관투자자는 순매수세를 기록했음.- 홍준표 테마주와 관련된 종목들이 등락을 보였으며, 아티스트스튜디오의 주가는 큰 폭으로 하락했음.- 크리스마스 이브에도 '산타 랠리'가 나타나지 않았음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:02:00",
                        "title": "2024년 대만 증시 ‘뛰는’동안 코스피는 ‘뒷걸음’ 外 [한강로 경제...",
                        "summary": "- 올해 코스피는 -8.09% 하락하며 아시아 주요국 중 유일하게 하락세를 보였음.- 삼성전자의 주가 하락과 정치적 불확실성이 소비심리에 부정적인 영향을 미쳤음.- 12월 소비자심리지수는 88.4로, 코로나19 이후 최대 폭 하락을 기록했음.- 한국과 대만 증시의 시가총액 차이가 9500억 달러로 벌어졌음.- 금융위원회는 자본시장 관련 법률 시행령을 의결하여 자사주 신주배정 금지를 결정했음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T07:00:00",
                        "title": "금융권 '솔로 탈출' 프로젝트 활활…직원 매칭해 '핑크빛 물결' 만든다",
                        "summary": "- 금융권에서 '솔로 탈출' 프로젝트가 활발히 진행되고 있으며, 직원들의 연애를 장려하는 프로그램이 인기를 끌고 있음.- BNK금융지주는 미혼 직원들을 위한 소개팅 프로그램을 실시했으며, 하나은행도 '사랑, 그게 뭔데' 프로그램을 진행함.- 이러한 프로그램은 저출산 문제 해결을 위한 노력의 일환으로, 직원들의 긍정적인 반응을 얻고 있음.- 부산 지역 금융기관들이 소개팅을 주선하는 이유는 지역 내 미혼 직원들이 새로운 인연을 만날 기회를 제공하기 위함임.- 신한은행도 유사한 프로그램을 통해 직원들의 연애를 장려하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:31:00",
                        "title": "'만년 소외주' 금융株의 부활, 밸류업 타고 날았다[2024 핫종목 결...",
                        "summary": "- 금융주가 올해 32.08% 상승하며 주목받았음. - 하나금융지주는 연초 대비 60.75% 상승했음. - 정부의 밸류업 정책이 금융주 상승의 주요 요인으로 작용했음. - 정치적 불확실성으로 외국인 투자자들이 금융주를 매도했음. - 밸류업 공시의 이행 여부가 향후 주가에 중요한 영향을 미칠 것으로 예상됨.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:02:00",
                        "title": "“M&A 시장 내년 더 어렵다”… 2금융 12개사 주인찾기 ‘난망’",
                        "summary": "- 금융권 M&A 시장이 올해 지지부진하며 내년에는 더욱 위축될 것으로 전망됨.- 현재 매각이 진행 중인 금융사는 총 12개사로, 높은 매각가가 문제로 지적됨.- 하나금융지주는 KDB생명 인수에 본입찰에 불참하여 매각이 무산됨.- 롯데손보와 롯데카드도 높은 매각가로 인해 매각에 실패했음.- 금융지주사의 소극적 태도가 M&A 시장 부진의 주요 원인으로 분석됨.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:01:00",
                        "title": "[스타트UP] “쉽고 빠르고 저렴” 1대 다(多) 메시지 발송 서비스 ...",
                        "summary": "- 자버는 기업이 고객과 내부 직원에게 메시지를 발송하는 1대 다 커뮤니케이션 서비스를 제공하는 스타트업임.- 현재 하나은행, KT 등 다양한 고객사를 확보하고 있으며, 매출이 매년 2배씩 성장하고 있음.- 자버는 전화번호만 있으면 텍스트, 이미지, 동영상 등 다양한 형식의 메시지를 쉽게 보낼 수 있는 점이 강점임.- 고객에게 유익한 정보를 제공하는 것을 최우선으로 하여 커뮤니케이션 마케팅을 진행하고 있음.- 자버는 미국, 일본 등 해외 진출도 준비 중이며, 글로벌 1대 다 커뮤니케이션 기업으로 성장할 계획임.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T05:55:00",
                        "title": "“은행이 시니어 고객에 목맨 이유 있었네”…자산 다 합치면 무려 40....",
                        "summary": "- 하나금융지주는 고령층 고객을 겨냥한 '하나 더 넥스트'라는 시니어 특화 브랜드를 출범했음.- 60세 이상 고령층의 순자산이 4307조원으로 증가하며, 금융권의 경쟁이 치열해지고 있음.- 하나은행은 12억원 초과 주택 보유자를 위한 민간 주택연금서비스를 혁신금융서비스로 지정받았음.- 신한은행과 우리은행 등 다른 은행들도 시니어 전용 서비스와 상품을 확대하고 있음.- 고령층 자산가를 위한 다양한 비금융 서비스도 제공되고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                ],
            },
            {
                "name": "알테오젠",
                "ticker": "A196170",
                "logo_image": "추후 반영",
                "ctry": "kr",
                "current_price": 296000,
                "change_rate": 5.71,
                "items_count": 10,
                "news": [
                    {
                        "date": "2024-12-25T09:31:00",
                        "title": "형인우 대표 2대주주 합류 엔솔바이오, ‘기평 돌입’",
                        "summary": "- 엔솔바이오사이언스가 코스닥 상장에 본격 나서며, 형인우 대표가 2대주주로 합류했음.- 기술성평가를 신청하고, BBB등급 이상을 목표로 하고 있음.- 최근 기술수출 성과와 파이프라인 강화로 회사 가치가 상승했음.- AI 기반 펩타이드 발굴 플랫폼을 통해 지속 가능한 성장 기반을 마련하고 있음.- 형인우 대표는 알테오젠의 주요 투자자로 알려져 있으며, 지분 5.11%를 보유하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T09:06:00",
                        "title": "김용주 리가켐바이오 대표 “1차 목표는 몸값 20조... 신약개발서 최...",
                        "summary": "- 김용주 리가켐바이오 대표는 기술수출이 기업가치 제고를 위한 임시방편이라고 언급했음.- 리가켐바이오는 ADC 기술을 중심으로 6년 연속 기술수출에 성공했으며, 올해 일본 오노약품공업에 기술수출을 했음.- 김 대표는 시가총액 20조원을 목표로 하며, 신약개발에 대한 투자를 아끼지 않겠다고 강조했음.- 현재 코스닥 시장에서 알테오젠은 시총 15조원으로 리가켐바이오와 비슷한 위치에 있음.- 리가켐바이오는 향후 5년 내 10~20개의 추가 파이프라인 확보를 목표로 하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T18:43:00",
                        "title": "신약물질 넘어 개발플랫폼도 수출…조단위 '빅딜' 이어간다 [제2 반도....",
                        "summary": "- K제약바이오 기업들이 기술수출에 성공하며 성장 가능성을 높이고 있음.- 올해 15개 기업이 기술수출에 성공, 총 계약 규모는 8조원을 초과했음.- 알테오젠은 MSD와 4억3200만 달러 규모의 계약을 체결했음.- 오름테라퓨틱스는 9억4500만 달러 규모의 기술수출 계약을 체결했음.- K제약바이오의 기술수출 모델이 발전하며 글로벌 시장에서 신뢰받는 파트너로 부상하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T17:52:00",
                        "title": '"엔비디아 꺾이자 테슬라 픽"…나스닥 제친 액티브 ETF',
                        "summary": "- 액티브 ETF가 최근 주식시장 변동성 속에서 두드러진 성과를 보였음.- 'TIMEFOLIO 미국나스닥100액티브' ETF는 올해 83.4%의 수익률을 기록하며 나스닥 지수를 크게 초과했음.- 삼성액티브자산운용의 'KoAct 바이오헬스케어액티브' ETF는 14.3%의 수익률을 기록했으며, 알테오젠이 주요 종목으로 포함됨.- 액티브 ETF는 시장 상황에 맞춰 종목 비중을 조절하여 수익을 추구하는 특징이 있음.- 최근 정치적 혼란으로 밸류업 ETF의 성과가 부진했음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T17:26:00",
                        "title": "거래소, 알테오젠 불성실공시법인 지정 예고",
                        "summary": "- 한국거래소 코스닥시장본부가 알테오젠에 대해 불성실공시법인 지정을 예고했음.- 사유는 회사합병 결정 철회 및 타법인 주식 취득결정 철회 등 공시번복임.- 불성실공시법인 지정 여부 결정 시한은 2025년 1월 20일임.- 알테오젠의 공시 번복이 주가에 미칠 영향이 우려됨.- 투자자들의 신뢰도에 부정적인 영향을 줄 가능성이 있음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T16:48:00",
                        "title": "조선 올라탄 로봇주…내년 주목할 종목은 [장 안의 화제]동영상기사",
                        "summary": "- 알테오젠은 최근 주가가 상승세를 보이고 있으며, 대주주 양도세 회피 매물 수요가 증가하고 있음.- 바이오 및 제약 섹터에서 알테오젠과 같은 종목들이 주목받고 있으며, 투자자들이 배당 수익을 노리고 있음.- 로봇주와 조선주가 연결되며 강세를 보이고 있는 가운데, 알테오젠도 이러한 흐름에 영향을 받을 수 있음.- 내년에는 외국인 투자자들이 한국 증시에 다시 유입될 가능성이 제기되고 있음.- 전반적으로 한국 증시는 내년 턴어라운드 가능성이 언급되고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T16:40:00",
                        "title": '코스피, 크리스마스 휴장 앞두고 2440선 턱걸이… "산타랠리 없었다"',
                        "summary": "- 코스피가 24일 하락 마감했으며, 개인과 외국인의 매도세가 영향을 미쳤음.- 코스닥은 소폭 상승했으며, 알테오젠은 6.86% 상승함.- 기관은 코스닥에서 972억원어치를 순매수했음.- 크리스마스를 앞두고 거래대금이 연평균에 미치지 못했음.- 삼성전자의 상승이 국내 증시의 낙폭을 줄이는 데 기여했음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T16:33:00",
                        "title": "코스피, 美산타랠리에 상승 출발했지만...외국인 매도에 '약보합' 마감",
                        "summary": "- 코스피는 외국인 매도에 따라 1.49포인트(0.06%) 하락하며 2440.52로 마감했음.- 미국 증시는 성탄특수에 대한 기대감으로 상승했으나, 국내 증시는 특별한 이슈가 없어 하락세를 보였음.- 외국인과 개인이 각각 172억원, 957억원을 순매도했으며, 기관이 220억원 순매수했음.- 코스닥은 0.87포인트(0.13%) 상승하며 680.11로 강보합 마감했음.- 알테오젠은 6.86% 상승하며 주가가 긍정적인 흐름을 보였음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T16:31:00",
                        "title": "결국 산타 랠리는 없었다…코스피, 외인·개인 이탈에 하락 마감",
                        "summary": "- 코스피는 외국인과 개인의 매도세로 하락 마감했음.- 고환율, 고금리, 정치 불확실성이 증시에 부담을 주었음.- 코스닥 지수는 소폭 상승했으나, 개인과 외국인이 매도세를 보였음.- 알테오젠은 코스닥에서 6%대 상승하며 긍정적인 흐름을 보였음.- 전반적으로 관망세가 지속되며 종목 차별화가 진행되었음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T16:21:00",
                        "title": "강달러 압박에 '팔자' 돌아선 외국인…코스피, 약보합 마감[시황종합.....",
                        "summary": "- 코스피는 0.06% 하락한 2440.52로 마감했음.- 외국인 투자자들이 하루 만에 '팔자'로 돌아서며 순매도 전환했음.- 미국 채권 금리 상승과 강달러가 투자 심리에 부정적인 영향을 미쳤음.- 코스닥은 0.13% 상승하며 680.11로 마감했음.- 알테오젠은 코스닥 시가총액 상위 종목 중 6.86% 상승했음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                ],
            },
            {
                "name": "HDC현대산업개발",
                "ticker": "A294870",
                "logo_image": "추후 반영",
                "ctry": "kr",
                "current_price": 18430,
                "change_rate": -3.15,
                "items_count": 9,
                "news": [
                    {
                        "date": "2024-12-25T09:29:00",
                        "title": '"설마 했는데 이럴 줄이야"...내년 아파트 값 또 치솟나?',
                        "summary": "- 내년 주요 건설사들의 민간 아파트 분양 물량이 15만 가구로 집계되어 2000년 이후 최저치를 기록할 전망임.- 아파트 공급 절벽으로 인한 부동산 시장의 쇼크 우려가 커지고 있음.- 수도권 분양 물량이 8만5천840가구로 전체의 59%를 차지하며, 수도권 쏠림 현상이 심화할 것으로 예상됨.- 2010년 이후 분양 물량이 가장 적었던 2010년보다도 2만6000가구 적음.- HDC현대산업개발의 분양 물량 일부는 포함되지 않았으나, 전체 분양 계획 물량은 여전히 최저치임.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T08:52:00",
                        "title": "“내년 민간 아파트 분양 25년 만에 최저”…공급 절벽에 시장 쇼크 오...",
                        "summary": "- 내년 민간 아파트 분양 예정 물량이 15만 가구에 미치지 못할 것으로 예상되며, 이는 2000년 이후 최저치임.- 수도권 쏠림 현상이 심화될 것으로 보이며, 수도권 분양 물량은 전체의 59%를 차지함.- HDC현대산업개발의 분양 물량 일부는 통계에 포함되지 않았으나, 전체 분양 계획 물량은 15만7000여 가구로 추정됨.- 아파트 공급 절벽이 현실화되면서 시장에 쇼크를 줄 수 있는 상황임.- 2~3년 후 입주 물량 감소가 우려됨.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:43:00",
                        "title": "\"금리, 더 낮아지긴 어렵겠지?\" 상업용부동산 업계 '고민'",
                        "summary": "- 한국은행의 금리 인하 속도가 늦춰질 것으로 예상되면서 상업용부동산 업계의 고민이 깊어짐.- 미국 연준이 내년 금리 인하 횟수를 4회에서 2회로 조정함에 따라 한국은행의 추가 금리 인하 기대가 줄어듦.- HDC현대산업개발은 경기 안성시의 물류센터 공사에서 인허가 지연으로 준공 시점을 넘기고 PF대출을 인수함.- 국민연금이 국내 부동산 시장에 7500억 원을 투자할 계획으로 긍정적인 소식이 있음.- 글로벌 투자은행들이 내년 한국은행의 금리 인하를 예상하고 있어 시장 전망이 엇갈림.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-25T06:00:00",
                        "title": "‘산타랠리’ 기대감 사라졌다…한파 불어닥치는 건설업계",
                        "summary": "- HDC현대산업개발을 포함한 일부 건설사 주가가 1년 새 25% 이상 하락했음.- 부도업체 수가 지난해보다 40% 이상 증가했음.- 주택사업 경기 회복이 어려운 상황임.- 주택사업경기전망지수가 하락세를 보이고 있음.- 강력한 주택담보대출 규제로 수도권 집값이 하락세로 돌아섰음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T15:41:00",
                        "title": "'16년 리모델링 추진' 대치2단지 조합 2월 해산 \"재건축 선회 박차...",
                        "summary": "- 서울 강남구 개포동 '성원대치2단지' 리모델링 조합이 16년 만에 해산하고 재건축으로 방향을 전환함.- 해산총회는 2025년 2월 22일로 확정되었으며, 조합원 1489명 중 640명이 동의함.- 강남구청은 조합원들에게 법적 문제를 지적하며 해산 총회 개최를 의무사항으로 강조함.- 조합은 재건축 추진을 위해 여러 단계를 밟아나갈 계획임.- 대치2단지는 국내 1등 학세권으로 수요자들의 관심이 집중될 것으로 예상됨.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T14:15:00",
                        "title": "건설사 10곳 중 8곳 CEO 교체… 내년 과제 '비용절감·리스크 관리...",
                        "summary": "- 올해 대형 건설사 10곳 중 8곳이 CEO를 교체했음.- HDC현대산업개발은 정경구 CFO를 대표이사로 선임했음.- 건설업계의 불황 타개를 위한 비용 절감과 리스크 관리가 주요 과제로 부각되었음.- HDC현대산업개발은 재무구조가 안정적이라는 평가를 받고 있음.- 내년에도 원자재 가격 상승과 경기 침체로 어려움이 예상됨.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T12:08:00",
                        "title": "서울원 아이파크는 한 달째 분양, 입주자 모집 이어진다",
                        "summary": "- HDC현대산업개발의 '서울원 아이파크'가 한 달째 분양 중이며, 예비입주자 추첨 행사를 진행하고 있음.- 업계에서는 약 30%의 미분양이 예상되며, 분양가가 인근 지역보다 높아 논란이 되고 있음.- 서울원 아이파크는 총 3032가구로 조성되며, 일반분양 물량은 1856가구임.- 높은 분양가와 설계 문제로 미분양이 발생했을 가능성이 제기되고 있음.- 인근 지역 분양시장에 부정적인 영향을 미칠 우려가 있음.",
                        "emotion": "negative",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T09:54:00",
                        "title": "될 곳은 된다…검증 마친 신흥 상권, 후속 상가 분양에 실수요자 관심",
                        "summary": "- HDC현대산업개발이 공급하는 '스타오씨엘 에비뉴Ⅱ'가 후속 상가 분양으로 수요자들의 관심을 받고 있음.- 검증된 상업지역 내 후속 상가는 안정적인 가치 상승이 기대되며, 임대인에게도 유리한 조건을 제공함.- 인천 뮤지엄파크와의 근접성으로 인해 유동인구 증가가 예상되며, 지역 명소로 자리잡을 가능성이 높음.- 시티오씨엘 4단지와 5단지의 고정수요를 확보할 수 있어 투자 매력이 높음.- 완공은 2025년 1월로 예정되어 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-23T17:35:00",
                        "title": 'HDC현산 광주 화정 아이파크 해체 완료…"이름 바꾸고 재시공"',
                        "summary": "- HDC현대산업개발이 광주 화정 아이파크의 해체 공사를 완료했음.- 아파트 이름을 '광주 센테니얼 아이파크'로 변경하고 재시공을 시작할 예정임.- 붕괴사고로 인해 1심 선고가 다음 달 예정이며, 검찰은 책임자에게 징역형을 구형했음.- 서울시는 판결 후 행정처분을 결정할 방침임.- 재시공은 2027년 상반기 중에 준공될 예정임.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                ],
            },
            {
                "name": "카카오페이",
                "ticker": "A377300",
                "logo_image": "추후 반영",
                "ctry": "kr",
                "current_price": 26700,
                "change_rate": -1.11,
                "items_count": 9,
                "news": [
                    {
                        "date": "2024-12-25T09:28:00",
                        "title": '"700만의 선택" 하나 트래블로그 환전금액 3조·절감수수료 1700억',
                        "summary": "- 하나금융그룹의 환전 서비스 '트래블로그' 가입자 수가 700만명을 돌파했음.- 누적 환전금액은 3조원, 고객들이 아낀 수수료는 약 1700억원에 달함.- 함영주 회장은 내년에도 최상의 손님 경험을 제공하겠다고 약속했음.- 트래블로그는 24시간 모바일 환전 서비스로, 다양한 통화의 무료환전이 가능함.- 카카오페이와 제휴하여 '카카오페이 트래블로그 체크카드'를 출시했음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T16:48:00",
                        "title": "카카오페이, '2024 소비자 ESG 혁신대상' 소비자안전상 수상",
                        "summary": "- 카카오페이가 '2024 소비자 ESG 혁신대상'에서 소비자안전상 시니어안전부문을 수상했음.- '사각사각 페이스쿨' 시니어클래스를 통해 1년간 5000여 명에게 디지털 금융 교육을 제공했음.- 시니어 친화형 서비스인 '큰 글씨 홈'과 '가족보안지킴이'를 개발하여 시니어의 접근성을 향상시켰음.- 카카오페이는 ESG 비전 하에 디지털 금융 서비스의 안전성을 높이기 위해 지속적으로 노력하고 있음.- 이윤근 ESG협의체장은 앞으로도 모든 소비자가 디지털 금융에 접근할 수 있도록 하겠다고 밝혔음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T16:15:00",
                        "title": "\"AI 리스크 사전 점검\" 카카오, '기술윤리 보고서' 발간",
                        "summary": "- 카카오는 '2024 그룹 기술윤리 보고서'를 발간하여 기술 윤리 활동 성과를 공개했음.- 그룹 차원의 기술윤리 거버넌스를 강화하고 리스크 사전 점검에 중점을 두었음.- AI 윤리 신규 항목 신설, 안전한 AI 체크리스트 도입, 생성형 AI 활용 정책을 수립했음.- 카카오는 'Kakao AI Safety Initiative'를 구축하고 AI 동맹에 가입하여 국제 협력에 나섰음.- 카카오페이를 포함한 주요 계열사들도 기술윤리 활동을 확대했음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T14:25:00",
                        "title": '"해외주식 없었으면 어쩔뻔"… 카카오증권, 약 3년만에 예수금 1조 넘...',
                        "summary": "- 카카오페이증권이 설립 2년 9개월 만에 예수금 1조원을 돌파했음.- 예수금 증가의 주요 원인은 최대 5%의 이자 혜택과 미국 주식 거래 활성화임.- 종합계좌는 예수금 30만원까지 연 2.5%의 기본 이용료를 지급하고, 추가로 2.5%의 이용료를 제공함.- 최근 출시한 연금저축 상품은 가입 절차를 간소화하여 3주 만에 5만개의 계좌를 확보했음.- 카카오페이증권은 앞으로도 예수금 지속 성장을 목표로 하고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T11:10:00",
                        "title": "‘네카토’ 3色 보험시장 공략법… 최후의 승자는?",
                        "summary": "- 네이버페이, 카카오페이, 토스가 각각 다른 전략으로 보험시장에 접근하고 있음.- 카카오페이는 직접 상품 개발에 집중하며, 생활밀착형 상품과 미니보험을 출시했음.- 토스는 GA를 통해 설계사 모집을 확대하고 있으며, 고객 데이터베이스를 활용한 영업 전략을 구사하고 있음.- 네이버페이는 보험 비교·추천 서비스에 집중하고 있으나, 보험사와의 수수료 문제로 갈등이 발생하고 있음.- 카카오페이는 적자 규모가 확대되고 있지만, 충성도 높은 고객 확보에 중점을 두고 있음.",
                        "emotion": "neutral",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-24T06:00:00",
                        "title": '[단독]"서학개미 덕에"…카카오페이증권, 첫 분기 흑자전환',
                        "summary": "- 카카오페이증권이 출범 이후 처음으로 분기 흑자를 달성했음.- 해외주식 거래대금 증가와 부동산 PF 시장 회복이 주요 요인으로 작용했음.- 4분기 흑자 전환이 확실시되며, 모든 임직원에게 5일 포상 휴가가 지급됨.- 증권가는 카카오페이증권의 성과를 긍정적으로 평가하고 있으며, 내년 연간 흑자 전환 가능성도 언급됨.- 해외주식 거래가 지속적으로 증가하고 있으며, 수수료 인상에도 불구하고 거래대금이 늘어났음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-23T17:49:00",
                        "title": '"용돈 주고받기 경험한 50대, 송금서비스 새 고객"',
                        "summary": "- 카카오페이 가입자 중 약 33%가 50대 이상이며, 이들은 자녀들에 의해 송금 서비스를 처음 경험한 후 직접 사용하기 시작했음.- 50대 이상 유저들은 주로 경조사나 용돈 송금 목적으로 카카오페이를 이용하고 있음.- 카카오페이는 2016년 송금 서비스를 시작한 이후, 다양한 기능을 추가하며 월 송금 건수가 1억 건을 돌파했음.- 카카오페이는 내년에 대화형 송금 서비스와 송금 문구 추천 기능을 강화할 계획임.- 송금 시 감정을 담은 메시지를 전할 수 있는 기능이 사용자들에게 긍정적인 반응을 얻고 있음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-23T17:17:00",
                        "title": "[2024 히트상품 총결산] 하나카드, 즐거운 해외여행 `필수 카드`",
                        "summary": "- 하나카드의 트래블로그 서비스가 해외여행 필수품으로 자리잡았음.- 하나카드는 해외 체크카드 시장에서 49.9%의 점유율을 기록하며 19개월 연속 1위를 유지했음.- 트래블GO체크카드는 비자와 제휴하여 출시된 첫 번째 트래블카드로, 다양한 혜택을 제공함.- 카카오페이와 제휴한 카카오페이 트래블로그 체크카드도 출시되어 최대 10% 포인트 적립 혜택이 있음.- 트래블로그의 무료환전 서비스가 내년 말까지 연장되었음.",
                        "emotion": "positive",
                        "type": "news",
                    },
                    {
                        "date": "2024-12-23T15:29:00",
                        "title": "카카오페이, 연말 맞이 ‘올해도 쩐했습니다’ 캠페인 진행",
                        "summary": "- 카카오페이가 연말을 맞아 ‘올해도 쩐했습니다’ 캠페인을 진행하며, 영상, 기부 이벤트, 테마 송금봉투를 공개했음.- 캠페인은 돈과 마음을 전달하는 순간을 조명하고, 카카오페이의 기업 철학을 공유하고자 기획되었음.- 유병재와 협업하여 돈에 얽힌 사연을 소개하는 콘텐츠를 진행하며, 기부금 1억원을 세이브더칠드런에 전달했음.- 추가 기부를 위한 매칭그랜트 이벤트를 개최하여, 영상 좋아요 수에 따라 기부금이 증가하는 방식임.- 새로운 송금봉투는 연말 분위기를 담아 디자인되었으며, 다음달 6일까지 이용 가능함.",
                        "emotion": "positive",
                        "type": "news",
                    },
                ],
            },
        ]

        return result_data

    def get_top_stories_temp(self) -> TopStoriesResponse:
        # 1. 한국 뉴스 2일치 가져오기
        # 2. 미국 뉴스 2일치 가져오기
        # 3. 미국 공시 2일치 가져오기

        return TopStoriesResponse(
            name="test", ticker="test", logo_image="test", ctry="test", current_price=100, change_rate=100, news=[]
        )


def get_news_service() -> NewsService:
    return NewsService()
