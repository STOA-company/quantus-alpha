from app.database.crud import database
from typing import Literal
from app.kispy.sdk import CustomKisClientV2
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class StockUtils:
    def __init__(self, nation: Literal["kr", "us"]):
        self.db = database
        self.kispy = CustomKisClientV2(nation=nation.upper())
        self.nation = nation

    def get_top_gainers(self, period: Literal["rt", "1d", "1w", "1m", "6m", "1y"], limit: int = 10):
        stocks = self.db._select(
            table="stock_trend",
            columns=["ticker"],
            order=f"change_{period}",
            ascending=False,
            ctry=self.nation,
            is_activate=True,
            is_trading_stopped=False,
            is_delisted=False,
            limit=limit,
        )
        tickers = [stock[0] for stock in stocks]
        return tickers

    def get_top_losers(self, period: Literal["rt", "1d", "1w", "1m", "6m", "1y"], limit: int = 10):
        stocks = self.db._select(
            table="stock_trend",
            columns=["ticker"],
            order=f"change_{period}",
            ascending=True,
            ctry=self.nation,
            is_activate=True,
            is_trading_stopped=False,
            is_delisted=False,
            limit=limit,
        )
        tickers = [stock[0] for stock in stocks]
        return tickers

    def activate_stock(self, ticker: str):
        self.db._update(
            table="stock_information",
            sets={"is_activate": True},
            ticker=ticker,
        )

        self.db._update(
            table="stock_trend",
            sets={"is_activate": True},
            ticker=ticker,
        )

    def deactivate_stock(self, ticker: str):
        self.db._update(
            table="stock_information",
            sets={"is_activate": False},
            ticker=ticker,
        )

        self.db._update(
            table="stock_trend",
            sets={"is_activate": False},
            ticker=ticker,
        )

    def update_time_series_data(self, ticker: str):
        try:
            ticker_ = ticker[1:] if self.nation == "kr" else ticker
            self.deactivate_stock(ticker)

            df = self.kispy.fetch_stock_data(ticker_)
            if df is None:
                logger.warning(f"데이터를 가져올 수 없음: {ticker}")
                return False

            for _, row in df.iterrows():
                try:
                    existing = self.db._select(
                        table=f"stock_{self.nation}_1d",
                        columns=["Ticker"],
                        Ticker=ticker,
                        Date=pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                    )

                    data = {
                        "Date": pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                        "Open": float(row["Open"]),
                        "High": float(row["High"]),
                        "Low": float(row["Low"]),
                        "Close": float(row["Close"]),
                        "Volume": int(row["Volume"]),
                    }

                    if existing:
                        self.db._update(table=f"stock_{self.nation}_1d", sets=data, Ticker=ticker, Date=data["Date"])
                    else:
                        data["Ticker"] = ticker
                        self.db._insert(table=f"stock_{self.nation}_1d", data=data)

                except Exception as e:
                    logger.error(f"데이터 업데이트 실패 {ticker}, date: {row['Date']}: {str(e)}")
                    continue

            self.activate_stock(ticker)
            return True

        except Exception as e:
            logger.error(f"주가 데이터 업데이트 실패 {ticker}: {str(e)}")
            self.activate_stock(ticker)
            return False

    def update_stock_trend(self, tickers: list[str]):
        try:
            # 각 ticker의 최신 날짜 조회
            latest_dates = self.db._select(
                table=f"stock_{self.nation}_1d",
                columns=["Ticker"],
                group_by=["Ticker"],
                aggregates={"max_date": ("Date", "max")},
                Ticker__in=tickers,
            )

            # 일별 데이터 수집
            daily_data = []
            select_columns = ["Ticker", "Date", "Close", "Volume", "Open", "High", "Low"]

            for ticker_row in latest_dates:
                ticker = ticker_row[0]
                max_date = ticker_row[1]  # aggregates 결과

                # 1년치 데이터 조회
                one_year_ago = (pd.to_datetime(max_date) - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
                ticker_data = self.db._select(
                    table=f"stock_{self.nation}_1d",
                    columns=select_columns,
                    Ticker=ticker,
                    Date__gte=one_year_ago,
                    order="Date",
                    ascending=False,
                )
                daily_data.extend(ticker_data)

            # 데이터프레임 변환 및 처리
            df = pd.DataFrame(daily_data, columns=select_columns)
            df = df.sort_values(by=["Ticker", "Date"], ascending=[True, False])

            # 거래대금 계산
            df["volume_change"] = (df["Open"] + df["High"] + df["Low"] + df["Close"]) / 4 * df["Volume"]

            # 현재 데이터와 이전 데이터 구분
            current_data = df.groupby("Ticker").first().reset_index()
            prev_data = df.groupby("Ticker").nth(1).reset_index()

            # 장 마감 시간 설정
            market_close_times = {
                "KR": {"hour": 15, "minute": 30, "second": 0},
                "US": {"hour": 16, "minute": 0, "second": 0},
            }
            close_time = market_close_times[self.nation.upper()]

            current_data["Date"] = pd.to_datetime(current_data["Date"]).apply(
                lambda x: x.replace(hour=close_time["hour"], minute=close_time["minute"], second=close_time["second"])
            )

            # 결과 데이터프레임 생성
            results = pd.DataFrame()
            results["ticker"] = current_data["Ticker"]
            results["last_updated"] = current_data["Date"]
            results["current_price"] = current_data["Close"].round(4)
            results["prev_close"] = prev_data["Close"].round(4)
            results["change_1d"] = ((current_data["Close"] - prev_data["Close"]) / prev_data["Close"] * 100).round(4)
            results["volume_1d"] = current_data["Volume"].round(4)
            results["volume_change_1d"] = current_data["volume_change"].round(4)
            results["change_sign"] = np.where(
                current_data["Close"] > prev_data["Close"], 1, np.where(current_data["Close"] < prev_data["Close"], -1, 0)
            )

            # 기간별 변화율 계산
            periods = {"1w": 5, "1m": 20, "6m": 120, "1y": None}

            for period, n_records in periods.items():
                period_data = df.copy() if n_records is None else df.groupby("Ticker").head(n_records)

                period_start_prices = period_data.groupby("Ticker").last()[["Close"]].reset_index()
                period_volumes = (
                    period_data.groupby("Ticker").agg({"Volume": "sum", "volume_change": "sum"}).reset_index()
                )

                # 가격 변화율 계산
                results = results.merge(
                    period_start_prices, left_on="ticker", right_on="Ticker", suffixes=("", f"_start_{period}")
                )
                results[f"change_{period}"] = (
                    (results["current_price"] - results["Close"]) / results["Close"] * 100
                ).round(4)
                results = results.drop(columns=["Close"])

                # 거래량 관련 데이터 병합
                results = results.merge(period_volumes, left_on="ticker", right_on="Ticker", suffixes=("", f"_{period}"))
                results[f"volume_{period}"] = results["Volume"].round(4)
                results[f"volume_change_{period}"] = results["volume_change"].round(4)
                results = results.drop(columns=["Volume", "volume_change"])

            # 데이터베이스 업데이트를 위한 데이터 준비
            update_data = []
            for _, row in results.iterrows():
                update_dict = {
                    "ticker": row["ticker"],
                    "last_updated": row["last_updated"],
                    "current_price": row["current_price"],
                    "prev_close": row["prev_close"],
                    "change_sign": row["change_sign"],
                    "change_rt": row["change_1d"],
                    "change_1d": row["change_1d"],
                    "change_1w": row["change_1w"],
                    "change_1m": row["change_1m"],
                    "change_6m": row["change_6m"],
                    "change_1y": row["change_1y"],
                    "volume_rt": row["volume_1d"],
                    "volume_1d": row["volume_1d"],
                    "volume_1w": row["volume_1w"],
                    "volume_1m": row["volume_1m"],
                    "volume_6m": row["volume_6m"],
                    "volume_1y": row["volume_1y"],
                    "volume_change_rt": row["volume_change_1d"],
                    "volume_change_1d": row["volume_change_1d"],
                    "volume_change_1w": row["volume_change_1w"],
                    "volume_change_1m": row["volume_change_1m"],
                    "volume_change_6m": row["volume_change_6m"],
                    "volume_change_1y": row["volume_change_1y"],
                }
                update_data.append(update_dict)

            # 벌크 업데이트 실행
            self.db._bulk_update(table="stock_trend", data=update_data, key_column="ticker")
            logger.info(f"Successfully updated {len(update_data)} records in stock_trend table")

        except Exception as e:
            logger.error(f"Error in update_stock_trend: {str(e)}")
            raise e

    def update_top_gainers(self):
        tickers = []
        for period in ["rt", "1d", "1w", "1m", "6m", "1y"]:
            tickers.extend(self.get_top_gainers(period))
        tickers = list(set(tickers))
        for ticker in tickers:
            self.update_time_series_data(ticker)
        self.update_stock_trend(tickers)

    def update_top_losers(self):
        tickers = []
        for period in ["rt", "1d", "1w", "1m", "6m", "1y"]:
            tickers.extend(self.get_top_losers(period))
        tickers = list(set(tickers))
        for ticker in tickers:
            self.update_time_series_data(ticker)
        self.update_stock_trend(tickers)


us_stock_utils = StockUtils(nation="us")
kr_stock_utils = StockUtils(nation="kr")
