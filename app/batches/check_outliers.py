import numpy as np
import pandas as pd
from scipy import stats
from app.database.crud import database
import logging
from app.kispy.sdk import fetch_stock_data
from app.utils.activate_utils import activate_stock

logger = logging.getLogger(__name__)


ZSCORE_THRESHOLD = 3


def detect_stock_trend_outliers(nation: str):
    """
    stock_trend change_rt 이상치 탐지
    """
    if nation == "US":
        market = ["NAS", "NYS", "AMS"]
    elif nation == "KR":
        market = ["KOSPI", "KOSDAQ"]
    else:
        raise ValueError(f"Invalid nation: {nation}")

    df = database._select(
        table="stock_trend",
        columns=[
            "ticker",
            "market",
            "change_rt",
            "change_1d",
            "change_1w",
            "change_1m",
            "change_6m",
            "change_1y",
        ],
        is_trading_stopped=0,
        is_delisted=0,
        market__in=market,
    )

    df = pd.DataFrame(
        df,
        columns=[
            "ticker",
            "market",
            "change_rt",
            "change_1d",
            "change_1w",
            "change_1m",
            "change_6m",
            "change_1y",
        ],
    )

    change_columns = ["change_rt", "change_1d", "change_1w", "change_1m", "change_6m", "change_1y"]
    outlier_tickers = set()

    for column in change_columns:
        # 결측치가 있는 행 제외
        valid_data = df.dropna(subset=[column])

        if len(valid_data) > 0:  # 데이터가 존재하는 경우에만 처리
            z_scores = np.abs(stats.zscore(valid_data[column]))

            outliers = valid_data[z_scores > ZSCORE_THRESHOLD]

            if not outliers.empty:
                logger.info(f"\nOutliers in {column}:")
                logger.info(f"Tickers: {outliers['ticker'].tolist()}")
                logger.info(f"Values: {outliers[column].tolist()}")
                logger.info(f"Z-scores: {z_scores[z_scores > ZSCORE_THRESHOLD].tolist()}")

                outlier_tickers.update(outliers["ticker"].tolist())

    return list(outlier_tickers)


def fetch_and_update_stock_data(ticker: str, nation: str):
    try:
        logger.info(f"Starting data update process for {ticker}")

        # 새로운 데이터 가져오기
        ticker_ = ticker[1:] if nation == "KR" else ticker
        new_data = fetch_stock_data(symbol=ticker_, nation=nation)
        if new_data is None:
            logger.error(f"Failed to fetch new data for {ticker}")
            return None

        # 데이터 업데이트
        result = _update_price_data(ticker=ticker, df=new_data, nation=nation)
        return result

    except Exception as e:
        logger.error(f"Error in fetch_and_update_stock_data for {ticker}: {str(e)}")
        return None


def _update_price_data(ticker: str, df: pd.DataFrame, nation: str):
    try:
        logger.info(f"Updating price data for {ticker}")

        table = "stock_kr_1d" if nation == "KR" else "stock_us_1d"

        existing_data = database._select(table=table, columns=["Category", "Market"], Ticker=ticker, limit=1)
        category = existing_data[0][0] if existing_data else ""
        market = existing_data[0][1] if existing_data else ""

        if nation == "KR":
            stock_info = database._select(
                table="stock_information", columns=["kr_name", "market"], ticker=ticker, limit=1
            )

            logger.info(f"stock_info: {stock_info}")

            if not stock_info:
                logger.warning(f"No stock information found for {ticker}")
                return None

            kr_name = stock_info[0][0]
            market = stock_info[0][1]

        df = df.reset_index() if "Date" not in df.columns else df

        update_data = []
        for _, row in df.iterrows():
            base_data = {
                "Ticker": ticker,
                "Date": row["Date"],
                "Open": row["Open"],
                "High": row["High"],
                "Low": row["Low"],
                "Close": row["Close"],
                "Volume": row["Volume"],
                "Market": market,
                "Category": category,
            }

            if nation == "KR":
                base_data.update({"Name": kr_name, "Isin": ""})

            update_data.append(base_data)

        database._delete(
            table=table,
            Ticker=ticker,
        )

        for data in update_data:
            database._insert(table=table, sets=data)

        logger.info(f"Successfully updated {len(update_data)} records for {ticker}")
        return len(update_data)

    except Exception as e:
        logger.error(f"Error updating price data for {ticker}: {str(e)}")
        raise


def check_and_recollect_outliers(nation: str):
    outlier_tickers = detect_stock_trend_outliers(nation=nation)

    if not outlier_tickers:
        return

    database._update(
        table="stock_trend",
        sets={"is_activate": 0},
        ticker__in=outlier_tickers,
    )

    for ticker in outlier_tickers:
        try:
            fetch_and_update_stock_data(ticker, nation=nation)
            activate_stock(ticker)
        except Exception as e:
            logger.error(f"Failed to update {ticker}: {str(e)}")


if __name__ == "__main__":
    # 테스트 용
    # tickers = ["A340930", "A419530", "A033790", "A090710"]
    # for ticker in tickers:
    #     fetch_and_update_stock_data(ticker=ticker, nation="KR")

    check_and_recollect_outliers(nation="KR")
