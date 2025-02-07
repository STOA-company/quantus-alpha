import numpy as np
import pandas as pd
from scipy import stats
from app.database.crud import database
import logging
from app.utils.activate_utils import activate_stock, deactivate_stock
from app.kispy.sdk import fetch_stock_data

logger = logging.getLogger(__name__)


ZSCORE_THRESHOLD = 1.5


def detect_and_deactivate_stock_trend_outliers(nation: str):
    """
    stock_trend 테이블의 변화율 필드에서 이상치 탐지 및 is_activate 비활성화
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
    deactivate_tickers = set()

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

                deactivate_tickers.update(outliers["ticker"].tolist())

    # 이상치 티커들 비활성화
    for ticker in deactivate_tickers:
        try:
            deactivate_stock(ticker)

            logger.info(f"티커 {ticker} 비활성화")

        except Exception as e:
            logger.error(f"티커 {ticker} 비활성화 실패: {e}")

    logger.info(f"총 {len(deactivate_tickers)}개의 티커 비활성화")

    return list(deactivate_tickers)


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


def check_and_recollect_outliers_us():
    deactivated_tickers = detect_and_deactivate_stock_trend_outliers(nation="US")

    for ticker in deactivated_tickers:
        fetch_and_update_stock_data(ticker, nation="US")
        activate_stock(ticker)


def check_and_recollect_outliers_kr():
    deactivated_tickers = detect_and_deactivate_stock_trend_outliers(nation="KR")

    for ticker in deactivated_tickers:
        fetch_and_update_stock_data(ticker, nation="KR")
        activate_stock(ticker)


if __name__ == "__main__":
    deactivated_tickers = detect_and_deactivate_stock_trend_outliers(nation="KR")

    for ticker in deactivated_tickers:
        fetch_and_update_stock_data(ticker, nation="KR")
        activate_stock(ticker)
        print(ticker)

    detect_and_deactivate_stock_trend_outliers(nation="KR")
