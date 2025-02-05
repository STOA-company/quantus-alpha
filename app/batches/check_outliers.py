import numpy as np
import pandas as pd
from scipy import stats
from app.database.crud import database
import logging

logger = logging.getLogger(__name__)


def detect_and_deactivate_stock_trend_outliers():
    """
    stock_trend 테이블의 변화율 필드에서 이상치 탐지 및 is_activate 비활성화
    """
    df = database._select(
        table="stock_trend",
        columns=["ticker", "change_rt", "change_1d", "change_1w", "change_1m", "change_6m", "change_1y", "is_activate"],
    )

    df = pd.DataFrame(
        df,
        columns=["ticker", "change_rt", "change_1d", "change_1w", "change_1m", "change_6m", "change_1y", "is_activate"],
    )

    change_columns = ["change_rt", "change_1d", "change_1w", "change_1m", "change_6m", "change_1y"]

    deactivate_tickers = set()

    for column in change_columns:
        data = df[column].dropna()

        z_scores = np.abs(stats.zscore(data))
        logger.info(f"Z-scores: {z_scores}")

        # 이상치 기준 (Z-score > 3)
        outlier_mask = z_scores > 3
        column_outliers = df.loc[outlier_mask, ["ticker", column]]
        logger.info(f"Column outliers: {column_outliers}")

        # 이상치 티커 추가
        deactivate_tickers.update(column_outliers["ticker"])

    # 이상치 티커들 비활성화
    for ticker in deactivate_tickers:
        try:
            database._update(table="stock_trend", sets={"is_activate": 0}, ticker=ticker)

            database._update(table="stock_information", sets={"is_activate": 0}, ticker=ticker)

            logger.info(f"티커 {ticker} 비활성화")

        except Exception as e:
            logger.error(f"티커 {ticker} 비활성화 실패: {e}")

    logger.info(f"총 {len(deactivate_tickers)}개의 티커 비활성화")

    return list(deactivate_tickers)


if __name__ == "__main__":
    deactivated_tickers = detect_and_deactivate_stock_trend_outliers()

    print("비활성화된 티커:")
    for ticker in deactivated_tickers:
        print(ticker)
