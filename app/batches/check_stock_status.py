from app.kispy.manager import KISAPIManager
import pandas as pd
from app.database.crud import database
import logging

logger = logging.getLogger(__name__)


def _collect_domestic_stock_status():
    """
    모든 주식의 상장 및 거래 상태 수집
    """
    api = KISAPIManager().get_api()

    try:
        tickers = database._select(table="stock_trend", columns=["ticker"], market__in=["KOSPI", "KOSDAQ", "KONEX"])

        for ticker_row in tickers:
            ticker = ticker_row[0]

            try:
                status = api.get_domestic_stock_status(ticker)

                database._update(
                    table="stock_trend",
                    sets={
                        "is_trading_stopped": status.get("is_trading_stopped", False),
                        "is_delisted": status.get("is_delisted", False),
                    },
                    ticker=ticker,
                )

                if status.get("is_trading_stopped", False):
                    logging.info(f"국내 주식 {ticker} 거래 정지: {status.get('name', '')}")

                if status.get("is_delisted", False):
                    logging.info(f"국내 주식 {ticker} 상장 폐지: {status.get('name', '')}")

            except Exception as e:
                logging.error(f"국내 주식 {ticker} 상태 확인 중 오류: {e}")

        logging.info("국내 주식 상태 업데이트 완료")

    except Exception as e:
        logging.error(f"국내 주식 상태 수집 중 오류: {e}")


def _collect_overseas_stock_status():
    """
    모든 해외 주식의 상장 및 거래 상태 수집
    """
    api = KISAPIManager().get_api()

    try:
        stocks = database._select(table="stock_trend", columns=["ticker", "market"], market__in=["NYS", "NAS", "AMEX"])

        for stock_row in stocks:
            ticker, market = stock_row[0], stock_row[1]
            market_code_map = {"NYS": 513, "NAS": 512, "AMEX": 529}
            logger.info(f"Checking status for {ticker} in {market}")
            market_type_code = market_code_map.get(market, 0)

            if market_type_code == 0:
                continue

            try:
                status = api.get_overseas_stock_status(ticker, market_type_code)

                database._update(
                    table="stock_trend",
                    sets={
                        "is_trading_stopped": status.get("is_trading_stopped", False),
                        "is_delisted": status.get("is_delisted", False),
                    },
                    ticker=ticker,
                )

                if status.get("is_trading_stopped", False):
                    logging.info(f"해외 주식 {ticker} 거래 정지: {status.get('name', '')} ({status.get('market', '')})")

                if status.get("is_delisted", False):
                    logging.info(f"해외 주식 {ticker} 상장 폐지: {status.get('name', '')} ({status.get('market', '')})")

            except Exception as e:
                logging.error(f"해외 주식 {ticker} 상태 확인 중 오류: {e}")

        logging.info("해외 주식 상태 업데이트 완료")

    except Exception as e:
        logging.error(f"해외 주식 상태 수집 중 오류: {e}")


def deactivate_zero_volume():
    """
    거래량이 0이고 거래정지가 아닌 종목들을 비활성화
    """
    try:
        # 활성화된 종목들 중 거래량 데이터 조회
        df = database._select(
            table="stock_trend",
            columns=[
                "ticker",
                "market",
                "volume_rt",
                "volume_1d",
                "volume_1w",
                "volume_1m",
                "volume_6m",
                "volume_1y",
                "is_trading_stopped",
            ],
            is_activate=1,
        )

        if not df:
            logger.warning("No active stocks found in stock_trend table")
            return []

        df = pd.DataFrame(
            df,
            columns=[
                "ticker",
                "market",
                "volume_rt",
                "volume_1d",
                "volume_1w",
                "volume_1m",
                "volume_6m",
                "volume_1y",
                "is_trading_stopped",
            ],
        )

        # 모든 거래량 필드가 0이고 거래정지가 아닌 종목 필터링
        volume_columns = ["volume_rt", "volume_1d", "volume_1w", "volume_1m", "volume_6m", "volume_1y"]

        zero_volume_mask = df[volume_columns].fillna(0).eq(0).all(axis=1)
        not_stopped_mask = df["is_trading_stopped"] != 1

        deactivate_df = df[zero_volume_mask & not_stopped_mask]

        deactivate_tickers = list(zip(deactivate_df["ticker"], deactivate_df["market"]))

        # 비활성화 처리
        for ticker, market in deactivate_tickers:
            try:
                database._update(table="stock_trend", sets={"is_activate": 0}, ticker=ticker)

                database._update(table="stock_information", sets={"is_activate": 0}, ticker=ticker)

                logger.info(f"Deactivated zero volume ticker: {ticker}")

            except Exception as e:
                logger.error(f"Failed to deactivate ticker {ticker}: {e}")

        logger.info(f"Total {len(deactivate_tickers)} zero volume tickers deactivated")

        return deactivate_tickers

    except Exception as e:
        logger.error(f"Error in detect_and_deactivate_zero_volume: {e}")
        return []


def iscd_stat_cls_code_batch():
    api = KISAPIManager().get_api()
    cared_tickers = []
    warned_tickers = []
    tickers = database._select(table="stock_trend", columns=["ticker"])
    for ticker in tickers:
        iscd_stat_cls_code = api.iscd_stat_cls_code(ticker)
        logger.info(iscd_stat_cls_code)
        if iscd_stat_cls_code is None:
            logger.info(f"No iscd_stat_cls_code for {ticker}")
            continue
        if iscd_stat_cls_code == "51":
            cared_tickers.append(ticker)
        elif iscd_stat_cls_code == "52":
            warned_tickers.append(ticker)

    database._update(table="stock_trend", sets={"is_cared": 1}, ticker__in=cared_tickers)
    database._update(table="stock_trend", sets={"is_warned": 1}, ticker__in=warned_tickers)


def main():
    database._update(
        table="stock_trend",
        sets={"is_trading_stopped": False, "is_delisted": False},
        ticker__in=database._select(table="stock_trend", columns=["ticker"]),
    )
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    _collect_domestic_stock_status()

    _collect_overseas_stock_status()

    deactivate_zero_volume()


if __name__ == "__main__":
    iscd_stat_cls_code_batch()
