import logging

from app.database.crud import database
from app.kispy.manager import KISAPIManager

logger = logging.getLogger(__name__)


def _collect_domestic_stock_status():
    """
    모든 주식의 상장 및 거래 상태 수집
    """
    api = KISAPIManager().get_api()

    try:
        tickers = database._select(table="stock_information", columns=["ticker"], market__in=["KOSPI", "KOSDAQ"])

        for ticker_row in tickers:
            ticker = ticker_row[0]

            try:
                status = api.get_domestic_stock_status(ticker)

                database._update(
                    table="stock_information",
                    sets={
                        "is_trading_stopped": status.get("is_trading_stopped", False),
                        "is_delisted": status.get("is_delisted", False),
                    },
                    ticker=ticker,
                )

                if status.get("is_trading_stopped", False):
                    logging.info(f"국내 주식 {ticker} 거래 정지: {status.get('name', '')}")
                    database._update(table="stock_information", sets={"is_trading_stopped": 1}, ticker=ticker)

                if status.get("is_delisted", False):
                    logging.info(f"국내 주식 {ticker} 상장 폐지: {status.get('name', '')}")
                    database._update(table="stock_information", sets={"is_delisted": 1}, ticker=ticker)
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
        stocks = database._select(
            table="stock_information", columns=["ticker", "market"], market__in=["NYS", "NAS", "AMEX"]
        )

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
                    table="stock_information",
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


def iscd_stat_cls_code_batch():
    api = KISAPIManager().get_api()
    cared_tickers = []
    warned_tickers = []
    tickers = database._select(table="stock_information", columns=["ticker"], ctry="kr")
    tickers = [ticker[0] for ticker in tickers]
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

    database._update(table="stock_information", sets={"is_cared": 1}, ticker__in=cared_tickers)
    database._update(table="stock_information", sets={"is_warned": 1}, ticker__in=warned_tickers)


def check_warned_stock_us_batch():
    tickers = database._select(table="stock_information", columns=["ticker"], ctry="us")
    tickers = [ticker[0] for ticker in tickers]
    database._update(table="stock_information", sets={"is_warned": 0}, ticker__in=tickers)
    stocks = database._select(
        table="USA_stock_factors",
        columns=["ticker", "last_close", "market_cap"],
        market_cap__lt=50000000,  # market_cap < 50,000,000
        last_close__lt=1,  # last_close < 1
    )
    tickers = [stock[0].split("-")[0] for stock in stocks]
    database._update(table="stock_information", sets={"is_warned": 1}, ticker__in=tickers)


if __name__ == "__main__":
    _collect_domestic_stock_status()
