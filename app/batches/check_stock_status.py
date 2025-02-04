from app.kispy.api import StockAPI
from app.database.crud import database
import logging

logger = logging.getLogger(__name__)


def _collect_domestic_stock_status():
    """
    모든 주식의 상장 및 거래 상태 수집
    """
    # API 클라이언트 초기화
    api = StockAPI()

    try:
        # stock_trend 테이블에서 모든 국내 티커 조회
        tickers = database._select(table="stock_trend", columns=["ticker"], market__in=["KOSPI", "KOSDAQ", "KONEX"])

        # 각 티커의 상태 확인
        for ticker_row in tickers:
            ticker = ticker_row[0]  # 첫 번째 열이 ticker

            try:
                status = api.get_domestic_stock_status(ticker)

                # stock_trend 테이블 업데이트
                database._update(
                    table="stock_trend",
                    sets={
                        "is_trading_stopped": status.get("is_trading_stopped", False),
                        "is_delisted": status.get("is_delisted", False),
                    },
                    ticker=ticker,
                )

                # 로깅
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
    # API 클라이언트 초기화
    api = StockAPI()

    try:
        # stock_trend 테이블에서 모든 해외 주식 티커와 마켓 유형 코드 조회
        stocks = database._select(table="stock_trend", columns=["ticker", "market"], market__in=["NYS", "NAS", "AMEX"])

        # 각 해외 주식의 상태 업데이트
        for stock_row in stocks:
            ticker, market = stock_row[0], stock_row[1]
            market_code_map = {"NYS": 513, "NAS": 512, "AMEX": 529}
            logger.info(f"Checking status for {ticker} in {market}")
            market_type_code = market_code_map.get(market, 0)

            if market_type_code == 0:
                continue

            try:
                # 해외 주식 상태 확인
                status = api.get_overseas_stock_status(ticker, market_type_code)

                # stock_trend 테이블 업데이트
                database._update(
                    table="stock_trend",
                    sets={
                        "is_trading_stopped": status.get("is_trading_stopped", False),
                        "is_delisted": status.get("is_delisted", False),
                    },
                    ticker=ticker,
                )

                # 로깅
                if status.get("is_trading_stopped", False):
                    logging.info(f"해외 주식 {ticker} 거래 정지: {status.get('name', '')} ({status.get('market', '')})")

                if status.get("is_delisted", False):
                    logging.info(f"해외 주식 {ticker} 상장 폐지: {status.get('name', '')} ({status.get('market', '')})")

            except Exception as e:
                logging.error(f"해외 주식 {ticker} 상태 확인 중 오류: {e}")

        logging.info("해외 주식 상태 업데이트 완료")

    except Exception as e:
        logging.error(f"해외 주식 상태 수집 중 오류: {e}")


def main():
    database._update(
        table="stock_trend",
        sets={"is_trading_stopped": False, "is_delisted": False},
        ticker__in=database._select(table="stock_trend", columns=["ticker"]),
    )
    # 로깅 설정
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # 국내 주식 상태 수집 및 업데이트
    _collect_domestic_stock_status()

    # 해외 주식 상태 수집 및 업데이트
    _collect_overseas_stock_status()


if __name__ == "__main__":
    main()
