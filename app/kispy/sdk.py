import logging
from kispy import KisAuth, KisClientV2
import pandas as pd
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)

load_dotenv(".env.dev")

auth = KisAuth(
    app_key=os.getenv("KIS_APP_KEY"),
    secret=os.getenv("KIS_SECRET"),
    account_no=os.getenv("KIS_ACCOUNT_NO"),
    is_real=True,  # 실전투자: True, 모의투자: False
)


def fetch_stock_data(symbol: str, market: str):
    try:
        # KisClientV2 초기화
        client = KisClientV2(auth=auth, nation="KR" if market in ["KOSPI", "KOSDAQ"] else "US")

        # 전체 기간 일봉 데이터 조회
        # period="d": 일봉
        # is_adjust=True: 수정주가 반영
        ohlcv = client.fetch_ohlcv(symbol=symbol, period="d", is_adjust=True)

        if not ohlcv:
            logger.warning(f"No OHLCV data for {symbol}")
            return None

        logger.info(f"Fetched OHLCV data for {symbol}")

        return pd.DataFrame(
            [
                {
                    "Date": item.date,
                    "Open": item.open,
                    "High": item.high,
                    "Low": item.low,
                    "Close": item.close,
                    "Volume": item.volume,
                }
                for item in ohlcv
            ]
        )

    except Exception as e:
        logger.error(f"Error fetching price data from KIS API for {symbol}: {str(e)}")
        return None
