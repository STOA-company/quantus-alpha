from kispy import KisAuth, KisClientV2
from kispy.models.market import OHLCV
import pandas as pd
from app.core.config import settings
from datetime import datetime
from app.core.logger import setup_logger

logger = setup_logger(__name__)

auth = KisAuth(
    app_key=settings.KIS_APP_KEY,
    secret=settings.KIS_SECRET,
    account_no=settings.KIS_ACCOUNT_NO,
    is_real=True,  # 실전투자: True, 모의투자: False
)


class CustomKisClientV2(KisClientV2):
    def __init__(self, nation: str):
        self.auth = auth
        super().__init__(auth=self.auth, nation=nation)

    def fetch_ohlcv(self, symbol: str, *args, **kwargs):
        print("NATION", self.nation)
        if self.nation == "kr":
            if kwargs.get("period") in ["d", "w", "M"]:
                try:
                    period_map = {"d": "D", "w": "W", "M": "M"}
                    histories = self.client.domestic_stock.quote.get_stock_price_history(
                        stock_code=symbol,
                        start_date=kwargs.get("start_date", "20000101"),
                        end_date=kwargs.get("end_date", datetime.now().strftime("%Y%m%d")),
                        period=period_map[kwargs.get("period", "d")],
                        is_adjust=kwargs.get("is_adjust", True),
                    )

                    if not histories:
                        return []

                    result = []
                    for history in histories:
                        try:
                            ohlcv = OHLCV(
                                date=datetime.strptime(history["stck_bsop_date"], "%Y%m%d"),
                                open=history["stck_oprc"],
                                high=history["stck_hgpr"],
                                low=history["stck_lwpr"],
                                close=history["stck_clpr"],
                                volume=history["acml_vol"],
                            )
                            result.append(ohlcv)
                        except Exception as e:
                            logger.error(f"Error processing record: {history}, Error: {e}")
                            continue

                    return result[: kwargs.get("limit")] if kwargs.get("limit") else result
                except Exception as e:
                    logger.error(f"Error processing domestic stock data: {e}")
                    raise
        else:
            print("SYMBOL", symbol)
            return super().fetch_ohlcv(symbol, *args, **kwargs)

    def fetch_stock_data(self, symbol: str):
        try:
            # 전체 기간 일봉 데이터 조회
            # period="d": 일봉
            # is_adjust=True: 수정주가 반영
            print("SYMBOL", symbol)
            ohlcv = self.fetch_ohlcv(symbol=symbol, period="d", is_adjust=True)
            print("OHLCV", ohlcv)
            if not ohlcv:
                print("NO OHLCV")
                logger.warning(f"No OHLCV data for {symbol}")
                return None

            today = datetime.now().date()
            ohlcv = [item for item in ohlcv if item.date.date() != today]

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
