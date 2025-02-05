import logging
import requests
from app.core.config import settings

logger = logging.getLogger(__name__)


class KISAPI:
    def __init__(self):
        """Initialize the Korean Stock API client"""
        self.app_key = settings.KIS_APP_KEY
        self.app_secret = settings.KIS_SECRET
        self.base_url = "https://openapi.koreainvestment.com:9443"
        self.access_token = self._get_access_token()

    def _get_access_token(self) -> str:
        """접근 토큰 발급"""
        url = f"{self.base_url}/oauth2/tokenP"

        data = {"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret}

        response = requests.post(url, json=data)
        response_data = response.json()

        return response_data.get("access_token")

    def get_domestic_stock_status(self, stock_code: str) -> dict:
        """
        개별 종목의 거래중지/상장폐지 여부 조회

        Args:
            stock_code (str): 종목 코드

        Returns:
            dict: 종목 상태 정보 {
                'ticker': 종목코드,
                'is_trading_stopped': 거래정지여부,
                'is_delisted': 상장폐지여부
            }
        """
        try:
            url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/search-stock-info"

            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {self.access_token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": "CTPF1002R",
                "custtype": "P",  # 개인
            }

            params = {
                "PDNO": stock_code,
                "PRDT_TYPE_CD": "300",  # 주식, ETF, ETN, ELW
            }

            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                raise Exception(f"API request failed with status {response.status_code}")

            data = response.json()

            if not data or "output" not in data:
                raise Exception(f"Failed to fetch data for {stock_code}")

            output = data["output"]

            return {
                "ticker": stock_code,
                "is_trading_stopped": output.get("tr_stop_yn", "N") == "Y",  # 거래정지여부
                "is_delisted": bool(output.get("lstg_abol_dt", "")),  # 상장폐지일이 있으면 상장폐지
                "name": output.get("prdt_abrv_name", ""),  # 종목명
                "message": data.get("msg1", "").strip(),  # API 응답 메시지
            }

        except Exception as e:
            print(f"Error fetching status for {stock_code}: {str(e)}")
            return {"ticker": stock_code, "error": str(e), "is_trading_stopped": False, "is_delisted": False}

    def get_overseas_stock_status(self, ticker: str, market_type_code: int) -> dict:
        """
        해외 주식의 거래중지/상장폐지 여부 조회

        Args:
            ticker (str): 종목 티커 (예: AAPL)
            market_type_code (int): 시장 유형 코드 (예: 512 - 미국 나스닥)

        Returns:
            dict: 종목 상태 정보 {
                'ticker': 종목티커,
                'is_trading_stopped': 거래정지여부,
                'is_delisted': 상장폐지여부
            }
        """
        try:
            url = f"{self.base_url}/uapi/overseas-price/v1/quotations/search-info"

            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {self.access_token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": "CTPF1702R",
                "custtype": "P",  # 개인
            }

            params = {"PDNO": ticker, "PRDT_TYPE_CD": market_type_code}

            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                raise Exception(f"API request failed with status {response}")

            data = response.json()

            if not data or "output" not in data:
                raise Exception(f"Failed to fetch data for {ticker}")

            output = data["output"]

            # 거래 정지 코드 정의
            trading_stop_codes = ["02", "03", "04", "05", "06"]

            return {
                "ticker": ticker,
                "is_trading_stopped": output.get("ovrs_stck_tr_stop_dvsn_cd", "01") in trading_stop_codes,
                "is_delisted": output.get("lstg_abol_item_yn", "N") == "Y",
                "name": output.get("prdt_name", ""),
            }

        except Exception as e:
            print(f"Error fetching status for overseas stock {ticker}: {str(e)}")
            return {"ticker": ticker, "error": str(e), "is_trading_stopped": False, "is_delisted": False}
