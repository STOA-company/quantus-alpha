import logging
import requests
from datetime import datetime, timedelta
from kispy.base import BaseAPI
from app.core.config import settings
import pytz

logger = logging.getLogger(__name__)


class KISAPI(BaseAPI):
    INDEX_MARKET_CODES = {
        "KOSPI": "0001",
        "KOSDAQ": "1001",
        "NASDAQ": "COMP",
        "SP500": "SPX",
    }

    def __init__(self, auth):
        """Initialize the Korean Stock API client"""
        self.app_key = settings.KIS_APP_KEY
        self.app_secret = settings.KIS_SECRET
        self.base_url = "https://openapi.koreainvestment.com:9443"
        self.access_token = self._get_access_token()
        super().__init__(auth=auth)

    def _get_access_token(self) -> str:
        """접근 토큰 발급"""
        url = f"{self.base_url}/oauth2/tokenP"

        data = {"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret}

        response = requests.post(url, json=data)
        response_data = response.json()

        return response_data.get("access_token")

    def refresh_token(self) -> bool:
        """토큰 갱신 메서드"""
        try:
            new_token = self._get_access_token()
            if new_token:
                self.access_token = new_token
                logger.info("Successfully refreshed access token")
                return True
            return False
        except Exception as e:
            logger.error(f"Error refreshing token: {str(e)}")
            return False

    def is_token_valid(self) -> bool:
        """토큰 유효성 검사"""
        if not self.access_token:
            return False

        try:
            # 테스트 API 호출
            test_url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {self.access_token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": "FHKST01010100",
            }

            response = requests.get(test_url, headers=headers)

            # 토큰 만료
            if response.status_code == 403:
                return False

            return True

        except Exception as e:
            logger.error(f"Error checking token validity: {str(e)}")
            return False

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

    def get_domestic_index_minute(self, period: str, market: str):
        try:
            endpoint = "/uapi/domestic-stock/v1/quotations/inquire-index-timeprice"
            url = f"{self.base_url}{endpoint}"

            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {self.access_token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": "FHPUP02110200",
                "custtype": "P",
            }

            if period[-1] == "s":
                time_period = int(period[:-1])
            elif period[-1] == "m":
                time_period = int(period[:-1]) * 60
            elif period[-1] == "h":
                time_period = int(period[:-1]) * 60 * 60
            else:
                raise ValueError(f"지원하지 않는 period: {period}")

            params = {
                "fid_input_hour_1": str(time_period),
                "fid_input_iscd": self.INDEX_MARKET_CODES[market],
                "fid_cond_mrkt_div_code": "U",
            }

            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                raise Exception(f"API request failed with status {response.status_code}")

            data = response.json()

            if data["rt_cd"] != "0":  # API 에러 체크
                raise Exception(f"API error: {data['msg1']}")

            return data["output"]

        except Exception as e:
            print(f"Error fetching stock index for {market}: {str(e)}")
            return {"error": str(e)}

    def get_global_index_minute(self, index_code: str, include_history: bool = True) -> dict:
        """
        해외 지수 분봉 데이터 조회

        Args:
            index_code (str): 지수 코드 (SPX: S&P500, COMP: 나스닥)
            include_history (bool): 과거 데이터 포함 여부

        Returns:
            dict: {
                'current_data': 현재가 정보,
                'minute_data': 분봉 데이터 DataFrame
            }
        """
        try:
            url = f"{self.base_url}/uapi/overseas-price/v1/quotations/inquire-time-indexchartprice"

            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {self.access_token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": "FHKST03030200",
                "custtype": "P",
            }

            params = {
                "FID_COND_MRKT_DIV_CODE": "N",  # 해외지수
                "FID_INPUT_ISCD": self.INDEX_MARKET_CODES[index_code],
                "FID_HOUR_CLS_CODE": "0",  # 정규장
                "FID_PW_DATA_INCU_YN": "Y" if include_history else "N",
            }

            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                raise Exception(f"API request failed with status {response.status_code}")

            data = response.json()

            if not data or "output1" not in data:
                raise Exception(f"Failed to fetch data for {index_code}")

            return data

        except Exception as e:
            logger.error(f"Error fetching global index data for {index_code}: {str(e)}")
            raise

    def get_domestic_index_1d(self, index_code: str, date: str, include_history: bool = True) -> dict:
        """
        국내 지수 일봉 데이터 조회

        Args:
            index_code (str): 지수 코드 (KOSPI, KOSDAQ, KOSPI200)
            include_history (bool): 과거 데이터 포함 여부

        Returns:
            dict: {
                'current_data': 현재가 정보,
                'day_data': 일봉 데이터 DataFrame
            }
        """
        try:
            url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-index-daily-price"
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {self.access_token}",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
                "tr_id": "FHPUP02120000",
                "custtype": "P",
            }

            params = {
                "fid_cond_mrkt_div_code": "U",
                "fid_input_iscd": self.INDEX_MARKET_CODES[index_code],
                "fid_input_date_1": date,
                "fid_period_div_code": "D",
            }
            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                raise Exception(f"API request failed with status {response.status_code}")

            data = response.json()

            if data["rt_cd"] != "0":
                raise Exception(f"API error: {data['msg1']}")

            return data
        except Exception as e:
            print(f"Error fetching domestic index data for {index_code}: {str(e)}")
            return {"error": str(e)}

    def get_stock_price_history_by_minute(
        self,
        symbol: str,
        time: str | None = None,
        limit: int | None = 30,
        desc: bool = False,
    ) -> list[dict]:
        """주식당일분봉조회[v1_국내주식-022]
        당일 분봉 데이터만 제공됩니다. (전일자 분봉 미제공)

        Args:
            symbol (str): 종목코드
            time (str | None): 조회 시작시간 (HHMMSS 형식, 예: "123000"은 12시 30분부터 조회) None인 경우 현재시각부터 조회
            limit (int): 조회 건수, 기본값 30건
            desc (bool): 시간 역순 정렬 여부, 기본값은 False (False: 과거순 정렬, True: 최신순 정렬)

        Returns:
            list[dict]: 주식 분봉 시세

        Note:
            - time에 미래 시각을 입력하면 현재 시각 기준으로 조회됩니다.
            - output2의 첫번째 배열의 체결량(cntg_vol)은 첫체결이 발생되기 전까지는 이전 분봉의 체결량이 표시됩니다.
            - 한 번의 API 호출로 최대 30건의 데이터를 가져올 수 있으며, 여러 번 호출하여 더 많은 데이터를 가져올 수 있습니다.
            - 개선 가능 사항 :
                - ETF, ETN의 분봉 데이터를 사용하여 국내 지수 분봉 데이터 추가 조회 가능
                - 섹터/업종별 지수 추가 조회 가능
        """
        path = "uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        url = f"{self._url}/{path}"

        headers = self._auth.get_header()
        headers["tr_id"] = "FHKST03010200"

        # 한국 시간대
        kr_tz = pytz.timezone("Asia/Seoul")
        now = datetime.now(kr_tz)

        if not time:
            time = now.strftime("%H%M%S")

        result: list[dict] = []
        today = now.strftime("%Y%m%d")
        current_time = time

        result: list[dict] = []

        while limit is None or len(result) < limit:
            params = {
                "FID_COND_MRKT_DIV_CODE": "J",  # 시장 분류 코드 (J : 주식)
                "FID_INPUT_ISCD": symbol,  # 종목코드
                "FID_INPUT_HOUR_1": current_time,  # 조회 시작 시간
                "FID_ETC_CLS_CODE": "",  # 종목 분류 코드 (기본값: 빈 문자열)
                "FID_PW_DATA_INCU_YN": "N",  # 데이터 포함 여부 (기본값: "N")
            }

            resp = self._request(method="get", url=url, headers=headers, params=params)

            records = list(resp.json["output2"])
            if not records:
                break

            for record in records:
                record["stck_cntg_hour"] = self._parse_date(f"{today}{record['stck_cntg_hour']}")

            if limit is not None:
                remaining = limit - len(result)
                records = records[:remaining]

            result.extend(records)

            if limit is not None and len(result) >= limit:
                break

            last_record = records[-1]
            if "stck_cntg_hour" not in last_record:
                break

            current_time = self._get_next_keyb_minute(records)

        if not desc:
            result.reverse()

        return result

    def _get_next_keyb_minute(self, records: list[dict], period: int = 1) -> str:
        last_record = records[-1]
        last_time: datetime = last_record["stck_cntg_hour"]
        next_time = last_time - timedelta(minutes=period)
        return next_time.strftime("%H%M%S")
