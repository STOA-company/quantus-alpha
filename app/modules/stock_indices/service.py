import yfinance as yf
import pytz


class StockIndicesService:
    def __init__(self):
        self.indices = {"KOSPI": "^KS11", "KOSDAQ": "^KQ11"}
        self.korea_tz = pytz.timezone("Asia/Seoul")

    async def get_indices_data(self):
        result = {}

        for index_name, ticker in self.indices.items():
            try:
                time_series = await self._get_index_data(ticker)
                if time_series:
                    result[index_name] = time_series

            except Exception as e:
                print(f"Error fetching {index_name}: {str(e)}")
                continue

        return result

    async def _get_index_data(self, ticker):
        # 티커 객체 생성
        index = yf.Ticker(ticker)

        # 오늘 데이터 가져오기 (5분 간격)
        df = index.history(period="1d", interval="5m")

        if len(df) == 0:
            return None

        # 전일 데이터 가져오기 (등락률 계산용)
        prev_day = index.history(period="2d", interval="1d")
        prev_close = float(prev_day["Close"].iloc[-2]) if len(prev_day) > 1 else float(df["Close"].iloc[0])

        # 시간별 데이터 생성
        time_series = []
        for i in range(len(df)):
            # 한국 시간으로 변환
            kr_time = df.index[i].tz_convert("Asia/Seoul")

            # 장 시간 내의 데이터만 포함 (9:00 ~ 15:30)
            if 9 <= kr_time.hour <= 15:
                if kr_time.hour == 15 and kr_time.minute > 30:
                    continue

                current_value = round(float(df["Close"].iloc[i]), 2)
                current_change = round(((current_value - prev_close) / prev_close * 100), 2)

                time_series.append({"time": kr_time.strftime("%H:%M"), "value": current_value, "change": current_change})

        return time_series

    async def get_indices_data_fifteen(self):
        result = {}

        for index_name, ticker in self.indices.items():
            try:
                time_series = await self._get_index_data_fifteen(ticker)
                if time_series:
                    result[index_name] = time_series

            except Exception as e:
                print(f"Error fetching {index_name}: {str(e)}")
                continue

        return result

    async def _get_index_data_fifteen(self, ticker):
        # 티커 객체 생성
        index = yf.Ticker(ticker)

        # 오늘 데이터 가져오기 (15분 간격)
        df = index.history(period="1d", interval="15m")

        if len(df) == 0:
            return None

        # 전일 데이터 가져오기 (등락률 계산용)
        prev_day = index.history(period="2d", interval="1d")
        prev_close = float(prev_day["Close"].iloc[-2]) if len(prev_day) > 1 else float(df["Close"].iloc[0])

        # 시간별 데이터 생성
        time_series = []
        for i in range(len(df)):
            # 한국 시간으로 변환
            kr_time = df.index[i].tz_convert("Asia/Seoul")

            # 장 시간 내의 데이터만 포함 (9:00 ~ 15:30)
            if 9 <= kr_time.hour <= 15:
                if kr_time.hour == 15 and kr_time.minute > 30:
                    continue

                current_value = round(float(df["Close"].iloc[i]), 2)
                current_change = round(((current_value - prev_close) / prev_close * 100), 2)

                time_series.append({"time": kr_time.strftime("%H:%M"), "value": current_value, "change": current_change})

        return time_series
