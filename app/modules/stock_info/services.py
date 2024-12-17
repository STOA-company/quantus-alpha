import pandas as pd
from app.database.crud import database
from app.core.exception.custom import DataNotFoundException
from app.modules.common.enum import Country
from app.modules.stock_info.schemas import Indicators, StockInfo
from app.core.logging.config import get_logger

logger = get_logger(__name__)


class StockInfoService:
    def __init__(self):
        self.db = database
        self.file_path = "static"
        self.file_name = "stock_{}_info.csv"

    async def get_stock_info(self, ctry: Country, ticker: str) -> StockInfo:
        """
        주식 정보 조회
        """
        if ctry != Country.US:
            raise DataNotFoundException(ticker=ctry.name, data_type="stock_info")

        file_name = self.file_name.format(ctry.name)
        FILE_PATH = f"{self.file_path}/{file_name}"
        df = pd.read_csv(FILE_PATH)
        result = df.loc[df["ticker"] == ticker].to_dict(orient="records")[0]
        if result is None:
            raise DataNotFoundException(ticker=ticker, data_type="stock_info")

        result = StockInfo(
            homepage_url=result["URL"],
            ceo_name=result["LastName"] + result["FirstName"],
            establishment_date=result["IncInDt"],
            listing_date=result["oldest_date"],
        )

        return result

    async def get_indicators(self, ctry: Country, ticker: str) -> Indicators:
        """
        기업 정보 조회 - 최신 데이터
        """
        try:
            # 실제 쿼리
            stock_data = self.db._select(table="stock_kr_1d", order="Date", ascending=False, limit=1, Ticker=ticker)

            financial_data = self.db._select(table="KOR_finpos", order="period_q", ascending=False, limit=1, Code=ticker)

            # 기본값 설정
            result = Indicators(per=None, pbr=None, roe=None)

            if stock_data and financial_data:
                # Row 객체의 _mapping 속성 사용
                stock_row = stock_data[0]._mapping
                fin_row = financial_data[0]._mapping

                stock_price = stock_row["Close"]
                retained_earnings = fin_row["retained_earnings"]
                total_equity = fin_row["total_equity"]

                print("=== DEBUG VALUES ===")
                print(f"Stock price: {format(stock_price, ',.2f')}")
                print(f"Retained earnings: {format(retained_earnings, ',.8f')}")
                print(f"Total equity: {format(total_equity, ',.2f')}")

                # 지표 계산
                per = round(stock_price / retained_earnings, 2) if retained_earnings and retained_earnings != 0 else None
                pbr = round(stock_price / total_equity, 2) if total_equity and total_equity != 0 else None

                # ROE 계산 과정 출력
                if total_equity and total_equity != 0:
                    roe_calc = (retained_earnings / total_equity) * 100
                    print(
                        f"ROE calculation: ({format(retained_earnings, ',.8f')} / {format(total_equity, ',.2f')}) * 100 = {format(roe_calc, ',.2f')}"
                    )
                    roe = round(roe_calc, 2)
                else:
                    roe = None

                result = Indicators(per=per, pbr=pbr, roe=roe)

            return result

        except Exception as e:
            print(f"Error: {str(e)}")
            raise e


def get_stock_info_service() -> StockInfoService:
    return StockInfoService()
