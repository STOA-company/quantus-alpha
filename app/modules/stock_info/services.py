import pandas as pd
from app.core.exception.custom import DataNotFoundException
from app.modules.common.enum import Country
from app.modules.stock_info.schemas import StockInfo
from app.core.logging.config import get_logger

logger = get_logger(__name__)

class StockInfoService:
    def __init__(self):
        self.file_path = "static"
        self.file_name = "stock_{}_info.csv"
        

    async def get_stock_info(self, ticker: str, ctry: Country) -> StockInfo:
        """
        주식 정보 조회
        """
        if ctry != Country.US:
            raise DataNotFoundException(ticker=ctry.name, data_type="stock_info")
        
        file_name = self.file_name.format(ctry.name)
        FILE_PATH = f"{self.file_path}/{file_name}"
        df = pd.read_csv(FILE_PATH)
        result = df.loc[df['ticker'] == ticker].to_dict(orient='records')[0]
        if result is None:
            raise DataNotFoundException(ticker=ticker, data_type="stock_info")
        
        result = StockInfo(
            homepage_url=result['URL'],
            ceo_name=result['LastName'] + result['FirstName'],
            establishment_date=result['IncInDt'],
            listing_date=result['oldest_date']
        )
        
        return result

def get_stock_info_service() -> StockInfoService:
    return StockInfoService()