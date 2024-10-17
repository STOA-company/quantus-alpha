import os
from fastapi import Depends, HTTPException
from app.modules.common.services import get_common_service, CommonService
from app.core.config import settings
import pandas as pd
from datetime import date
from typing import Optional

class PriceService:
    def __init__(self, common_service: CommonService = Depends(get_common_service)):
        self.common_service = common_service

    async def read_price_data(self, ctry: str, ticker: str, start_date: Optional[date] = None, end_date: Optional[date] = None):
        file_path = os.path.join(settings.DATA_DIR, ctry, "price.parquet")
        df = await self.common_service.read_local_file(file_path)
        filtered_df = df[df['Code'] == ticker]
        if filtered_df.empty:
            raise HTTPException(status_code=404, detail=f"No price data found for ticker {ticker}")
        if start_date:
            filtered_df = filtered_df[filtered_df['period'] >= pd.Timestamp(start_date)]
        if end_date:
            filtered_df = filtered_df[filtered_df['period'] <= pd.Timestamp(end_date)]        
        return {"data": filtered_df.to_dict(orient="records")}

def get_price_service(common_service: CommonService = Depends(get_common_service)):
    return PriceService(common_service)