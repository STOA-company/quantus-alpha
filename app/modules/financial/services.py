import os
from fastapi import Depends
from app.modules.common.services import get_common_service, CommonService
from app.core.config import settings

class FinancialService:
    def __init__(self, common_service: CommonService = Depends(get_common_service)):
        self.common_service = common_service

    async def read_financial_data(self, data_type: str, ctry: str, ticker: str):
        file_path = os.path.join(settings.DATA_DIR, ctry, "financial_updated", data_type, f"{ticker}.parquet")
        return await self.common_service.read_local_file(file_path)

def get_financial_service(common_service: CommonService = Depends(get_common_service)):
    return FinancialService(common_service)