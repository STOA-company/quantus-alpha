import pandas as pd
import httpx
from fastapi import HTTPException

class CommonService:
    async def read_local_file(self, file_path):
        try:
            df = pd.read_parquet(file_path)
            return df
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    async def make_external_api_call(self, url: str, method: str = "GET", **kwargs):
        async with httpx.AsyncClient() as client:
            response = await client.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()

def get_common_service():
    return CommonService()