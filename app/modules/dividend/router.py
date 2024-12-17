from fastapi import APIRouter

from app.modules.common.schemas import BaseResponse
from app.modules.dividend.schemas import DividendItem


router = APIRouter()


@router.get("/", response_model=BaseResponse[DividendItem])
async def get_dividend():
    return {"message": "Hello, World!"}
