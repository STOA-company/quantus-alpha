from fastapi import APIRouter
from app.api.v1 import api_router
from app.core.config import settings

router = APIRouter()
router.include_router(api_router, prefix=settings.API_V1_STR)
