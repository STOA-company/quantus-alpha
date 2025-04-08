from fastapi import APIRouter
from app.api.v1 import api_router
from app.api.v2 import api_router as api_router_v2
from app.core.config import settings
from app.monitoring.router import router as monitoring_router

router = APIRouter()
router.include_router(api_router, prefix=settings.API_V1_STR)
router.include_router(api_router_v2, prefix=settings.API_V2_STR)
router.include_router(monitoring_router)
