from fastapi import APIRouter

from app.modules.community.v2.router import router as community_router

api_router = APIRouter()

api_router.include_router(community_router, prefix="/community", tags=["community/v2"])
