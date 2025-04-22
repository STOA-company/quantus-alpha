from fastapi import APIRouter

from app.modules.community.v2.router import router as community_router
from app.modules.disclosure.v2.router import router as disclosure_router
from app.modules.news.v2.router import router as news_router

api_router = APIRouter()

api_router.include_router(community_router, prefix="/community", tags=["community/v2"])
api_router.include_router(news_router, prefix="/news", tags=["news/v2"])
api_router.include_router(disclosure_router, prefix="/disclosure", tags=["disclosure/v2"])
