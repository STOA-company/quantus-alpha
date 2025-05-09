from fastapi import APIRouter

from app.modules.community.v2.router import router as community_router
from app.modules.disclosure.v2.router import router as disclosure_router
from app.modules.interest.v2.router import router as interest_router
from app.modules.news.v2.router import router as news_router
from app.modules.user.v2.router import router as user_router

api_router = APIRouter()

api_router.include_router(community_router, prefix="/community", tags=["community/v2"])
api_router.include_router(news_router, prefix="/news", tags=["news/v2"])
api_router.include_router(disclosure_router, prefix="/disclosure", tags=["disclosure/v2"])
api_router.include_router(interest_router, prefix="/interest", tags=["interest/v2"])
api_router.include_router(user_router, prefix="/user", tags=["user/v2"])
