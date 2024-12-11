from fastapi import APIRouter
from app.modules.financial.router import router as financial_router
from app.modules.price.router import router as price_router
from app.modules.stock_indices.router import router as stock_indices_router
from app.modules.news.router import router as news_router

api_router = APIRouter()

api_router.include_router(financial_router, prefix="/financial", tags=["financial"])
api_router.include_router(price_router, prefix="/price", tags=["price"])
api_router.include_router(stock_indices_router, prefix="/stock-indices", tags=["stock-indices"])
api_router.include_router(news_router, prefix="/news", tags=["news"])