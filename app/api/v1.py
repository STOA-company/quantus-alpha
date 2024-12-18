from fastapi import APIRouter
from app.modules.financial.router import router as financial_router
from app.modules.price.router import router as price_router
from app.modules.stock_indices.router import router as stock_indices_router
from app.modules.news.router import router as news_router
from app.modules.stock_info.router import router as stock_info_router
from app.modules.disclosure.router import router as disclosure_router
from app.modules.dividend.router import router as dividend_router

api_router = APIRouter()

api_router.include_router(financial_router, prefix="/financial", tags=["financial"])
api_router.include_router(price_router, prefix="/price", tags=["price"])
api_router.include_router(stock_indices_router, prefix="/stock-indices", tags=["stock-indices"])
api_router.include_router(news_router, prefix="/news", tags=["news"])
api_router.include_router(stock_info_router, prefix="/info", tags=["stock-info"])
api_router.include_router(disclosure_router, prefix="/disclosure", tags=["disclosure"])
api_router.include_router(dividend_router, prefix="/dividend", tags=["dividend"])
