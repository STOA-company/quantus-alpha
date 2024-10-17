from fastapi import APIRouter
from app.modules.financial.router import router as financial_router
from app.modules.price.router import router as price_router

api_router = APIRouter()

api_router.include_router(financial_router, prefix="/financial", tags=["financial"])
api_router.include_router(price_router, prefix="/price", tags=["price"])