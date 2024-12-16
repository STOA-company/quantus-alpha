from fastapi import APIRouter
from app.modules.price.router_v2 import router as price_router

api_router = APIRouter()

api_router.include_router(price_router, prefix="/price", tags=["price"])
