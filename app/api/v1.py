from fastapi import APIRouter
from app.modules.financial.router import router as financial_router
from app.modules.price.router import router as price_router
from app.modules.stock_indices.router import router as stock_indices_router
from app.modules.news.old_router import router as news_router
from app.modules.news.router import router as news_router_renewal
from app.modules.stock_info.router import router as stock_info_router
from app.modules.disclosure.router import router as disclosure_router
from app.modules.dividend.router import router as dividend_router
from app.modules.sector.router import router as sector_router
from app.modules.search.router import router as search_router
from app.modules.trending.router import router as trending_router
from app.modules.oauth.router import router as oauth_router
from app.modules.user.router import router as user_router
from app.modules.community.router import router as community_router
from app.modules.screener.stock.router import router as screener_router
from app.modules.screener.etf.router import router as screener_etf_router
from app.modules.payments.router import router as payments_router
from app.modules.interest.router import router as interest_router

api_router = APIRouter()

api_router.include_router(financial_router, prefix="/financial", tags=["financial"])
api_router.include_router(price_router, prefix="/price", tags=["price"])
api_router.include_router(stock_indices_router, prefix="/stock-indices", tags=["stock-indices"])
api_router.include_router(news_router, prefix="/news", tags=["news"])
api_router.include_router(news_router_renewal, prefix="/news", tags=["news"])
api_router.include_router(stock_info_router, prefix="/info", tags=["stock-info"])
api_router.include_router(disclosure_router, prefix="/disclosure", tags=["disclosure"])
api_router.include_router(dividend_router, prefix="/dividend", tags=["dividend"])
api_router.include_router(sector_router, prefix="/sector", tags=["sector"])
api_router.include_router(search_router, prefix="/search", tags=["search"])
api_router.include_router(trending_router, prefix="/trending", tags=["trending"])
api_router.include_router(oauth_router, prefix="/oauth", tags=["oauth"])
api_router.include_router(user_router, prefix="/user", tags=["user"])
api_router.include_router(community_router, prefix="/community", tags=["community"])
api_router.include_router(screener_router, prefix="/screener", tags=["screener"])
api_router.include_router(screener_etf_router, prefix="/screener/etf", tags=["screener-etf"])
api_router.include_router(payments_router, prefix="/payments", tags=["payments"])
api_router.include_router(interest_router, prefix="/interest", tags=["interest"])
