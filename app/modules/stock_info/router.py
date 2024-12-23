import random
from fastapi import APIRouter, Depends

from app.modules.common.enum import FearAndGreedIndex
from app.modules.common.schemas import BaseResponse
from app.modules.common.utils import check_ticker_country_len_2
from app.modules.news.services import NewsService, get_news_service
from app.modules.price.services import PriceService, get_price_service
from app.modules.stock_info.schemas import FearGreedIndexItem, FearGreedIndexResponse, Indicators
from .services import StockInfoService, get_stock_info_service
from app.database.conn import db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


# @router.get("", response_model=BaseResponse[StockInfo], summary="주식 정보 조회")
# async def get_stock_info(
#     ctry: Country,
#     ticker: str,
#     service: StockInfoService = Depends(get_stock_info_service),
#     db: AsyncSession = Depends(db.get_async_db),
# ):
#     data = await service.get_stock_info(ctry, ticker, db)
#     return BaseResponse(status_code=200, message="주식 정보를 성공적으로 조회했습니다.", data=data)


@router.get("/indicators", response_model=BaseResponse[Indicators], summary="지표 조회")
async def get_indicators(
    ticker: str,
    service: StockInfoService = Depends(get_stock_info_service),
    db: AsyncSession = Depends(db.get_async_db),
):
    ctry = check_ticker_country_len_2(ticker)
    data = await service.get_indicators(ctry, ticker)
    return BaseResponse(status_code=200, message="지표 정보를 성공적으로 조회했습니다.", data=data)


@router.get("/combined", summary="종목 정보, 지표, 기업 정보 전체 조회")
async def get_combined(
    ticker: str,
    stock_service: StockInfoService = Depends(get_stock_info_service),
    summary_service: PriceService = Depends(get_price_service),
    news_service: NewsService = Depends(get_news_service),
    price_service: PriceService = Depends(get_price_service),
    db: AsyncSession = Depends(db.get_async_db),
):
    ctry = check_ticker_country_len_2(ticker)

    stock_info = await stock_service.get_stock_info(ctry, ticker, db)
    indicators = await stock_service.get_indicators(ctry, ticker)
    summary = await summary_service.get_price_data_summary(ctry, ticker, db)
    latest = news_service.get_latest_news(ticker=ticker)
    price = await price_service.get_real_time_price_data(ticker)

    data = {
        "summary": summary,
        "indicators": indicators,
        "stock_info": stock_info,
        "latest": latest,
        "price": price,
    }

    return BaseResponse(status_code=200, message="종목 정보, 지표, 기업 정보를 성공적으로 조회했습니다.", data=data)


@router.get("/similar", summary="연관 종목 조회")
async def get_similar_stocks(
    ticker: str,
    service: StockInfoService = Depends(get_stock_info_service),
    db: AsyncSession = Depends(db.get_async_db),
):
    ctry = check_ticker_country_len_2(ticker)
    data = await service.get_similar_stocks(ctry, ticker, db)
    return BaseResponse(status_code=200, message="연관 종목을 성공적으로 조회했습니다.", data=data)


# 공포와 탐욕 지수
@router.get(
    "/fear_greed_index", summary="공포와 탐욕 지수 조회 // (mock)", response_model=BaseResponse[FearGreedIndexResponse]
)
async def get_fear_greed_index(
    service: StockInfoService = Depends(get_stock_info_service),
):
    data = FearGreedIndexResponse(
        kor_stock=FearGreedIndexItem(
            fear_greed_index=random.randint(0, 100),
            last_close=random.choice(list(FearAndGreedIndex)).name,
            last_week=random.choice(list(FearAndGreedIndex)).name,
            last_month=random.choice(list(FearAndGreedIndex)).name,
            last_year=random.choice(list(FearAndGreedIndex)).name,
        ),
        us_stock=FearGreedIndexItem(
            fear_greed_index=random.randint(0, 100),
            last_close=random.choice(list(FearAndGreedIndex)).name,
            last_week=random.choice(list(FearAndGreedIndex)).name,
            last_month=random.choice(list(FearAndGreedIndex)).name,
            last_year=random.choice(list(FearAndGreedIndex)).name,
        ),
    )

    return BaseResponse(status_code=200, message="공포와 탐욕 지수를 성공적으로 조회했습니다.", data=data)
