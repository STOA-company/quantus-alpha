import random

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import setup_logger
from app.database.conn import db
from app.modules.common.enum import FearAndGreedIndex, TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.common.utils import check_ticker_country_len_2
from app.modules.news.old_services import NewsService, get_news_service
from app.modules.news.schemas import LatestNewsResponse
from app.modules.price.services import PriceService, get_price_service
from app.modules.stock_info.schemas import FearGreedIndexItem, FearGreedIndexResponse, Indicators
from app.utils.krx import create_etf_integrated_info

from .services import StockInfoService, get_stock_info_service

router = APIRouter()
logger = setup_logger(__name__)


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


@router.get("/combined_old", summary="종목 정보, 지표, 기업 정보 전체 조회")
async def get_combined(
    ticker: str,
    lang: TranslateCountry = TranslateCountry.KO,
    stock_service: StockInfoService = Depends(get_stock_info_service),
    summary_service: PriceService = Depends(get_price_service),
    news_service: NewsService = Depends(get_news_service),
    price_service: PriceService = Depends(get_price_service),
):
    type = await stock_service.get_type(ticker)
    ctry = await stock_service.get_ctry_by_ticker(ticker)
    if ctry is None:
        ctry = check_ticker_country_len_2(ticker)
    logger.info(f"Processing combined data for {ticker} ({ctry})")

    try:
        stock_info = None
        etf_info = None
        if type == "stock":
            stock_info = await stock_service.get_stock_info(ctry, ticker, lang)
            logger.info("Successfully fetched stock_info")

        if type == "etf":
            if ctry == "us":
                etf_info = await stock_service.get_us_etf_info(ticker)
            else:
                etf_info = await stock_service.get_etf_info(ticker)
            logger.info("Successfully fetched etf_info")

    except Exception as e:
        logger.error(f"Error fetching stock_info: {e}")
        stock_info = None

    try:
        indicators = None
        if type == "stock":
            indicators = await stock_service.get_indicators(ctry, ticker)
            logger.info("Successfully fetched indicators")

    except Exception as e:
        logger.error(f"Error fetching indicators: {e}")
        indicators = None

    try:
        summary = await summary_service.get_price_data_summary(ctry, type, ticker, lang)
        logger.info("Successfully fetched summary")
    except Exception as e:
        logger.error(f"Error fetching summary: {e}")
        summary = None

    try:
        if type == "stock":
            latest = news_service.get_latest_news(ticker=ticker, lang=lang)
        elif type == "etf":
            latest = news_service.get_etf_latest_news(ticker=ticker, lang=lang)
        logger.info("Successfully fetched latest news")
    except Exception as e:
        logger.error(f"Error fetching latest news: {e}")
        latest = LatestNewsResponse(date="2000-01-01 00:00:00", content="", type="")

    try:
        price = await price_service.get_real_time_price_data(ticker)
        logger.info("Successfully fetched price")
    except Exception as e:
        logger.error(f"Error fetching price: {e}")
        price = None

    if type == "stock":
        data = {
            "summary": summary,
            "indicators": indicators,
            "stock_info": stock_info,
            "latest": latest,
            "price": price,
        }
    elif type == "etf":
        data = {
            "summary": summary,
            "etf_info": etf_info,
            "latest": latest,
            "price": price,
        }

    # 모든 데이터가 None인 경우에만 404 반환
    if all(v is None for v in data.values()):
        raise HTTPException(status_code=404, detail="No data found for the given ticker")

    stock_service.increment_search_score(ticker)
    return BaseResponse(
        type=type, status_code=200, message="종목 정보, 지표, 기업 정보를 성공적으로 조회했습니다.", data=data
    )

########################################################################################################################################
@router.get("/combined", summary="종목 정보, 지표, 기업 정보 전체 조회")
async def get_combined_new(
    ticker: str,
    lang: TranslateCountry = TranslateCountry.KO,
    stock_service: StockInfoService = Depends(get_stock_info_service),
    summary_service: PriceService = Depends(get_price_service),
    news_service: NewsService = Depends(get_news_service),
    price_service: PriceService = Depends(get_price_service),
):
    # 변수 초기화
    stock_info = None
    etf_info = None
    indicators = None
    summary = None
    latest = LatestNewsResponse(date="2000-01-01 00:00:00", content="", type="")
    price = None
    data = {}

    try: 
        stock_info_db = await stock_service.get_stock_info_db(ticker)
        type, ctry = stock_info_db.type, stock_info_db.ctry
        stock_factors = await stock_service.get_stock_factors_db(ctry, ticker)
        
        logger.info(f"Processing combined data for {ticker} ({ctry})")

        try:
            if type == "stock":
                stock_info = await stock_service.get_stock_info_v2(ctry, ticker, lang, stock_info_db)
                logger.info("Successfully fetched stock_info")

            if type == "etf":
                if ctry == "us":
                    etf_info = await stock_service.get_us_etf_info(ticker)
                else:
                    etf_info = await stock_service.get_etf_info(ticker)
                logger.info("Successfully fetched etf_info")

            if type == "stock":
                indicators = await stock_service.get_indicators_v2(ctry, ticker, stock_factors)
                logger.info("Successfully fetched indicators")

            summary = await summary_service.get_price_data_summary_v2(ctry, type, ticker, lang, stock_factors, stock_info_db)
            logger.info("Successfully fetched summary")
        except Exception as e:
            logger.error(f"Error fetching stock data: {e}")

        try:
            if type == "stock":
                latest = await news_service.get_latest_news_v2(ticker=ticker, lang=lang)
            elif type == "etf":
                latest = await news_service.get_etf_latest_news(ticker=ticker, lang=lang)
            logger.info("Successfully fetched latest news")
        except Exception as e:
            logger.error(f"Error fetching latest news: {e}")

        try:
            price = await price_service.get_real_time_price_data(ticker)
            logger.info("Successfully fetched price")
        except Exception as e:
            logger.error(f"Error fetching price: {e}")

        if type == "stock":
            data = {
                "summary": summary,
                "indicators": indicators,
                "stock_info": stock_info,
                "latest": latest,
                "price": price,
            }
        elif type == "etf":
            data = {
                "summary": summary,
                "etf_info": etf_info,
                "latest": latest,
                "price": price,
            }
    except Exception as e:
        logger.error(f"Error in combined_new endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    # 모든 데이터가 None인 경우에만 404 반환
    if all(v is None for v in data.values()):
        raise HTTPException(status_code=404, detail="No data found for the given ticker")

    stock_service.increment_search_score(ticker)
    return BaseResponse(
        type=type, status_code=200, message="종목 정보, 지표, 기업 정보를 성공적으로 조회했습니다.", data=data
    )
########################################################################################################################################

@router.get("/holdings", summary="ETF 종목 조회")
async def get_holdings(
    ticker: str,
    service: StockInfoService = Depends(get_stock_info_service),
):
    data = await service.get_etf_holdings(ticker)
    return BaseResponse(status_code=200, message="ETF 종목을 성공적으로 조회했습니다.", data=data)


@router.get("/similar", summary="연관 종목 조회")
async def get_similar_stocks(
    ticker: str,
    lang: TranslateCountry = TranslateCountry.KO,
    type: str = "stock",
    service: StockInfoService = Depends(get_stock_info_service),
):
    if type == "stock":
        data = await service.get_similar_stocks(ticker, lang)
    elif type == "etf":
        return BaseResponse(status_code=200, message="ETF 종목을 성공적으로 조회했습니다.", data=None)
    else:
        raise HTTPException(status_code=400, detail="Invalid type")

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


@router.get("/update_etf_parquet", summary="ETF 파일 업데이트")
async def update_etf_parquet():
    create_etf_integrated_info()
    return BaseResponse(status_code=200, message="ETF 파일을 성공적으로 업데이트했습니다.")



            