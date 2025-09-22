import asyncio
import random
import time

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import setup_logger
from app.database.conn import db
from app.modules.common.enum import FearAndGreedIndex, TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.common.utils import check_ticker_country_len_2
from app.modules.news.old_services import NewsService, get_news_service
from app.modules.news.v2.services import NewsService as NewsServiceV2, get_news_service as get_news_service_v2
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
    news_service: NewsServiceV2 = Depends(get_news_service_v2),
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
        # DB 접근 최소화: 종목 정보와 지표를 한 번에 조회
        stock_info_db, stock_factors = await stock_service.get_stock_info_with_factors_db(ticker)
        type, ctry = stock_info_db.type, stock_info_db.ctry
        
        logger.info(f"Processing combined data for {ticker} ({ctry})")

        # 병렬 처리: 모든 독립적인 데이터 조회를 동시에 실행
        async def fetch_stock_or_etf_info():
            start_time = time.time()
            try:
                if type == "stock":
                    result = await stock_service.get_stock_info_v2(ctry, ticker, lang, stock_info_db)
                elif type == "etf":
                    if ctry == "us":
                        result = await stock_service.get_us_etf_info(ticker)
                    else:
                        result = await stock_service.get_etf_info(ticker)
                else:
                    result = None
                
                elapsed_time = time.time() - start_time
                logger.info(f"fetch_stock_or_etf_info completed in {elapsed_time:.3f}s")
                return result
            except Exception as e:
                elapsed_time = time.time() - start_time
                logger.error(f"Error fetching {type}_info after {elapsed_time:.3f}s: {e}")
                return None

        async def fetch_indicators():
            start_time = time.time()
            try:
                if type == "stock":
                    result = await stock_service.get_indicators_v2(ctry, ticker, stock_factors)
                else:
                    result = None
                
                elapsed_time = time.time() - start_time
                logger.info(f"fetch_indicators completed in {elapsed_time:.3f}s")
                return result
            except Exception as e:
                elapsed_time = time.time() - start_time
                logger.error(f"Error fetching indicators after {elapsed_time:.3f}s: {e}")
                return None

        async def fetch_summary():
            start_time = time.time()
            try:
                result = await summary_service.get_price_data_summary_v2(ctry, type, ticker, lang, stock_factors, stock_info_db)
                elapsed_time = time.time() - start_time
                logger.info(f"fetch_summary completed in {elapsed_time:.3f}s")
                return result
            except Exception as e:
                elapsed_time = time.time() - start_time
                logger.error(f"Error fetching summary after {elapsed_time:.3f}s: {e}")
                return None

        async def fetch_latest_news():
            start_time = time.time()
            try:
                if type == "stock":
                    result = await news_service.get_latest_news_v2(ticker=ticker, lang=lang)
                # result = LatestNewsResponse(date="2000-01-01 00:00:00", content="", type="")
                elapsed_time = time.time() - start_time
                logger.info(f"fetch_latest_news completed in {elapsed_time:.3f}s")
                return result
            except Exception as e:
                elapsed_time = time.time() - start_time
                logger.error(f"Error fetching latest news after {elapsed_time:.3f}s: {e}")
                return LatestNewsResponse(date="2000-01-01 00:00:00", content="", type="")

        async def fetch_price():
            start_time = time.time()
            try:
                result = await price_service.get_real_time_price_data(ticker)
                final_result = result if hasattr(result, 'data') else None
                elapsed_time = time.time() - start_time
                logger.info(f"fetch_price completed in {elapsed_time:.3f}s")
                return final_result
            except Exception as e:
                elapsed_time = time.time() - start_time
                logger.error(f"Error fetching price after {elapsed_time:.3f}s: {e}")
                return None

        # 모든 태스크를 병렬로 실행
        logger.info(f"Starting parallel execution for {ticker}")
        parallel_start_time = time.time()
        
        # 각 태스크의 시작 시간을 개별적으로 기록
        task_start_times = {}
        
        async def timed_fetch_stock_or_etf_info():
            task_start_times['stock_or_etf'] = time.time()
            return await fetch_stock_or_etf_info()
            
        async def timed_fetch_indicators():
            task_start_times['indicators'] = time.time()
            return await fetch_indicators()
            
        async def timed_fetch_summary():
            task_start_times['summary'] = time.time()
            return await fetch_summary()
            
        async def timed_fetch_latest_news():
            task_start_times['latest_news'] = time.time()
            return await fetch_latest_news()
            
        async def timed_fetch_price():
            task_start_times['price'] = time.time()
            return await fetch_price()
        
        (stock_or_etf_result, indicators, summary, latest, price) = await asyncio.gather(
            timed_fetch_stock_or_etf_info(),
            timed_fetch_indicators(),
            timed_fetch_summary(),
            timed_fetch_latest_news(),
            timed_fetch_price(),
            return_exceptions=False  # 개별 함수에서 예외 처리하므로 False
        )
        
        parallel_elapsed_time = time.time() - parallel_start_time
        
        # 각 태스크의 실제 시작 시간 분석
        logger.info(f"Task start times analysis:")
        for task_name, start_time in task_start_times.items():
            delay = start_time - parallel_start_time
            logger.info(f"  {task_name}: started after {delay:.3f}s delay")
        
        logger.info(f"All parallel tasks completed in {parallel_elapsed_time:.3f}s")
        
        # 결과 할당
        if type == "stock":
            stock_info = stock_or_etf_result
        else:
            etf_info = stock_or_etf_result

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



            