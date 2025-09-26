import math
from typing import Annotated, List

from app.modules import price
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.core.exception.base import DuplicateException, NotFoundException
from app.core.logger import setup_logger
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from app.modules.common.schemas import BaseResponse, NewsDisclosureResponse
from app.modules.interest.v2.request import (
    AddInterestRequest,
    DeleteInterestRequest,
    MoveInterestRequest,
    UpdateInterestOrderRequest,
    UpdateInterestRequest,
)
from app.modules.interest.v2.response import InterestGroupResponse, InterestPriceResponse
from app.modules.interest.v2.service import InterestService, get_interest_service
from app.modules.news.v2.schemas import InterestDisclosureResponse, InterestNewsResponse, TopStoriesResponse
from app.modules.news.v2.services import NewsService, get_news_service
from app.utils.quantus_auth_utils import get_current_user_redis as get_current_user

logger = setup_logger(__name__)

router = APIRouter()


# 관심 그룹 조회
@router.get("/groups", summary="관심 그룹 조회")
async def get_groups(
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    """
    사용자의 관심 그룹 목록을 조회하는 엔드포인트입니다.

    Response:
        [
            {
                "id": int,      # 관심 그룹의 고유 ID
                "name": str     # 관심 그룹의 이름 (예: "실시간 인기", "기본" 등)
            },
            ...
        ]

    - 사용자가 로그인하지 않은 경우 401 Unauthorized 에러가 발생합니다.
    - 사용자가 처음 접속하는 경우, 기본 그룹("실시간 인기")이 자동으로 생성됩니다.
    - 그룹은 order 필드에 따라 정렬되어 반환됩니다.
    """
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return await service.get_interest_group(current_user["uid"])


# 관심 그룹 조회
@router.get("/groups/{ticker}", summary="관심 그룹 조회")
async def get_groups_by_ticker(
    ticker: str,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return await service.get_interest_group_by_ticker(current_user["uid"], ticker)


# 관심 그룹 생성
@router.post("/groups", summary="관심 그룹 생성")
async def create_group(
    name: str,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    """
    새로운 관심 그룹을 생성하는 엔드포인트입니다.

    Args:
        name (str): 생성할 그룹의 이름

    Response:
        {
            "message": str,    # 성공 메시지
            "group_id": int    # 생성된 그룹의 ID
        }

    - 사용자가 로그인하지 않은 경우 401 Unauthorized 에러가 발생합니다.
    - 동일한 이름의 그룹이 이미 존재하는 경우 409 Conflict 에러가 발생합니다.
    """
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        group_id = await service.create_interest_group(current_user["uid"], name)
        return {"message": "관심 그룹이 생성되었습니다.", "group_id": group_id}
    except DuplicateException as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.detail)


# 관심 그룹 삭제
@router.delete("/groups", summary="관심 그룹 삭제")
async def delete_group(
    group_id: int,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    """
    기존 관심 그룹을 삭제하는 엔드포인트입니다.

    Args:
        group_id (int): 삭제할 그룹의 ID

    Response:
        {
            "message": str,    # 성공 메시지
            "group_id": int    # 삭제된 그룹의 ID
        }

    - 사용자가 로그인하지 않은 경우 401 Unauthorized 에러가 발생합니다.
    - 그룹이 존재하지 않는 경우 404 Not Found 에러가 발생합니다.
    - 수정 불가능한 그룹(예: "실시간 인기")은 삭제할 수 없습니다.
    """
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        await service.delete_interest_group(group_id, current_user["uid"])
        return {"message": "관심 그룹이 삭제되었습니다.", "group_id": group_id}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.detail)


# 관심 그룹 이름 수정
@router.put("/groups", summary="관심 그룹 이름 수정")
async def update_group_name(
    group_id: int,
    name: str,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    """
    관심 그룹의 이름을 수정하는 엔드포인트입니다.

    Args:
        group_id (int): 수정할 그룹의 ID
        name (str): 새로운 그룹 이름

    Response:
        {
            "message": str,    # 성공 메시지
            "group_id": int    # 수정된 그룹의 ID
        }

    - 사용자가 로그인하지 않은 경우 401 Unauthorized 에러가 발생합니다.
    - 그룹이 존재하지 않는 경우 404 Not Found 에러가 발생합니다.
    - 동일한 이름의 그룹이 이미 존재하는 경우 409 Conflict 에러가 발생합니다.
    - 수정 불가능한 그룹(예: "실시간 인기")은 이름을 변경할 수 없습니다.
    """
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        await service.update_interest_group_name(group_id, name, current_user["uid"])
        return {"message": f"관심 그룹 이름이 {name}으로 수정되었습니다.", "group_id": group_id}
    except NotFoundException as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except DuplicateException as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.detail)


########################################################
# 관심 종목 추가
@router.post("/", summary="관심 종목 추가")
async def add_interest(
    request: AddInterestRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        interest_id = await service.add_interest(request.group_id, request.ticker, current_user["uid"])
        return {"message": f"관심 종목에 {request.ticker}가 추가되었습니다.", "interest_id": interest_id}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))


# 관심 종목 삭제
@router.delete("/", summary="관심 종목 삭제")
async def delete_interest(
    request: DeleteInterestRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        await service.delete_interest(request.group_id, request.tickers, current_user["uid"])
        return {"message": f"관심 종목에서 {', '.join(request.tickers)}가 삭제되었습니다."}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))


# 관심 종목 수정
@router.put("/", summary="관심 종목 수정")
async def update_interest(
    request: UpdateInterestRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        await service.update_interest(user_id=current_user["uid"], group_ids=request.group_ids, ticker=request.ticker)
        return {"message": f"{request.ticker}가 {', '.join(map(str, request.group_ids))} 그룹에 추가되었습니다."}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))


########################################################
# 관심 그룹 / 종목 리스트
@router.get("/list", summary="관심 그룹 / 종목 리스트", response_model=BaseResponse[List[InterestGroupResponse]])
async def get_interest_list(
    lang: Annotated[
        TranslateCountry | None, Query(description="언어 코드, 예시: ko, en", optional=True)
    ] = TranslateCountry.KO,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        data = await service.get_interest_list(current_user["uid"], lang)
        return BaseResponse(status_code=200, message="Successfully retrieved interest list", data=data)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))


# 관심 그룹 / 종목 순서 변경
@router.put("/order", summary="그룹/종목 순서 변경")
async def update_order(
    request: UpdateInterestOrderRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        await service.update_order(user_id=current_user["uid"], group_id=request.group_id, order_list=request.order)

        message = "그룹 순서가 변경되었습니다." if request.group_id is None else "종목 순서가 변경되었습니다."
        return BaseResponse(status_code=200, message=message, data=None)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))


# 관심 종목 그룹 이동
@router.post("/move", summary="관심 종목 그룹 이동")
async def move_interest(
    request: MoveInterestRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

        await service.move_interest(request.from_group_id, request.to_group_id, request.tickers, current_user["uid"])
        return {"message": "관심 종목 그룹 이동이 완료되었습니다."}
    except HTTPException as e:
        raise e


########################################################
# 관심 종목 주요소식 모아보기 / 스토리
@router.get(
    "/stories/{group_id}/old",
    summary="관심 종목 주요소식 모아보기 / 스토리",
    response_model=BaseResponse[List[TopStoriesResponse]],
)
async def top_stories(
    group_id: int,
    request: Request,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en", optional=True)] = None,
    news_service: NewsService = Depends(get_news_service),
    service: InterestService = Depends(get_interest_service),
    user: AlphafinderUser = Depends(get_current_user),  # noqa
):
    tickers = await service.get_interest_tickers(group_id)
    if len(tickers) == 0:
        return BaseResponse(status_code=200, message="Successfully retrieved news data", data=[])
    # subscription_level = user.subscription_level if user else 1 # TODO :: 유저 테이블 통합 후 주석 해제
    # stories_count = 30 if subscription_level >= 3 else 10
    data = await news_service.top_stories(
        request=request, tickers=tickers, lang=lang, stories_count=30, user=user
    )  # TODO :: stories_count 변경 필요
    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=data)



# 관심 종목 가격 조회
@router.get("/{group_id}/price/old", summary="관심 종목 가격 조회", response_model=BaseResponse[List[InterestPriceResponse]])
async def get_interest_price(
    group_id: int,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = "ko",
    service: InterestService = Depends(get_interest_service),
):
    tickers = await service.get_interest_tickers(group_id)
    ticker_price_data = await service.get_interest_price(tickers=tickers, group_id=group_id, lang=lang)
    return BaseResponse(status_code=200, message="Successfully retrieved interest price data", data=ticker_price_data)


# 관심 종목 뉴스
## v1과 response가 다름!!!
@router.get("/news/{group_id}/old", summary="관심 종목 뉴스")
async def interest_news(
    group_id: int,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = "ko",
    page: Annotated[int, Query(description="페이지 번호, 기본값: 1")] = 1,
    size: Annotated[int, Query(description="페이지 사이즈, 기본값: 10")] = 10,
    news_service: NewsService = Depends(get_news_service),
    service: InterestService = Depends(get_interest_service),
    user: AlphafinderUser = Depends(get_current_user),  # noqa
):
    tickers = await service.get_interest_tickers(group_id)
    if len(tickers) == 0:
        return BaseResponse(
            status_code=200,
            message="Successfully retrieved news data",
            data=InterestNewsResponse(news=[], has_next=False),
        )
    total_news_data = await news_service.get_news(lang=lang, tickers=tickers)

    # if user.subscription_level < 3:
    #     total_news_data = news_service.mask_news_items(total_news_data)

    offset = (page - 1) * size
    news_data = total_news_data[offset : offset + size]

    # if user.subscription_level >= 3: # TODO :: 유저 테이블 통합 후 주석 해제
    has_next = len(total_news_data) > page * size
    # else:
    #     current_position = offset * limit + len(news_data)
    #     has_next = current_position < len(total_news_data)

    total_count = len(total_news_data)
    total_pages = math.ceil(total_count / size)
    current_page = page

    total_count = len(total_news_data)
    total_pages = math.ceil(total_count / size)
    current_page = page

    response_data = InterestNewsResponse(news=news_data, has_next=has_next)
    return NewsDisclosureResponse(
        status_code=200,
        message="Successfully retrieved news data",
        data=response_data,
        total_count=total_count,
        total_pages=total_pages,
        current_page=current_page,
        offset=offset,
        size=size,
    )


# 관심 종목 공시
## v1과 response가 다름!!!
@router.get("/disclosure/{group_id}/old", summary="관심 종목 공시")
async def interest_disclosure(
    group_id: int,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = "ko",
    page: Annotated[int, Query(description="페이지 번호, 기본값: 1")] = 1,
    size: Annotated[int, Query(description="페이지 사이즈, 기본값: 10")] = 10,
    news_service: NewsService = Depends(get_news_service),
    service: InterestService = Depends(get_interest_service),
    user: AlphafinderUser = Depends(get_current_user),  # noqa
):
    tickers = await service.get_interest_tickers(group_id)
    if len(tickers) == 0:
        return BaseResponse(
            status_code=200,
            message="Successfully retrieved disclosure data",
            data=InterestDisclosureResponse(disclosure=[], has_next=False),
        )
    total_disclosure_data = await news_service.get_disclosure(lang=lang, tickers=tickers)

    # 레벨 3 미만 사용자의 경우 데이터 마스킹 적용
    # if user.subscription_level < 3:
    #     total_disclosure_data = news_service.mask_disclosure_items(total_disclosure_data)

    offset = (page - 1) * size
    disclosure_data = total_disclosure_data[offset : offset + size]

    # if user.subscription_level >= 3:
    has_next = len(total_disclosure_data) > page * size
    # else:
    #     current_position = offset * limit + len(disclosure_data)
    #     has_next = current_position < len(total_disclosure_data)

    total_count = len(total_disclosure_data)
    total_pages = math.ceil(total_count / size)
    current_page = page

    total_count = len(total_disclosure_data)
    total_pages = math.ceil(total_count / size)
    current_page = page

    response_data = InterestDisclosureResponse(disclosure=disclosure_data, has_next=has_next)

    return NewsDisclosureResponse(
        status_code=200,
        message="Successfully retrieved disclosure data",
        data=response_data,
        total_count=total_count,
        total_pages=total_pages,
        current_page=current_page,
        offset=offset,
        size=size
    )

########################################################

@router.get("/mobile/{group_id}", summary="모바일용 스토리 조회", response_model=BaseResponse)
async def top_stories_mobile(
    group_id: int,
    request: Request,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en", optional=True)] = None,
    page: Annotated[int, Query(description="페이지 번호, 기본값: 1")] = 1,
    size: Annotated[int, Query(description="페이지 사이즈, 기본값: 10")] = 10,
    news_service: NewsService = Depends(get_news_service),
    interest_service: InterestService = Depends(get_interest_service),
    user: AlphafinderUser = Depends(get_current_user),  # noqa
):
    tickers = await interest_service.get_interest_tickers(group_id)
    
    if len(tickers) == 0:
        return BaseResponse(status_code=404, message="관심 종목이 없습니다", data=[])

    # 병렬로 데이터 가져오기
    interest_top_stories = await news_service.top_stories_elasticsearch(request=request, tickers=tickers, lang=lang, stories_count=30, user=user)
    interest_price_data = await interest_service.get_interest_price_elasticsearch(tickers=tickers, group_id=group_id, lang=lang)
    total_news_data = await news_service.get_news_elasticsearch(lang=lang, tickers=tickers)

    offset = (page - 1) * size
    news_data = total_news_data[offset : offset + size]

    # if user.subscription_level >= 3: # TODO :: 유저 테이블 통합 후 주석 해제
    has_next = len(total_news_data) > page * size
    # else:
    #     current_position = offset * limit + len(news_data)
    #     has_next = current_position < len(total_news_data)

    total_count = len(total_news_data)
    total_pages = math.ceil(total_count / size)
    current_page = page

    total_count = len(total_news_data)
    total_pages = math.ceil(total_count / size)
    current_page = page

    interest_news_data = InterestNewsResponse(news=news_data, has_next=has_next)

    # 모바일용 통합 응답 데이터 구성
    mobile_data = {
        "top_stories": interest_top_stories,  # TopStoriesResponse 리스트
        "price_data": interest_price_data,    # InterestPriceResponse 리스트  
        "news_data": NewsDisclosureResponse(
            status_code=200,
            message="Successfully retrieved news data",
            data=interest_news_data,
            total_count=total_count,
            total_pages=total_pages,
            current_page=current_page,
            offset=offset,
            size=size,
        )
    }

    return BaseResponse(
        status_code=200,
        message="모바일용 관심 종목 데이터를 성공적으로 조회했습니다",
        data=mobile_data
    )

# elasticsearch test
@router.get(
    "/stories/{group_id}",
    summary="관심 종목 주요소식 모아보기 / 스토리",
    response_model=BaseResponse[List[TopStoriesResponse]],
)
async def top_stories_elasticsearch(
    group_id: int,
    request: Request,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en", optional=True)] = None,
    news_service: NewsService = Depends(get_news_service),
    service: InterestService = Depends(get_interest_service),
    user: AlphafinderUser = Depends(get_current_user),  # noqa
):
    import time
    total_start_time = time.time()
    
    # logger.info(f"Starting top_stories_elasticsearch for group_id {group_id}")
    tickers_start_time = time.time()
    tickers = await service.get_interest_tickers(group_id)
    tickers_elapsed = time.time() - tickers_start_time
    
    if len(tickers) == 0:
        return BaseResponse(status_code=200, message="Successfully retrieved news data", data=[])
    
    # subscription_level = user.subscription_level if user else 1 # TODO :: 유저 테이블 통합 후 주석 해제
    # stories_count = 30 if subscription_level >= 3 else 10
    stories_start_time = time.time()
    data = await news_service.top_stories_elasticsearch(
        request=request, tickers=tickers, lang=lang, stories_count=30, user=user
    )  # TODO :: stories_count 변경 필요
    stories_elapsed = time.time() - stories_start_time
    
    total_elapsed = time.time() - total_start_time
    logger.info(f"[top_stories_elasticsearch router] Total: {total_elapsed:.3f}s | get_interest_tickers: {tickers_elapsed:.3f}s | top_stories_elasticsearch: {stories_elapsed:.3f}s")
    
    return BaseResponse(status_code=200, message="Successfully retrieved news data", data=data)

# 관심 종목 가격 조회
@router.get("/{group_id}/price", summary="관심 종목 가격 조회", response_model=BaseResponse[List[InterestPriceResponse]])
async def get_interest_price_elasticsearch(
    group_id: int,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = "ko",
    service: InterestService = Depends(get_interest_service),
):
    logger.info(f"Starting get_interest_price_elasticsearch for group_id {group_id}")
    tickers = await service.get_interest_tickers(group_id)
    logger.info(f"Retrieved tickers for group_id {group_id}: {tickers}")
    ticker_price_data = await service.get_interest_price_elasticsearch(tickers=tickers, group_id=group_id, lang=lang)
    logger.info(f"Completed get_interest_price_elasticsearch for group_id {group_id}, found {len(ticker_price_data)} results")
    return BaseResponse(status_code=200, message="Successfully retrieved interest price data", data=ticker_price_data)

@router.get("/news/{group_id}", summary="관심 종목 뉴스")
async def interest_news_elasticsearch(
    group_id: int,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = "ko",
    page: Annotated[int, Query(description="페이지 번호, 기본값: 1")] = 1,
    size: Annotated[int, Query(description="페이지 사이즈, 기본값: 10")] = 10,
    news_service: NewsService = Depends(get_news_service),
    service: InterestService = Depends(get_interest_service),
    user: AlphafinderUser = Depends(get_current_user),  # noqa
):
    logger.info(f"Starting interest_news_elasticsearch for group_id {group_id}")
    tickers = await service.get_interest_tickers(group_id)
    if len(tickers) == 0:
        return BaseResponse(
            status_code=200,
            message="Successfully retrieved news data",
            data=InterestNewsResponse(news=[], has_next=False),
        )
    total_news_data = await news_service.get_news_elasticsearch(lang=lang, tickers=tickers)

    # if user.subscription_level < 3:
    #     total_news_data = news_service.mask_news_items(total_news_data)

    offset = (page - 1) * size
    news_data = total_news_data[offset : offset + size]

    # if user.subscription_level >= 3: # TODO :: 유저 테이블 통합 후 주석 해제
    has_next = len(total_news_data) > page * size
    # else:
    #     current_position = offset * limit + len(news_data)
    #     has_next = current_position < len(total_news_data)

    total_count = len(total_news_data)
    total_pages = math.ceil(total_count / size)
    current_page = page

    total_count = len(total_news_data)
    total_pages = math.ceil(total_count / size)
    current_page = page

    response_data = InterestNewsResponse(news=news_data, has_next=has_next)
    return NewsDisclosureResponse(
        status_code=200,
        message="Successfully retrieved news data",
        data=response_data,
        total_count=total_count,
        total_pages=total_pages,
        current_page=current_page,
        offset=offset,
        size=size,
    )

@router.get("/disclosure/{group_id}", summary="관심 종목 공시")
async def interest_disclosure_elasticsearch(
    group_id: int,
    lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = "ko",
    page: Annotated[int, Query(description="페이지 번호, 기본값: 1")] = 1,
    size: Annotated[int, Query(description="페이지 사이즈, 기본값: 10")] = 10,
    news_service: NewsService = Depends(get_news_service),
    service: InterestService = Depends(get_interest_service),
    user: AlphafinderUser = Depends(get_current_user),  # noqa
):
    logger.info(f"Starting interest_disclosure_elasticsearch for group_id {group_id}")
    tickers = await service.get_interest_tickers(group_id)
    if len(tickers) == 0:
        return BaseResponse(
            status_code=200,
            message="Successfully retrieved disclosure data",
            data=InterestDisclosureResponse(disclosure=[], has_next=False),
        )
    total_disclosure_data = await news_service.get_disclosure_elasticsearch(lang=lang, tickers=tickers)

    # 레벨 3 미만 사용자의 경우 데이터 마스킹 적용
    # if user.subscription_level < 3:
    #     total_disclosure_data = news_service.mask_disclosure_items(total_disclosure_data)

    offset = (page - 1) * size
    disclosure_data = total_disclosure_data[offset : offset + size]

    # if user.subscription_level >= 3:
    has_next = len(total_disclosure_data) > page * size
    # else:
    #     current_position = offset * limit + len(disclosure_data)
    #     has_next = current_position < len(total_disclosure_data)

    total_count = len(total_disclosure_data)
    total_pages = math.ceil(total_count / size)
    current_page = page

    total_count = len(total_disclosure_data)
    total_pages = math.ceil(total_count / size)
    current_page = page

    response_data = InterestDisclosureResponse(disclosure=disclosure_data, has_next=has_next)

    return NewsDisclosureResponse(
        status_code=200,
        message="Successfully retrieved disclosure data",
        data=response_data,
        total_count=total_count,
        total_pages=total_pages,
        current_page=current_page,
        offset=offset,
        size=size
    )

########################################################