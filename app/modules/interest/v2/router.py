from typing import List

from fastapi import APIRouter, Depends, HTTPException

from app.core.exception.base import DuplicateException, NotFoundException
from app.core.logger import setup_logger
from app.models.models_users import AlphafinderUser
from app.modules.common.schemas import BaseResponse
from app.modules.interest.v2.request import AddInterestRequest, DeleteInterestRequest, UpdateInterestOrderRequest
from app.modules.interest.v2.response import InterestGroupResponse
from app.modules.interest.v2.service import InterestService, get_interest_service
from app.utils.quantus_auth_utils import get_current_user

logger = setup_logger(__name__)

router = APIRouter()


# 관심 그룹 조회
@router.get("/groups", description="관심 그룹 조회")
def get_groups(
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    print(current_user)
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return service.get_interest_group(current_user["uid"])


# 관심 그룹 생성
@router.post("/groups", description="관심 그룹 생성")
def create_group(
    name: str,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        group_id = service.create_interest_group(current_user["uid"], name)
        return {"message": "관심 그룹이 생성되었습니다.", "group_id": group_id}
    except DuplicateException as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.detail)


# 관심 그룹 삭제
@router.delete("/groups", description="관심 그룹 삭제")
def delete_group(
    group_id: int,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        service.delete_interest_group(group_id, current_user["uid"])
        return {"message": "관심 그룹이 삭제되었습니다.", "group_id": group_id}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=e.status_code, detail=e.detail)


# 관심 그룹 이름 수정
@router.put("/groups", description="관심 그룹 이름 수정")
def update_group_name(
    group_id: int,
    name: str,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")
        service.update_interest_group_name(group_id, name, current_user["uid"])
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
@router.post("/", description="관심 종목 추가")
def add_interest(
    request: AddInterestRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        interest_id = service.add_interest(request.group_id, request.ticker, current_user["uid"])
        return {"message": f"관심 종목에 {request.ticker}가 추가되었습니다.", "interest_id": interest_id}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))


# 관심 종목 삭제
@router.delete("/", description="관심 종목 삭제")
def delete_interest(
    request: DeleteInterestRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        service.delete_interest(request.group_id, request.tickers, current_user["uid"])
        return {"message": f"관심 종목에서 {', '.join(request.tickers)}가 삭제되었습니다."}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))


########################################################
# 관심 그룹 / 종목 리스트
@router.get("/list", description="관심 그룹 / 종목 리스트", response_model=BaseResponse[List[InterestGroupResponse]])
def get_interest_list(
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        data = service.get_interest_list(current_user["uid"])
        return BaseResponse(status_code=200, message="Successfully retrieved interest list", data=data)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))


# 관심 그룹 / 종목 순서 변경
@router.put("/order", description="그룹/종목 순서 변경")
def update_order(
    request: UpdateInterestOrderRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        service.update_order(user_id=current_user["uid"], group_id=request.group_id, order_list=request.order)

        message = "그룹 순서가 변경되었습니다." if request.group_id is None else "종목 순서가 변경되었습니다."
        return BaseResponse(status_code=200, message=message, data=None)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))


########################################################
# @router.get("/news-leaderboard/{group_id}")
# def get_news_leaderboard(
#     group_id: int,
#     lang: TranslateCountry = Query(default=TranslateCountry.KO, description="언어 코드, 예시: ko, en"),
#     service: InterestService = Depends(get_interest_service),
#     user: AlphafinderUser = Depends(get_current_user),
# ):
#     level = user.subscription_level if user else 1
#     data = service.get_interest_news_leaderboard(group_id, lang, level)
#     return BaseResponse(status_code=200, message="Successfully retrieved leaderboard data", data=data)


# @router.get("/disclosure-leaderboard/{group_id}")
# def get_disclosure_leaderboard(
#     group_id: int,
#     lang: TranslateCountry = Query(default=TranslateCountry.KO, description="언어 코드, 예시: ko, en"),
#     service: InterestService = Depends(get_interest_service),
#     user: AlphafinderUser = Depends(get_current_user),
# ):
#     level = user.subscription_level if user else 1
#     data = service.get_interest_disclosure_leaderboard(group_id, lang, level)
#     return BaseResponse(status_code=200, message="Successfully retrieved leaderboard data", data=data)


# @router.post("/update")
# def update_interest(
#     request: UpdateInterestRequest,
#     current_user: AlphafinderUser = Depends(get_current_user),
#     service: InterestService = Depends(get_interest_service),
# ):
#     if not current_user:
#         raise HTTPException(status_code=401, detail="Unauthorized")
#     service.update_interest(current_user.id, request.group_ids, request.ticker)
#     return {"message": f"종목 : {request.ticker}, 그룹 : {', '.join(map(str, request.group_ids))} 수정되었습니다."}


# @router.get("/columns")
# def get_columns(lang: Literal["ko", "en"] = "ko"):
#     columns = ["티커", "종목명", "현재가", "등락율", "거래대금", "거래량"]
#     if lang == "en":
#         columns = ["Ticker", "Name", "Price", "Change", "Amount", "Volume"]
#     return columns


# @router.get("/news/{group_id}", response_model=BaseResponse[InterestNewsResponse])
# def interest_news(
#     group_id: int,
#     lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = "ko",
#     offset: Annotated[int, Query(description="페이지 번호, 기본값: 0")] = 0,
#     limit: Annotated[int, Query(description="페이지 사이즈, 기본값: 10")] = 20,
#     news_service: NewsService = Depends(get_news_service),
#     service: InterestService = Depends(get_interest_service),
#     user: AlphafinderUser = Depends(get_current_user),
# ):
#     ticker_infos = service.get_interest_tickers(group_id)
#     if len(ticker_infos) == 0:
#         return BaseResponse(
#             status_code=200,
#             message="Successfully retrieved news data",
#             data=InterestNewsResponse(news=[], has_next=False),
#         )
#     tickers = [ticker_info["ticker"] for ticker_info in ticker_infos]
#     total_news_data = news_service.get_news(lang=lang, tickers=tickers)

#     if user.subscription_level < 3:
#         total_news_data = news_service.mask_news_items(total_news_data)

#     news_data = total_news_data[offset * limit : offset * limit + limit]

#     if user.subscription_level >= 3:
#         has_next = len(total_news_data) > offset * limit + limit
#     else:
#         current_position = offset * limit + len(news_data)
#         has_next = current_position < len(total_news_data)

#     response_data = InterestNewsResponse(news=news_data, has_next=has_next)
#     return BaseResponse(
#         status_code=200,
#         message="Successfully retrieved news data",
#         data=response_data,
#     )


# @router.get("/disclosure/{group_id}", response_model=BaseResponse[InterestDisclosureResponse])
# def interest_disclosure(
#     group_id: int,
#     lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en")] = "ko",
#     offset: Annotated[int, Query(description="페이지 번호, 기본값: 0")] = 0,
#     limit: Annotated[int, Query(description="페이지 사이즈, 기본값: 10")] = 20,
#     news_service: NewsService = Depends(get_news_service),
#     service: InterestService = Depends(get_interest_service),
#     user: AlphafinderUser = Depends(get_current_user),
# ):
#     ticker_infos = service.get_interest_tickers(group_id)
#     if len(ticker_infos) == 0:
#         return BaseResponse(
#             status_code=200,
#             message="Successfully retrieved news data",
#             data=InterestDisclosureResponse(disclosure=[], has_next=False),
#         )
#     tickers = [ticker_info["ticker"] for ticker_info in ticker_infos]
#     total_disclosure_data = news_service.get_disclosure(lang=lang, tickers=tickers)

#     # 레벨 3 미만 사용자의 경우 데이터 마스킹 적용
#     if user.subscription_level < 3:
#         total_disclosure_data = news_service.mask_disclosure_items(total_disclosure_data)

#     disclosure_data = total_disclosure_data[offset * limit : offset * limit + limit]

#     if user.subscription_level >= 3:
#         has_next = len(total_disclosure_data) > offset * limit + limit
#     else:
#         current_position = offset * limit + len(disclosure_data)
#         has_next = current_position < len(total_disclosure_data)

#     response_data = InterestDisclosureResponse(disclosure=disclosure_data, has_next=has_next)

#     return BaseResponse(
#         status_code=200,
#         message="Successfully retrieved disclosure data",
#         data=response_data,
#     )


# @router.get("/stories/{group_id}", response_model=BaseResponse[List[TopStoriesResponse]])
# def top_stories(
#     group_id: int,
#     request: Request,
#     lang: Annotated[TranslateCountry | None, Query(description="언어 코드, 예시: ko, en", optional=True)] = None,
#     news_service: NewsService = Depends(get_news_service),
#     service: InterestService = Depends(get_interest_service),
#     user: AlphafinderUser = Depends(get_current_user),
# ):
#     ticker_infos = service.get_interest_tickers(group_id)
#     if len(ticker_infos) == 0:
#         return BaseResponse(status_code=200, message="Successfully retrieved news data", data=[])
#     tickers = [ticker_info["ticker"] for ticker_info in ticker_infos]
#     subscription_level = user.subscription_level if user else 1
#     stories_count = 30 if subscription_level >= 3 else 10
#     data = news_service.top_stories(request=request, tickers=tickers, lang=lang, stories_count=stories_count)
#     return BaseResponse(status_code=200, message="Successfully retrieved news data", data=data)


# @router.get("/{group_id}/tickers")
# def get_interest_tickers(
#     group_id: int,
#     service: InterestService = Depends(get_interest_service),
# ):
#     ticker_infos = service.get_interest_tickers(group_id)
#     return [
#         {"ticker": ticker_info["ticker"], "name": ticker_info["name"], "country": ticker_info["country"]}
#         for ticker_info in ticker_infos
#     ]


# @router.get("/{group_id}/count")
# def get_interest_count(
#     group_id: int,
#     service: InterestService = Depends(get_interest_service),
# ):
#     count = service.get_interest_count(group_id)
#     return {"count": count}


# @router.get("/{group_id}")
# def get_interest(
#     group_id: int,
#     lang: Literal["ko", "en"] = "ko",
#     offset: int = 0,
#     limit: Optional[int] = 50,
#     current_user: AlphafinderUser = Depends(get_current_user),
#     service: InterestService = Depends(get_interest_service),
# ) -> InterestResponse:
#     if not current_user:
#         raise HTTPException(status_code=401, detail="Unauthorized")
#     interests = service.get_interest(group_id, lang, offset, limit)
#     data = [InterestTable.from_dict(interest) for interest in interests["data"]]
#     return InterestResponse(has_next=interests["has_next"], data=data)


# @router.get("/info/{ticker}")
# def get_interest_info(
#     ticker: str,
#     current_user: AlphafinderUser = Depends(get_current_user),
#     service: InterestService = Depends(get_interest_service),
# ):
#     if not current_user:
#         return {"is_interested": False, "groups": []}
#     return service.get_interest_info(current_user.id, ticker)
