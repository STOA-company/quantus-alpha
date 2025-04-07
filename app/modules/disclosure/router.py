from typing import Annotated, Optional
from fastapi import APIRouter, Query, Depends
from app.modules.common.enum import TranslateCountry
from app.modules.disclosure.schemas import DisclosureResponse
from app.modules.disclosure.services import DisclosureService, get_disclosure_service

router = APIRouter()


# @router.get("/old", response_model=DisclosureResponse)
# async def get_disclosure(
#     ticker: Annotated[Optional[str], Query(description="종목 코드, 예시: SHYF")] = None,
#     year: Annotated[Optional[str], Query(description="연도, 예시: 2024, 기본값: 올해")] = None,
#     language: Annotated[TranslateCountry, Query(description="언어, 기본값: ko")] = "ko",
#     page: Annotated[Optional[int], Query(description="페이지 번호, 기본값: 1")] = 1,
#     size: Annotated[Optional[int], Query(description="페이지 크기, 기본값: 6")] = 6,
#     service: DisclosureService = Depends(get_disclosure_service),
# ):
#     result = await service.get_disclosure(ticker=ticker, year=year, language=language, page=page, size=size)
#     return DisclosureResponse(status_code=200, message="Successfully retrieved disclosure data", **result)


@router.get("", response_model=DisclosureResponse, summary="상세페이지 공시 데이터")
async def renewal_disclosure(
    ticker: Annotated[str, Query(..., description="종목 코드, 예시: CACI")] = None,
    lang: Annotated[TranslateCountry, Query(description="언어, 기본값: ko")] = TranslateCountry.KO,
    date: Annotated[Optional[str], Query(description="연도, 예시: 2024 or 20241230, 기본값: 올해")] = None,
    page: Annotated[Optional[int], Query(description="페이지 번호, 기본값: 1")] = 1,
    size: Annotated[Optional[int], Query(description="페이지 크기, 기본값: 6")] = 6,
    service: DisclosureService = Depends(get_disclosure_service),
):
    data, total_count, total_pages, offset, emotion_counts = await service.renewal_disclosure(
        ticker=ticker, date=date, page=page, size=size, lang=lang
    )

    return DisclosureResponse(
        status_code=200,
        message="Successfully retrieved disclosure data",
        data=data,
        total_count=total_count,
        total_pages=total_pages,
        current_page=page,
        offset=offset,
        size=size,
        positive_count=emotion_counts["positive_count"],
        negative_count=emotion_counts["negative_count"],
        neutral_count=emotion_counts["neutral_count"],
    )
