from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Query

from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from app.modules.disclosure.v2.schemas import DisclosureResponse
from app.modules.disclosure.v2.services import DisclosureService, get_disclosure_service
from app.utils.quantus_auth_utils import get_current_user

router = APIRouter()


@router.get("", response_model=DisclosureResponse, summary="상세페이지 공시 데이터")
async def renewal_disclosure(
    ticker: Annotated[str, Query(..., description="종목 코드, 예시: CACI")] = None,
    lang: Annotated[TranslateCountry, Query(description="언어, 기본값: ko")] = TranslateCountry.KO,
    date: Annotated[Optional[str], Query(description="연도, 예시: 2024 or 20241230, 기본값: 올해")] = None,
    page: Annotated[Optional[int], Query(description="페이지 번호, 기본값: 1")] = 1,
    size: Annotated[Optional[int], Query(description="페이지 크기, 기본값: 6")] = 6,
    service: DisclosureService = Depends(get_disclosure_service),
    user: AlphafinderUser = Depends(get_current_user),
):
    data, total_count, total_pages, offset, emotion_counts = await service.get_disclosure_detail(
        ticker=ticker, date=date, page=page, size=size, lang=lang, user=user
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
