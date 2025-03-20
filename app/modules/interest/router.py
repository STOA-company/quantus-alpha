from fastapi import APIRouter, Depends, HTTPException
from typing import Literal, List, Optional
from app.utils.oauth_utils import get_current_user
from app.models.models_users import AlphafinderUser
from app.modules.interest.service import InterestService, get_interest_service
from app.modules.interest.response import InterestResponse
from app.modules.interest.request import AddInterestRequest

router = APIRouter()


@router.get("/{group_id}")
def get_interest(
    group_id: int,
    lang: Literal["kr", "en"] = "kr",
    offset: int = 0,
    limit: Optional[int] = 50,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
) -> List[InterestResponse]:
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    interests = service.get_interest(group_id, lang, offset, limit)
    return [InterestResponse.from_dict(interest) for interest in interests]


@router.post("/")
def add_interest(
    request: AddInterestRequest,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return service.add_interest(request.group_id, request.ticker)


@router.delete("/")
def delete_interest(
    group_id: int,
    tickers: List[str],
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return service.delete_interest(group_id, tickers)


@router.get("/columns")
def get_columns(lang: Literal["kr", "en"] = "kr"):
    columns = ["티커", "종목명", "현재가", "등락율", "거래대금", "거래량"]
    if lang == "en":
        columns = ["Ticker", "Name", "Price", "Change", "Amount", "Volume"]
    return columns


@router.get("/groups")
def get_groups(
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return service.get_interest_group(current_user.id)


@router.post("/groups")
def create_group(
    name: str,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return service.create_interest_group(current_user.id, name)


@router.delete("/groups")
def delete_group(
    group_id: int,
    current_user: AlphafinderUser = Depends(get_current_user),
    service: InterestService = Depends(get_interest_service),
):
    if not current_user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return service.delete_interest_group(group_id)
