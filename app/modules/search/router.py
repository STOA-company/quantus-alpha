from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.database.conn import db
from app.modules.common.enum import TranslateCountry
from app.modules.search.schemas import SearchResponse
from app.modules.search.service import SearchService, get_search_service


router = APIRouter()


@router.get("", summary="검색 기능")
def search(
    query: str,
    ctry: TranslateCountry = Query(default=TranslateCountry.KO, description="검색 시 나올 기업명 언어(ko, en)"),
    offset: int = Query(0, description="검색 시작 위치"),
    limit: int = Query(20, description="검색 결과 수"),
    service: SearchService = Depends(get_search_service),
    db: Session = Depends(db.get_db),
) -> SearchResponse:
    # limit + 1개를 요청하여 더 있는지 확인
    search_result = service.search(query, ctry, offset, limit + 1, db)
    has_more = len(search_result) > limit
    if has_more:
        search_result = search_result[:-1]  # 마지막 항목 제거

    return SearchResponse(status_code=200, message="검색이 완료되었습니다.", data=search_result, has_more=has_more)
