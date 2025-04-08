from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse
from starlette.templating import Jinja2Templates
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional
import logging
from app.middlewares.trusted_hosts import get_current_username
from app.middlewares.monitoring import metrics_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/monitoring", tags=["monitoring"])

# 템플릿 디렉토리 설정 - 절대 경로 사용
templates_path = os.path.join(os.getcwd(), "static/templates")
templates = Jinja2Templates(directory=templates_path)

# Jinja2 환경에 min 함수 추가
templates.env.globals["min"] = min


# API 상태 확인 결과 로드
def load_api_health_data() -> Optional[Dict[str, Any]]:
    """API 상태 확인 결과 로드"""
    history_file = os.path.join(os.getcwd(), "log/api_health_history.json")

    try:
        if os.path.exists(history_file):
            with open(history_file, "r") as f:
                data = json.load(f)
                return data
        else:
            return None
    except Exception as e:
        logger.error(f"API 상태 데이터 로드 실패: {str(e)}")
        return None


@router.get("/dashboard", response_class=HTMLResponse)
async def monitoring_dashboard(request: Request, username: str = Depends(get_current_username)):
    """모니터링 대시보드 페이지"""
    # API 메트릭 가져오기
    api_metrics = metrics_store.get_metrics_summary()

    # API 상태 확인 결과 가져오기
    health_data = load_api_health_data()

    # 응답 시간 임계값 설정
    thresholds = {
        "warning": 1000,  # 1초
        "critical": 3000,  # 3초
    }

    return templates.TemplateResponse(
        "monitoring_dashboard.html",
        {
            "request": request,
            "api_metrics": api_metrics,
            "health_data": health_data,
            "thresholds": thresholds,
            "current_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "page_title": "API 모니터링 대시보드",
        },
    )


@router.get("/api/metrics")
async def get_api_metrics(username: str = Depends(get_current_username)):
    """API 메트릭 JSON으로 반환"""
    return metrics_store.get_metrics_summary()


@router.get("/api/hourly-stats")
async def get_hourly_statistics(username: str = Depends(get_current_username)):
    """시간별 API 사용 통계"""
    return metrics_store.get_hourly_statistics()


@router.get("/api/endpoint-hourly-stats")
async def get_endpoint_hourly_stats(endpoint: str = None, username: str = Depends(get_current_username)):
    """엔드포인트별 시간 통계"""
    return metrics_store.get_endpoint_hourly_stats(endpoint)


@router.get("/api/health")
async def get_api_health(username: str = Depends(get_current_username)):
    """API 상태 확인 결과 JSON으로 반환"""
    health_data = load_api_health_data()
    if health_data is None:
        raise HTTPException(status_code=404, detail="API 상태 데이터를 찾을 수 없습니다")
    return health_data
