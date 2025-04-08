import time
import logging
import json
from typing import Dict, List, Optional
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
from datetime import datetime, timedelta
import os
from collections import defaultdict

logger = logging.getLogger("api_monitoring")

# 로그 디렉토리 확인 및 생성
log_dir = os.path.join(os.getcwd(), "log")
os.makedirs(log_dir, exist_ok=True)

# API 로그 전용 로거 생성
api_stats_logger = logging.getLogger("api_stats")
api_stats_logger.setLevel(logging.INFO)
api_stats_logger.propagate = False

# API 통계 로그 파일 핸들러
api_stats_handler = logging.FileHandler(os.path.join(log_dir, "api_stats.log"))
api_stats_handler.setLevel(logging.INFO)
api_stats_logger.addHandler(api_stats_handler)


# 간단한 인메모리 메트릭 저장소
class MetricsStore:
    def __init__(self, window_size: int = 60):
        self.window_size = window_size  # 분 단위로 데이터 보관
        self.endpoints_metrics: Dict[str, List[Dict]] = defaultdict(list)
        self.endpoints_errors: Dict[str, List[Dict]] = defaultdict(list)
        self.last_cleanup = datetime.now()

        # 시간별 통계를 위한 데이터 구조 추가
        self.hourly_metrics = {}  # 시간별 지표
        self.hourly_visitors = {}  # 시간별 접속자
        self.client_ips = set()  # 현재 시간대의 고유 IP
        self.current_hour = datetime.now().strftime("%Y-%m-%d %H:00")

    def add_request(self, endpoint: str, method: str, status_code: int, duration_ms: float, client_ip: str = None):
        now = datetime.now()
        hour_key = now.strftime("%Y-%m-%d %H:00")

        # 시간별 지표 업데이트
        if hour_key != self.current_hour:
            # 이전 시간대 데이터를 로그에 기록
            self._log_hourly_data(self.current_hour)

            # 시간이 바뀌면 클라이언트 IP 집합 초기화
            self.client_ips = set()
            self.current_hour = hour_key

        # 시간별 엔드포인트 호출 통계
        if hour_key not in self.hourly_metrics:
            self.hourly_metrics[hour_key] = defaultdict(int)

        # 엔드포인트별 호출 횟수 증가
        self.hourly_metrics[hour_key][endpoint] += 1

        # 시간별 접속자 수 집계 (IP 기반)
        if client_ip:
            if hour_key not in self.hourly_visitors:
                self.hourly_visitors[hour_key] = set()
            self.hourly_visitors[hour_key].add(client_ip)
            self.client_ips.add(client_ip)

        self.endpoints_metrics[endpoint].append(
            {
                "timestamp": now,
                "method": method,
                "status_code": status_code,
                "duration_ms": duration_ms,
                "client_ip": client_ip,
            }
        )

        # 에러인 경우 별도 저장
        if status_code >= 400:
            self.endpoints_errors[endpoint].append(
                {"timestamp": now, "method": method, "status_code": status_code, "duration_ms": duration_ms}
            )

        # 일정 시간마다 오래된 데이터 정리
        if (now - self.last_cleanup).total_seconds() > 60:
            self._cleanup_old_data()
            self.last_cleanup = now

    def _log_hourly_data(self, hour_key: str):
        """시간별 데이터를 로그 파일에 기록"""
        if hour_key not in self.hourly_metrics:
            return

        # 시간대별 총 API 호출 수 계산
        total_calls = sum(self.hourly_metrics[hour_key].values())

        # 시간대별 접속자 수
        visitor_count = len(self.hourly_visitors.get(hour_key, set()))

        # 엔드포인트별 호출 수 로깅
        log_data = {
            "timestamp": hour_key,
            "total_calls": total_calls,
            "unique_visitors": visitor_count,
            "endpoints": dict(self.hourly_metrics[hour_key]),
        }

        # JSON 형식으로 로그 기록
        api_stats_logger.info(json.dumps(log_data))

    def _cleanup_old_data(self):
        cutoff_time = datetime.now() - timedelta(minutes=self.window_size)
        cutoff_hour = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:00")

        for endpoint in list(self.endpoints_metrics.keys()):
            self.endpoints_metrics[endpoint] = [
                m for m in self.endpoints_metrics[endpoint] if m["timestamp"] > cutoff_time
            ]

            self.endpoints_errors[endpoint] = [e for e in self.endpoints_errors[endpoint] if e["timestamp"] > cutoff_time]

        # 24시간 이상 지난 시간별 통계 정리
        for hour_key in list(self.hourly_metrics.keys()):
            if hour_key < cutoff_hour:
                # 삭제하기 전에 로그에 기록
                self._log_hourly_data(hour_key)

                del self.hourly_metrics[hour_key]
                if hour_key in self.hourly_visitors:
                    del self.hourly_visitors[hour_key]

    def get_metrics_summary(self):
        """엔드포인트별 메트릭 요약 반환"""
        summary = {}

        for endpoint, metrics in self.endpoints_metrics.items():
            if not metrics:
                continue

            durations = [m["duration_ms"] for m in metrics]
            status_codes = [m["status_code"] for m in metrics]

            summary[endpoint] = {
                "count": len(metrics),
                "avg_duration_ms": sum(durations) / len(durations) if durations else 0,
                "max_duration_ms": max(durations) if durations else 0,
                "min_duration_ms": min(durations) if durations else 0,
                "error_count": sum(1 for s in status_codes if s >= 400),
                "error_rate": sum(1 for s in status_codes if s >= 400) / len(status_codes) if status_codes else 0,
                "status_codes": {str(code): status_codes.count(code) for code in set(status_codes)},
            }

        return summary

    def get_hourly_statistics(self):
        """시간별 통계 데이터 반환"""
        result = []

        # 시간별 데이터를 시간순으로 정렬
        sorted_hours = sorted(self.hourly_metrics.keys())

        for hour in sorted_hours:
            # 시간대별 총 API 호출 수 계산
            total_calls = sum(self.hourly_metrics[hour].values())

            # 시간대별 접속자 수
            visitor_count = len(self.hourly_visitors.get(hour, set()))

            # 상위 5개 엔드포인트 추출
            top_endpoints = sorted(self.hourly_metrics[hour].items(), key=lambda x: x[1], reverse=True)[:5]

            result.append(
                {
                    "hour": hour,
                    "top_endpoints": dict(top_endpoints),
                    "total_calls": total_calls,
                    "unique_visitors": visitor_count,
                }
            )

        return result

    def get_endpoint_hourly_stats(self, endpoint: Optional[str] = None):
        """특정 엔드포인트 또는 모든 엔드포인트의 시간별 호출 통계 반환"""
        # 모든 시간대 정보 가져오기
        sorted_hours = sorted(self.hourly_metrics.keys())

        if not sorted_hours:
            return []

        if endpoint:
            # 특정 엔드포인트 통계만 반환
            result = []
            for hour in sorted_hours:
                result.append({"hour": hour, "calls": self.hourly_metrics[hour].get(endpoint, 0)})
            return {"endpoint": endpoint, "data": result}
        else:
            # 모든 엔드포인트 수집
            all_endpoints = set()
            for hour_data in self.hourly_metrics.values():
                all_endpoints.update(hour_data.keys())

            # 각 엔드포인트의 시간별 호출 수
            result = {}
            for ep in all_endpoints:
                ep_data = []
                for hour in sorted_hours:
                    ep_data.append({"hour": hour, "calls": self.hourly_metrics[hour].get(ep, 0)})
                result[ep] = ep_data

            return result


# 글로벌 메트릭 스토어 객체
metrics_store = MetricsStore()


class MonitoringMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, exclude_paths: Optional[List[str]] = None, slow_api_threshold_ms: int = 1000):
        super().__init__(app)
        self.exclude_paths = exclude_paths or ["/metrics", "/health-check"]
        self.slow_api_threshold_ms = slow_api_threshold_ms

    async def dispatch(self, request: Request, call_next):
        # 특정 경로는 모니터링에서 제외
        if any(request.url.path.startswith(path) for path in self.exclude_paths):
            return await call_next(request)

        start_time = time.time()

        # 요청 처리 시도
        try:
            response = await call_next(request)
            status_code = response.status_code

        except Exception as e:
            # 예외 발생 시 로깅하고 예외 다시 발생
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000

            logger.error(
                f"Exception during request: {request.url.path} method={request.method} "
                f"duration={duration_ms:.2f}ms error={str(e)}"
            )
            raise

        # 응답 시간 계산
        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000

        # 엔드포인트 식별 (동적 경로 파라미터 처리)
        path = request.url.path

        # 클라이언트 IP 추출
        client_ip = request.client.host if request.client else None

        # 메트릭 저장 (클라이언트 IP 추가)
        metrics_store.add_request(
            endpoint=path, method=request.method, status_code=status_code, duration_ms=duration_ms, client_ip=client_ip
        )

        # 느린 API 요청 로깅
        if duration_ms > self.slow_api_threshold_ms:
            log_data = {
                "type": "slow_api",
                "path": path,
                "method": request.method,
                "duration_ms": round(duration_ms, 2),
                "status_code": status_code,
                "threshold_ms": self.slow_api_threshold_ms,
            }
            logger.warning(f"SLOW API: {json.dumps(log_data)}")

        # 오류 응답 로깅
        if status_code >= 400:
            log_data = {
                "type": "api_error",
                "path": path,
                "method": request.method,
                "duration_ms": round(duration_ms, 2),
                "status_code": status_code,
            }
            logger.error(f"API ERROR: {json.dumps(log_data)}")

        # 기본 로깅 (모든 요청)
        log_data = {
            "type": "api_request",
            "path": path,
            "method": request.method,
            "duration_ms": round(duration_ms, 2),
            "status_code": status_code,
        }
        logger.info(f"API: {json.dumps(log_data)}")

        return response


# 메트릭 엔드포인트를 위한 라우터
async def metrics_endpoint(request: Request):
    """API 성능 메트릭을 JSON 형식으로 반환하는 엔드포인트"""
    return metrics_store.get_metrics_summary()


def setup_monitoring(app: FastAPI, slow_api_threshold_ms: int = 1000):
    """애플리케이션에 모니터링 미들웨어와 메트릭 엔드포인트를 설정"""
    # 모니터링 미들웨어 추가
    app.add_middleware(MonitoringMiddleware, slow_api_threshold_ms=slow_api_threshold_ms)

    # 메트릭 엔드포인트 추가
    app.add_route("/metrics", metrics_endpoint, methods=["GET"])

    logger.info("API 모니터링 시스템이 설정되었습니다.")
