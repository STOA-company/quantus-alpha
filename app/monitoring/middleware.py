import time
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from .metrics import REQUEST_COUNT, REQUEST_LATENCY, update_system_metrics


class PrometheusMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()

        # Skip metrics endpoint to avoid recursion
        if request.url.path == "/metrics":
            return await call_next(request)

        method = request.method
        path = request.url.path

        try:
            response = await call_next(request)
            status_code = response.status_code

        except Exception as e:
            status_code = 500
            raise e
        finally:
            # Record request duration
            duration = time.time() - start_time
            REQUEST_LATENCY.labels(method=method, endpoint=path).observe(duration)

            # Record request count
            REQUEST_COUNT.labels(method=method, endpoint=path, status_code=status_code).inc()

            # Update system metrics
            update_system_metrics()

        return response
