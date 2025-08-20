from opentelemetry import trace
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

class TracingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp):
        super().__init__(app)
    
    async def dispatch(self, request, call_next):
        # 라우터별로 다른 서비스명 설정
        path_parts = request.url.path.split('/')
        router_name = path_parts[3] if len(path_parts) > 2 else "unknown"
        
        # 라우터별 서비스명 매핑
        service_mapping = {
            "financial": "financial-service",
            "news": "news-service", 
            "user": "user-service",
            "community": "community-service",
            "disclosure": "disclosure-service",
            "dividend": "dividend-service"
        }
        
        # 매핑된 서비스명 사용 (기본값: Test-Service)
        service_name = service_mapping.get(router_name, "Test-Service")
        
        # 해당 서비스명으로 트레이서 생성
        tracer = trace.get_tracer(service_name)
        
        with tracer.start_as_current_span(f"{request.method} {request.url.path}") as span:
            span.set_attribute("router.name", router_name)
            span.set_attribute("endpoint", request.url.path)
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.url", str(request.url))
            span.set_attribute("service.name", service_name)  # 서비스명도 span에 추가
            
            response = await call_next(request)
            span.set_attribute("http.status_code", response.status_code)
            
            return response