import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from prometheus_client import Counter, Histogram

from app.utils.oauth_utils import get_current_user

from .constants import LLM_MODEL
from .metrics import STREAMING_CONNECTIONS, STREAMING_ERRORS, STREAMING_MESSAGES_COUNT
from .service import chat_service

logger = logging.getLogger(__name__)

router = APIRouter()

# 프로메테우스 메트릭
CHAT_REQUEST_COUNT = Counter("chat_requests_total", "Total number of chat requests", ["model", "status"])

CHAT_RESPONSE_TIME = Histogram("chat_response_time_seconds", "Chat response time in seconds", ["model"])


@router.get("/stream")
async def stream_chat(query: str, model: str = LLM_MODEL, current_user: str = Depends(get_current_user)):
    """채팅 스트리밍 응답"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    logger.info(f"스트리밍 채팅 요청 수신: query={query[:30]}..., model={model}")
    CHAT_REQUEST_COUNT.labels(model=model, status="streaming").inc()
    STREAMING_CONNECTIONS.inc()

    async def event_generator():
        """표준 SSE 형식의 이벤트 생성기"""
        try:
            message_count = 0
            async for chunk in chat_service.process_query(query, model):
                message_count += 1
                # 올바른 SSE 형식으로 응답 생성 (각 행이 data: 로 시작하고 빈 줄로 끝나야 함)
                if isinstance(chunk, str):
                    STREAMING_MESSAGES_COUNT.labels(model=model).inc()
                    yield f"data: {chunk}\n\n"

            logger.info(f"스트리밍 응답 완료: 총 {message_count}개 메시지 전송됨")

        except Exception as e:
            logger.error(f"스트리밍 응답 생성 중 오류: {str(e)}")
            STREAMING_ERRORS.labels(model=model, error_type="streaming_error").inc()
            yield f"data: 오류가 발생했습니다: {str(e)}\n\n"
        finally:
            STREAMING_CONNECTIONS.dec()

    # 올바른 SSE 응답을 위한 헤더 설정
    headers = {
        "Content-Type": "text/event-stream",  # 명시적으로 SSE 콘텐츠 타입 지정
        "Cache-Control": "no-cache, no-transform",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",  # NGINX 버퍼링 비활성화
        "Transfer-Encoding": "chunked",
    }

    return StreamingResponse(event_generator(), headers=headers)
