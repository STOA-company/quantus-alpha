import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from prometheus_client import Counter, Histogram

from .metrics import STREAMING_CONNECTIONS, STREAMING_ERRORS, STREAMING_MESSAGES_COUNT
from .schemas import ChatRequest
from .service import chat_service

logger = logging.getLogger(__name__)

router = APIRouter()

# 프로메테우스 메트릭
CHAT_REQUEST_COUNT = Counter("chat_requests_total", "Total number of chat requests", ["model", "status"])

CHAT_RESPONSE_TIME = Histogram("chat_response_time_seconds", "Chat response time in seconds", ["model"])


@router.post("/request")
async def request_chat(chat_request: ChatRequest):
    """채팅 요청 처리 (비동기)"""
    try:
        CHAT_REQUEST_COUNT.labels(model=chat_request.model, status="requested").inc()

        # 비동기 처리
        result = await chat_service.send_message(query=chat_request.query, model=chat_request.model)

        if result.get("status") == "error":
            CHAT_REQUEST_COUNT.labels(model=chat_request.model, status="error").inc()
            raise HTTPException(status_code=500, detail=result.get("error", "알 수 없는 오류"))

        return result

    except Exception as e:
        logger.error(f"채팅 요청 처리 중 오류: {str(e)}")
        CHAT_REQUEST_COUNT.labels(model=chat_request.model, status="error").inc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/result/{job_id}")
async def get_chat_result(job_id: str):
    """채팅 결과 조회"""
    try:
        result = await chat_service.get_message_result(job_id)

        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("message", "알 수 없는 오류"))

        return result

    except Exception as e:
        logger.error(f"결과 조회 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stream")
async def stream_chat(query: str, model: str = "gpt4mi"):
    """채팅 스트리밍 응답"""
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


@router.get("/conversation/{conversation_id}")
async def get_conversation(conversation_id: str):
    """대화 정보 조회"""
    conversation = await chat_service.get_conversation(conversation_id)

    if not conversation:
        raise HTTPException(status_code=404, detail="대화를 찾을 수 없습니다")

    return conversation
