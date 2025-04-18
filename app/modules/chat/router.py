import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from prometheus_client import Counter, Histogram

from .schemas import ChatRequest, ErrorResponse
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

    async def response_generator():
        """응답 생성기"""
        try:
            async for chunk in chat_service.process_query(query, model):
                yield chunk
        except Exception as e:
            logger.error(f"스트리밍 응답 생성 중 오류: {str(e)}")
            error_response = ErrorResponse(message=f"스트리밍 처리 오류: {str(e)}")
            yield error_response.model_dump_json()

    CHAT_REQUEST_COUNT.labels(model=model, status="streaming").inc()

    return StreamingResponse(response_generator(), media_type="text/event-stream")


@router.get("/conversation/{conversation_id}")
async def get_conversation(conversation_id: str):
    """대화 정보 조회"""
    conversation = await chat_service.get_conversation(conversation_id)

    if not conversation:
        raise HTTPException(status_code=404, detail="대화를 찾을 수 없습니다")

    return conversation
