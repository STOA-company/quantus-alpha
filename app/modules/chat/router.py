import json
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


@router.post("/conversation")
def create_conversation(first_message: str, current_user: str = Depends(get_current_user)):
    """대화 생성"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    conversation = chat_service.create_conversation(first_message, current_user.id)
    return {"conversation_id": conversation.id}


@router.get("/conversation/list")
def get_conversation_list(current_user: str = Depends(get_current_user)):
    """대화 목록 조회"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    conversation_list = chat_service.get_conversation_list(current_user.id)
    return [
        {"conversation_id": conversation.id, "title": conversation.title, "preview": conversation.messages[-1].content}
        for conversation in conversation_list
    ]


@router.get("/conversation/{conversation_id}")
def get_conversation(conversation_id: int, current_user: str = Depends(get_current_user)):
    """대화 조회"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    conversation = chat_service.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="존재하지 않는 대화입니다.")

    if conversation.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="권한이 없습니다.")

    return {"conversation_id": conversation.id, "title": conversation.title, "messages": conversation.messages}


@router.get("/tasks/{message_id}")
def get_tasks(message_id: int, current_user: str = Depends(get_current_user)):
    """대화 작업 목록 조회"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    tasks = chat_service.get_tasks(message_id)
    return {"message_id": message_id, "tasks": tasks}


@router.patch("/conversation/{conversation_id}")
def update_conversation(conversation_id: int, title: str, current_user: str = Depends(get_current_user)):
    """대화 수정"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    conversation = chat_service.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="존재하지 않는 대화입니다.")

    if conversation.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="권한이 없습니다.")

    chat_service.update_conversation(conversation_id, title)
    return {"conversation_id": conversation_id, "title": title}


@router.delete("/conversation/{conversation_id}")
def delete_conversation(conversation_id: int, current_user: str = Depends(get_current_user)):
    """대화 삭제"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    conversation = chat_service.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="존재하지 않는 대화입니다.")

    if conversation.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="권한이 없습니다.")
    chat_service.delete_conversation(conversation_id)
    return {"conversation_id": conversation_id, "message": "대화가 삭제되었습니다."}


@router.get("/stream")
async def stream_chat(
    query: str, conversation_id: int, model: str = LLM_MODEL, current_user: str = Depends(get_current_user)
):
    """채팅 스트리밍 응답"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    status = chat_service.get_status(conversation_id)
    if status == "pending":
        raise HTTPException(status_code=429, detail="대기 중입니다.")
    elif status == "progress":
        raise HTTPException(status_code=429, detail="답변이 생성 중입니다.")

    conversation = chat_service.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="존재하지 않는 대화입니다.")

    if len(conversation.messages) == 0:
        title = query
        chat_service.update_conversation(conversation_id, title)

    root_message = chat_service.add_message(conversation_id, query, "user")

    logger.info(f"스트리밍 채팅 요청 수신: query={query[:30]}..., model={model}")
    CHAT_REQUEST_COUNT.labels(model=model, status="streaming").inc()
    STREAMING_CONNECTIONS.inc()

    async def event_generator():
        """표준 SSE 형식의 이벤트 생성기"""
        assistant_response = None
        system_response = ""
        try:
            message_count = 0
            async for chunk in chat_service.process_query(query, conversation_id, model):
                message_count += 1
                # 올바른 SSE 형식으로 응답 생성 (각 행이 data: 로 시작하고 빈 줄로 끝나야 함)
                if isinstance(chunk, str):
                    STREAMING_MESSAGES_COUNT.labels(model=model).inc()

                    try:
                        chunk_data = json.loads(chunk)
                        if chunk_data.get("status") == "success":
                            assistant_response = chunk_data.get("content", "")
                        else:
                            system_response += chunk_data.get("content", "")
                            system_response += "\n"
                    except json.JSONDecodeError:
                        # JSON 파싱 실패시 그대로 전달
                        pass

                    yield f"data: {chunk}\n\n"

            logger.info(f"스트리밍 응답 완료: 총 {message_count}개 메시지 전송됨")

            if assistant_response:
                chat_service.add_message(conversation_id, assistant_response, "assistant", root_message.id)
                logger.info(f"성공 응답 저장 완료: {assistant_response[:50]}...")

            if system_response:
                chat_service.add_message(conversation_id, system_response, "system", root_message.id)
                logger.info(f"시스템 응답 저장 완료: {system_response[:50]}...")

        except Exception as e:
            logger.error(f"스트리밍 응답 생성 중 오류: {str(e)}")
            # TODO: ROLLBACK
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


@router.get("/status/{conversation_id}")
def get_status(conversation_id: int, current_user: str = Depends(get_current_user)):
    """대화 상태 조회"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    conversation = chat_service.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="존재하지 않는 대화입니다.")

    if conversation.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="권한이 없습니다.")

    status = chat_service.get_status(conversation_id)
    return {"status": status}
