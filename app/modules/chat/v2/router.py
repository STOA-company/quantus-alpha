import json
import logging
from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from app.models.models_users import AlphafinderUser
from app.modules.chat.infrastructure.constants import LLM_MODEL
from app.modules.chat.infrastructure.rate import check_rate_limit, decrement_rate_limit, increment_rate_limit
from app.modules.chat.v2.service import chat_service
from app.monitoring.web_metrics import (
    CHAT_REQUEST_COUNT,
    STREAMING_CONNECTIONS,
    STREAMING_ERRORS,
    STREAMING_MESSAGES_COUNT,
)
from app.modules.chat.schemas import SendToEmailRequest
from app.utils.quantus_auth_utils import get_current_user_redis as get_current_user, is_staff
from app.core.logger import get_logger


logger = get_logger(__name__)

router = APIRouter()


@router.post("/conversation")
def create_conversation(first_message: str, current_user: AlphafinderUser = Depends(get_current_user)):
    """대화 생성"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    # 사용자가 스태프인지 확인
    user_is_staff = is_staff(current_user)

    # 요청 제한 확인 (스태프가 아닌 경우)
    if not user_is_staff and not check_rate_limit(current_user["uid"], user_is_staff):
        raise HTTPException(status_code=429, detail="일일 사용 한도를 초과했습니다. 하루에 3번만 요청할 수 있습니다.")

    conversation = chat_service.create_conversation(first_message, current_user["uid"])
    return {"conversation_id": conversation.id, "title": conversation.title}


@router.get("/conversation/list")
def get_conversation_list(current_user: AlphafinderUser = Depends(get_current_user)):
    """대화 목록 조회"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    conversation_list = chat_service.get_conversation_list(current_user["uid"])
    if len(conversation_list) == 0:
        return []
    return [
        {
            "conversation_id": conversation.id,
            "title": conversation.title,
            "preview": conversation.preview,
            "updated_at": conversation.updated_at + timedelta(hours=9) if conversation.updated_at else None,
        }
        for conversation in conversation_list
    ]


@router.get("/conversation/{conversation_id}")
def get_conversation(conversation_id: int, current_user: AlphafinderUser = Depends(get_current_user)):
    """대화 조회"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    conversation = chat_service.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="존재하지 않는 대화입니다.")

    if conversation.user_id != current_user["uid"]:
        raise HTTPException(status_code=403, detail="권한이 없습니다.")

    messages = [
        {
            **message.dict(),
            "created_at": message.created_at + timedelta(hours=9) if message.created_at else None,
            "is_liked": chat_service.get_feedback(message.id).is_liked if chat_service.get_feedback(message.id) else None,
        }
        for message in conversation.messages
    ]

    return {"conversation_id": conversation.id, "title": conversation.title, "messages": messages}


@router.get("/tasks/{message_id}")
def get_tasks(message_id: int, current_user: AlphafinderUser = Depends(get_current_user)):
    """대화 작업 목록 조회"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    tasks = chat_service.get_tasks(message_id)
    return {"message_id": message_id, "tasks": tasks}


@router.patch("/conversation/{conversation_id}")
def update_conversation(conversation_id: int, title: str, current_user: AlphafinderUser = Depends(get_current_user)):
    """대화 수정"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    conversation = chat_service.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="존재하지 않는 대화입니다.")

    if conversation.user_id != current_user["uid"]:
        raise HTTPException(status_code=403, detail="권한이 없습니다.")

    chat_service.update_conversation(conversation_id=conversation_id, title=title)
    return {"conversation_id": conversation_id, "title": title}


@router.delete("/conversation/{conversation_id}")
def delete_conversation(conversation_id: int, current_user: AlphafinderUser = Depends(get_current_user)):
    """대화 삭제"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    conversation = chat_service.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="존재하지 않는 대화입니다.")

    if conversation.user_id != current_user["uid"]:
        raise HTTPException(status_code=403, detail="권한이 없습니다.")
    chat_service.delete_conversation(conversation_id)
    return {"conversation_id": conversation_id, "message": "대화가 삭제되었습니다."}


@router.get("/stream")
async def stream_chat(
    query: str, conversation_id: int, model: str = LLM_MODEL, current_user: AlphafinderUser = Depends(get_current_user)
):
    """채팅 스트리밍 응답"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    # 사용자가 스태프인지 확인
    user_is_staff = is_staff(current_user)

    # 요청 제한 확인 (스태프가 아닌 경우)
    if not user_is_staff and not check_rate_limit(current_user["uid"], user_is_staff):
        raise HTTPException(status_code=429, detail="일일 사용 한도를 초과했습니다. 하루에 3번만 요청할 수 있습니다.")

    status = chat_service.get_status(conversation_id)
    if status == "pending":
        raise HTTPException(status_code=429, detail="대기 중입니다.")
    elif status == "progress":
        raise HTTPException(status_code=429, detail="답변이 생성 중입니다.")

    conversation = chat_service.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="존재하지 않는 대화입니다.")

    increment_rate_limit(current_user["uid"], user_is_staff)

    if len(conversation.messages) == 0:
        title = query
        chat_service.update_conversation(conversation_id, title)

    if conversation.messages and conversation.messages[-1].role == "user" and conversation.messages[-1].content == query:
        root_message = conversation.messages[-1]
    else:
        root_message = chat_service.add_message(conversation_id, query, "user")

    logger.info(f"스트리밍 채팅 요청 수신: query={query[:30]}..., model={model}")
    CHAT_REQUEST_COUNT.labels(model=model, status="streaming").inc()
    STREAMING_CONNECTIONS.inc()

    async def event_generator():
        """표준 SSE 형식의 이벤트 생성기"""
        assistant_response = None
        try:
            message_count = 0
            async for chunk in chat_service.process_query(query, conversation_id, model):
                message_count += 1
                # 올바른 SSE 형식으로 응답 생성 (각 행이 data: 로 시작하고 빈 줄로 끝나야 함)
                if isinstance(chunk, str):
                    STREAMING_MESSAGES_COUNT.labels(conversation_id=str(conversation_id)).inc()

                    try:
                        chunk_data = json.loads(chunk)
                        status = chunk_data.get("status")
                        
                        if status == "success":
                            assistant_response = chunk_data.get("content", "")
                        elif status == "progress":
                            # progress 상태일 때도 DB에 저장
                            progress_content = chunk_data.get("content", "")
                            progress_title = chunk_data.get("title", "")
                            if progress_content:
                                # progress 메시지를 system role로 저장
                                chat_service.add_message(
                                    conversation_id=conversation_id,
                                    content=f"[{progress_title}] {progress_content}" if progress_title else progress_content,
                                    role="system",
                                    root_message_id=root_message.id
                                )

                    except json.JSONDecodeError:
                        # JSON 파싱 실패시 그대로 전달
                        pass

                    yield f"data: {chunk}\n\n"

            logger.info(f"스트리밍 응답 완료: 총 {message_count}개 메시지 전송됨")

            if assistant_response:
                chat_service.store_final_response(conversation_id, root_message.id)
                chat_service.store_analysis_history(conversation_id, root_message.id)

                if conversation.preview is None:
                    conversation.preview = assistant_response[:100]
                    chat_service.update_conversation(
                        conversation_id=conversation_id,
                        preview=conversation.preview,
                    )
                logger.info(f"성공 응답 저장 완료: {assistant_response[:50]}...")
                
                # 스트리밍 완료 후 대기 중인 이메일 요청들 처리
                logger.info(f"이메일 큐 처리 시작: conversation_id={conversation_id}")
                try:
                    await chat_service.process_pending_email_requests(conversation_id)
                    logger.info(f"이메일 큐 처리 완료: conversation_id={conversation_id}")
                except Exception as email_error:
                    logger.error(f"이메일 큐 처리 중 오류: {str(email_error)}")
            else:
                logger.warning(f"assistant_response가 없어서 큐 처리를 건너뜀: conversation_id={conversation_id}")
                
                # assistant_response가 없어도 큐 처리는 시도해보자
                logger.info(f"assistant_response 없이도 이메일 큐 처리 시도: conversation_id={conversation_id}")
                try:
                    await chat_service.process_pending_email_requests(conversation_id)
                    logger.info(f"이메일 큐 처리 완료 (응답 없음): conversation_id={conversation_id}")
                except Exception as email_error:
                    logger.error(f"이메일 큐 처리 중 오류 (응답 없음): {str(email_error)}")

        except Exception as e:
            logger.error(f"스트리밍 응답 생성 중 오류: {str(e)}")
            # TODO: ROLLBACK
            STREAMING_ERRORS.labels(error_type="streaming_error", conversation_id=str(conversation_id)).inc()
            decrement_rate_limit(current_user["uid"])
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
def get_status(conversation_id: int, current_user: AlphafinderUser = Depends(get_current_user)):
    """대화 상태 조회"""
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    conversation = chat_service.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="존재하지 않는 대화입니다.")

    if conversation.user_id != current_user["uid"]:
        raise HTTPException(status_code=403, detail="권한이 없습니다.")

    status = chat_service.get_status(conversation_id)
    return {"status": status}


@router.post("/feedback")
def feedback_response(
    message_id: int, is_liked: bool, feedback: Optional[str] = None, current_user: AlphafinderUser = Depends(get_current_user)
):
    try:
        if not current_user:
            raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

        created_feedback = chat_service.feedback_response(message_id, current_user["uid"], is_liked, feedback)
        return {
            "message_id": created_feedback.response_id,
            "is_liked": created_feedback.is_liked,
            "feedback": created_feedback.feedback,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/send_to_email")
async def send_to_email(
    request: SendToEmailRequest,
    current_user: AlphafinderUser = Depends(get_current_user)
    ):
    try: 
        if not current_user:
            raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

        status = chat_service.get_status(request.conversation_id)
        logger.info(f"status: {status}")
        logger.info(f"request: {request}")
        
        if status == "success":
            # AI 답변이 완료된 상태 - 즉시 이메일 전송
            await chat_service.send_to_email(request.conversation_id, request.email)
            return {
                "message": "이메일이 전송되었습니다.",
                "email": request.email,
                "status": "sent"
            }
        elif status in ["pending", "progress"]:
            # AI 답변이 진행 중 - 큐에 추가하고 대기
            queue_id = await chat_service.queue_email_request(
                request.conversation_id, 
                request.email, 
                current_user["uid"]
            )
            return {
                "message": "AI 답변이 완료되면 자동으로 이메일이 전송됩니다.",
                "email": request.email,
                "status": "queued",
                "queue_id": queue_id
            }
        else:
            raise HTTPException(status_code=400, detail="알 수 없는 상태입니다.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
