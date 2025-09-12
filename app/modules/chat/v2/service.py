import json
import logging
from typing import AsyncGenerator, List, Optional, Dict, Any
import asyncio

import httpx
import redis
import os
from app.utils.email_utils import send_email, create_notification_email
from app.utils.email_queue_utils import email_queue_manager
from app.core.config import settings

from app.modules.chat.infrastructure.config import llm_config
from app.modules.chat.infrastructure.constants import LLM_MODEL
from app.modules.chat.llm_client import llm_client
from app.modules.chat.models import Conversation, Feedback, Message
from app.modules.chat.v2.repository import conversation_repository, message_repository
from app.utils.markdown_to_pdf_utils import markdown_to_pdf
from app.core.logger import get_logger

logger = get_logger(__name__)


class ChatService:
    def __init__(self):
        # Redis 클라이언트 초기화
        self.redis_client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD if settings.REDIS_PASSWORD else None,
            decode_responses=True
        )
    
    def create_conversation(self, first_message: str, user_id: int) -> Conversation:
        conversation = conversation_repository.create(first_message, user_id)

        conversation_repository.add_message(conversation_id=conversation.id, content=first_message, role="user")

        return conversation

    def get_conversation(self, conversation_id: str) -> Optional[Conversation]:
        conversation = conversation_repository.get_by_id(conversation_id)
        messages = conversation.messages
        if messages:
            latest_message = messages[-1]
            if latest_message.role == "user":
                final_response_id, final_response = self.store_final_response(conversation_id, latest_message.id)
                analysis_history_id, analysis_history = self.store_analysis_history(conversation_id, latest_message.id)
                if final_response is not None:
                    conversation.add_message(
                        content=final_response,
                        role="assistant",
                        id=final_response_id,
                        root_message_id=latest_message.id,
                    )
                if analysis_history is not None:
                    conversation.add_message(
                        content=analysis_history,
                        role="system",
                        id=analysis_history_id,
                        root_message_id=latest_message.id,
                    )
        return conversation

    def get_conversation_list(self, user_id: int) -> List[Conversation]:
        return conversation_repository.get_by_user_id(user_id)

    def get_messages(self, conversation_id: str) -> List[Message]:
        return message_repository.get_by_conversation_id(conversation_id)

    def add_message(
        self, conversation_id: str, content: str, role: str, root_message_id: Optional[int] = None
    ) -> Optional[Message]:
        return conversation_repository.add_message(conversation_id, content, role, root_message_id)

    def update_conversation(
        self, conversation_id: str, title: Optional[str] = None, preview: Optional[str] = None
    ) -> Conversation:
        return conversation_repository.update(conversation_id=conversation_id, title=title, preview=preview)

    def delete_conversation(self, conversation_id: str) -> bool:
        return conversation_repository.delete(conversation_id)

    def get_tasks(self, message_id: int) -> List[str]:
        return message_repository.get_tasks(message_id)

    async def process_query(
        self, query: str, conversation_id: int = None, model: str = LLM_MODEL
    ) -> AsyncGenerator[str, None]:
        """LLM 스트리밍 요청 처리"""
        async for chunk in llm_client.process_query(query, model):
            data = json.loads(chunk)

            if data.get("status") == "submitted" and "job_id" in data and conversation_id:
                job_id = data.get("job_id")
                conversation_repository.update(conversation_id=conversation_id, latest_job_id=job_id)

            yield chunk

    async def process_query_background(
        self, query: str, conversation_id: int, model: str, user_id: int
    ):
        """백그라운드에서 LLM 처리 - SSE 연결과 독립적으로 실행"""
        heartbeat_key = f"chat_heartbeat:{conversation_id}"
        
        try:
            logger.info(f"백그라운드 LLM 처리 시작: conversation_id={conversation_id}")
            
            # 초기 heartbeat 설정 (30초 TTL)
            self.redis_client.setex(heartbeat_key, 30, "running")
            
            # 루트 메시지 생성/조회
            conversation = conversation_repository.get_by_id(conversation_id)
            if conversation.messages and conversation.messages[-1].role == "user" and conversation.messages[-1].content == query:
                root_message = conversation.messages[-1]
            else:
                root_message = conversation_repository.add_message(conversation_id, query, "user")
            
            assistant_response = None
            
            async for chunk in llm_client.process_query(query, model):
                # 폴링할 때마다 heartbeat 갱신 (30초 TTL)
                self.redis_client.setex(heartbeat_key, 30, "running")
                
                try:
                    data = json.loads(chunk)
                    status = data.get("status")
                    
                    # job_id 저장
                    if status == "submitted" and "job_id" in data:
                        job_id = data.get("job_id")
                        conversation_repository.update(conversation_id=conversation_id, latest_job_id=job_id)
                    
                    # progress 메시지 DB 저장
                    elif status == "progress":
                        progress_content = data.get("content", "")
                        progress_title = data.get("title", "")
                        if progress_content:
                            # title과 content 분리 저장
                            message = Message(
                                conversation_id=conversation_id,
                                content=progress_content,
                                role="system",
                                title=progress_title,
                                root_message_id=root_message.id
                            )
                            message_repository.create(message)
                    
                    # 최종 응답 저장
                    elif status == "success":
                        assistant_response = data.get("content", "")
                        
                except json.JSONDecodeError:
                    continue
            
            # 최종 처리
            if assistant_response:
                self.store_final_response(conversation_id, root_message.id)
                self.store_analysis_history(conversation_id, root_message.id)
                
                # 프리뷰 업데이트
                if conversation.preview is None:
                    conversation_repository.update(
                        conversation_id=conversation_id,
                        preview=assistant_response[:100]
                    )
                
                # 이메일 큐 처리
                try:
                    await self.process_pending_email_requests(conversation_id)
                    logger.info(f"백그라운드 이메일 큐 처리 완료: conversation_id={conversation_id}")
                except Exception as email_error:
                    logger.error(f"백그라운드 이메일 큐 처리 오류: {str(email_error)}")
                
                logger.info(f"백그라운드 LLM 처리 완료: conversation_id={conversation_id}")
            else:
                logger.warning(f"백그라운드 처리에서 응답을 받지 못함: conversation_id={conversation_id}")
                
        except Exception as e:
            logger.error(f"백그라운드 LLM 처리 중 오류: conversation_id={conversation_id}, error={str(e)}")
            # 실패 상태 저장 (필요시 추가 구현)
        finally:
            # heartbeat 정리
            try:
                self.redis_client.delete(heartbeat_key)
                logger.info(f"백그라운드 작업 heartbeat 정리: {heartbeat_key}")
            except Exception as e:
                logger.error(f"Heartbeat 정리 실패: {str(e)}")

    def get_progress_messages(self, conversation_id: int, offset: int = 0) -> List[Message]:
        """대화의 진행 상황 메시지들을 offset부터 조회"""
        messages = message_repository.get_by_conversation_id(conversation_id)
        # logger.info(f"get_progress_messages: conversation_id={conversation_id}, total_messages={len(messages)}")
        
        system_messages = [msg for msg in messages if msg.role == "system"]
        # logger.info(f"get_progress_messages: system_messages={len(system_messages)}, offset={offset}")
        
        # for i, msg in enumerate(system_messages):
        #     logger.info(f"System message {i}: {msg.content[:50]}...")
            
        result = system_messages[offset:] if len(system_messages) > offset else []
        logger.info(f"get_progress_messages: returning {len(result)} messages")
        return result

    def get_final_response_message(self, conversation_id: int) -> Optional[Message]:
        """대화의 최종 assistant 응답 조회"""
        messages = message_repository.get_by_conversation_id(conversation_id)
        assistant_messages = [msg for msg in messages if msg.role == "assistant"]
        return assistant_messages[-1] if assistant_messages else None

    def get_status(self, conversation_id: int) -> str:
        latest_job_id = conversation_repository.get_latest_job_id(conversation_id)

        if not latest_job_id:
            return "success"

        try:
            response = httpx.get(f"{llm_config.base_url}/{latest_job_id}", headers={"Access-Key": llm_config.api_key})
            if response.status_code == 200:
                status = response.json().get("status").lower()
                return status
            else:
                logger.warning(f"AI 서버 응답 오류: {response.status_code}")
                return "connection_error"
        except Exception as e:
            logger.error(f"AI 서버 연결 실패: {str(e)}")
            return "connection_error"
    
    def check_polling_health(self, conversation_id: int) -> dict:
        """폴링 연결 상태를 확인하고 진단 정보를 반환"""
        # Redis heartbeat 확인
        heartbeat_key = f"chat_heartbeat:{conversation_id}"
        heartbeat_status = self.redis_client.get(heartbeat_key)
        is_background_running = heartbeat_status == "running"
        
        # AI 서버 상태 확인
        ai_status = self.get_status(conversation_id)
        
        # 백그라운드 태스크가 죽었는지 판단
        # AI 서버는 정상(progress)인데 백그라운드 태스크가 없으면 복구 필요
        needs_recovery = (
            ai_status == "progress" and not is_background_running
        ) or ai_status == "connection_error"
        
        return {
            "ai_server_status": ai_status,
            "is_background_running": is_background_running,
            "heartbeat_key": heartbeat_key,
            "needs_recovery": needs_recovery
        }
    
    async def recover_polling_connection(self, conversation_id: int) -> dict:
        """끊어진 폴링 연결을 복구 시도"""
        try:
            logger.info(f"폴링 연결 복구 시작: conversation_id={conversation_id}")
            
            # 대화 정보 조회
            conversation = conversation_repository.get_by_id(conversation_id)
            if not conversation:
                return {"success": False, "error": "대화를 찾을 수 없습니다."}
            
            # 기존 job_id로 AI 서버 상태 재확인
            latest_job_id = conversation_repository.get_latest_job_id(conversation_id)
            if not latest_job_id:
                return {"success": False, "error": "job_id가 없습니다."}
            
            # AI 서버와 직접 연결 테스트
            try:
                response = httpx.get(
                    f"{llm_config.base_url}/{latest_job_id}", 
                    headers={"Access-Key": llm_config.api_key},
                    timeout=10.0
                )
                if response.status_code == 200:
                    status_data = response.json()
                    ai_status = status_data.get("status", "").lower()
                    
                    if ai_status == "progress":
                        # AI 서버는 여전히 처리 중이므로 백그라운드 폴링 재시작
                        logger.info(f"AI 서버는 정상, 백그라운드 폴링 재시작: conversation_id={conversation_id}")
                        # TODO: 일단 주석처리, 나중에 다시 활성화
                        # asyncio.create_task(
                        #     self.process_query_background(
                        #         conversation.title,
                        #         conversation_id,
                        #         "gpt4o",  # 기본 모델로 재시작
                        #         conversation.user_id
                        #     )
                        # )
                        
                        # 임시 폴링을 백그라운드 태스크로 실행
                        asyncio.create_task(self.polling_temp(conversation_id, latest_job_id))
                        return {"success": True, "message": "폴링 연결을 복구했습니다.", "status": "restarted"}
                    elif ai_status == "success":
                        # AI 처리가 완료된 상태
                        return {"success": True, "message": "AI 처리가 이미 완료되었습니다.", "status": "completed"}
                    else:
                        return {"success": False, "error": f"AI 서버 상태 이상: {ai_status}"}
                else:
                    logger.error(f"AI 서버 응답 오류: {response.status_code}")
                    return {"success": False, "error": f"AI 서버 연결 실패: {response.status_code}"}
                    
            except Exception as e:
                logger.error(f"AI 서버 연결 테스트 실패: {str(e)}")
                return {"success": False, "error": f"AI 서버 연결 불가: {str(e)}"}
                
        except Exception as e:
            logger.error(f"폴링 연결 복구 중 오류: {str(e)}")
            return {"success": False, "error": f"복구 실패: {str(e)}"}
    
    async def polling_temp(self, conversation_id: int, job_id: str) -> dict:
        """임시 폴링 메서드 - 기존 폴링 로직을 단순화"""
        import time
        
        heartbeat_key = f"chat_heartbeat:{conversation_id}"
        
        try:
            logger.info(f"임시 폴링 시작: conversation_id={conversation_id}, job_id={job_id}")
            
            # 임시 폴링 heartbeat 설정
            self.redis_client.setex(heartbeat_key, 30, "running")
            
            polling_interval = 3.0
            max_timeout = 300  # 5분 타임아웃
            start_time = time.time()
            previous_result = ""
            message_count = 0
            
            while (time.time() - start_time) < max_timeout:
                # 폴링 간격 조정
                elapsed_time = time.time() - start_time
                if elapsed_time > 60:
                    polling_interval = 4.0
                if elapsed_time > 180:
                    polling_interval = 5.0
                
                await asyncio.sleep(polling_interval)
                
                # heartbeat 갱신
                self.redis_client.setex(heartbeat_key, 30, "running")
                
                # AI 서버 상태 확인
                try:
                    response = httpx.get(
                        f"{llm_config.base_url}/{job_id}", 
                        headers={"Access-Key": llm_config.api_key},
                        timeout=10.0
                    )
                    
                    if response.status_code != 200:
                        logger.warning(f"임시 폴링 중 오류: {response.status_code}")
                        continue
                        
                    status_data = response.json()
                    status = status_data.get("status")
                    
                    logger.info(f"임시 폴링 상태: {status}")
                    
                    # 오류 체크
                    if status == "ERROR" or status_data.get("error"):
                        error_msg = status_data.get("error", "알 수 없는 오류")
                        logger.error(f"임시 폴링 중 AI 오류: {error_msg}")
                        return {"success": False, "error": f"AI 서버 오류: {error_msg}"}
                    
                    # 진행 상황 저장
                    step_info = status_data.get("step_info", {})
                    if step_info and isinstance(step_info, dict):
                        step_title = step_info.get("title", "")
                        step_message = step_info.get("message", "")
                        
                        if step_message and step_message != previous_result:
                            previous_result = step_message
                            
                            # DB에 progress 메시지 저장
                            content = f"[{step_title}] {step_message}" if step_title else step_message
                            message = self.add_message(
                                conversation_id=conversation_id,
                                content=content,
                                role="system"
                            )
                            
                            if message:
                                message_count += 1
                                logger.info(f"임시 폴링 메시지 저장: {content[:50]}...")
                    
                    # 완료 체크
                    if status in ["SUCCESS", "COMPLETED"]:
                        logger.info(f"임시 폴링 완료: conversation_id={conversation_id}, 저장된 메시지: {message_count}개")
                        return {
                            "success": True, 
                            "status": "completed",
                            "message_count": message_count,
                            "elapsed_time": time.time() - start_time
                        }
                        
                except httpx.RequestError as e:
                    logger.error(f"임시 폴링 네트워크 오류: {str(e)}")
                    continue
                except Exception as e:
                    logger.error(f"임시 폴링 중 예외: {str(e)}")
                    continue
            
            # 타임아웃
            logger.warning(f"임시 폴링 타임아웃: conversation_id={conversation_id}")
            return {
                "success": False, 
                "error": "타임아웃",
                "message_count": message_count,
                "elapsed_time": time.time() - start_time
            }
            
        except Exception as e:
            logger.error(f"임시 폴링 중 오류: {str(e)}")
            return {"success": False, "error": f"폴링 실패: {str(e)}"}
        finally:
            # heartbeat 정리
            try:
                self.redis_client.delete(heartbeat_key)
                logger.info(f"임시 폴링 heartbeat 정리: {heartbeat_key}")
            except Exception as e:
                logger.error(f"임시 폴링 heartbeat 정리 실패: {str(e)}")

    def get_final_response(self, conversation_id: int) -> tuple[str, str]:
        latest_job_id = conversation_repository.get_latest_job_id(conversation_id)
        if not latest_job_id:
            return None, None

        if self.get_status(conversation_id) != "success":
            return None, None

        final_response, analysis_history = llm_client.get_final_response(latest_job_id)
        analysis_history = "\n".join(analysis_history)
        return final_response, analysis_history

    def store_final_response(self, conversation_id: int, root_message_id: int) -> tuple[int, str]:
        final_response, _ = self.get_final_response(conversation_id)
        if final_response is not None:
            message = conversation_repository.add_message(
                conversation_id=conversation_id, content=final_response, role="assistant", root_message_id=root_message_id
            )
            return message.id, final_response
        return None, None

    def store_analysis_history(self, conversation_id: int, root_message_id: int) -> tuple[int, str]:
        _, analysis_history = self.get_final_response(conversation_id)
        if analysis_history is not None:
            message = conversation_repository.add_message(
                conversation_id=conversation_id, content=analysis_history, role="history", root_message_id=root_message_id
            )
            return message.id, analysis_history
        return None, None

    def feedback_response(
        self, message_id: int, user_id: int, is_liked: bool, feedback: Optional[str] = None
    ) -> Feedback:
        try:
            message = message_repository.get_by_id(message_id)

            if message.role != "assistant":
                raise ValueError("답변에 대한 피드백만 가능합니다.")
            existing_feedback = message_repository.get_feedback(message_id)
            if existing_feedback:
                return message_repository.update_feedback(message_id, is_liked, feedback)
            else:
                return message_repository.create_feedback(message_id, user_id, is_liked, feedback)
        except ValueError as e:
            raise ValueError(f"피드백 처리 중 오류 발생: {e}")
        except Exception as e:
            raise Exception(f"피드백 처리 중 오류 발생: {e}")

    def get_feedback(self, message_id: int) -> Feedback:
        return message_repository.get_feedback(message_id)

    async def send_to_email(self, conversation_id: str, email: str):        
        messages = message_repository.get_by_conversation_id(conversation_id)
        # logger.info(f"messages: {messages}")

        title = []
        report_messages = []
        # assistant 역할의 메시지만 필터링
        for msg in messages:
            if msg.role == "user":
                title.append(msg.content)
            if msg.role == "assistant":
                report_messages.append(msg.content)
        
        if title:
            title = title[-1]
        else:
            raise Exception("user 메시지를 찾을 수 없습니다.")

        if report_messages:
            report_markdown = report_messages[0]
        else:
            raise Exception("assistant 메시지를 찾을 수 없습니다.")

        # PDF 생성 (워터마크는 markdown_to_pdf_utils.py에서 처리)
        os.makedirs("./reports", exist_ok=True)
        title = title.replace(" ", "_")
        report_pdf_path = f"./reports/{title}_report.pdf"
        
        report_pdf = markdown_to_pdf(
            report_markdown, 
            report_pdf_path
        )
        
        if report_pdf is None:
            raise Exception("PDF 생성에 실패했습니다.")
            
        logger.info(f"PDF 생성 완료: {report_pdf}")
        
        # 이메일 템플릿 생성
        email_template = create_notification_email(
            greeting="안녕하세요!",
            content=f"요청하신 '{title}'에 대한 리포트가 준비되었습니다. 첨부된 PDF 파일을 확인해주세요.",
            closing="감사합니다."
        )
        
        # 이메일 전송
        try:
            logger.info(f"첨부할 파일 경로: {report_pdf}")
            logger.info(f"파일 존재 여부: {os.path.exists(report_pdf)}")
            logger.info(f"파일 크기: {os.path.getsize(report_pdf) if os.path.exists(report_pdf) else '파일 없음'}")
            
            asyncio.create_task(send_email(
                template=email_template,
                email=email,
                subject=f"{title}에 대한 리포트 결과입니다.",
                attachment_paths=[report_pdf],
                email_type="insight"
            ))
        except Exception as e:
            raise Exception(f"이메일 전송 중 오류가 발생했습니다: {str(e)}")
    
    async def queue_email_request(self, conversation_id: int, email: str, user_id: int) -> str:
        """이메일 전송 요청을 큐에 추가"""
        queue_id = email_queue_manager.add_to_queue(conversation_id, email, user_id)
        logger.info(f"이메일 요청이 큐에 추가됨: conversation_id={conversation_id}, email={email}, queue_id={queue_id}")
        return queue_id
    
    def get_email_queue_status(self, queue_id: str) -> Optional[Dict[str, Any]]:
        """이메일 큐 상태 조회"""
        return email_queue_manager.get_status(queue_id)
    
    async def process_pending_email_requests(self, conversation_id: int) -> None:
        """특정 대화에 대한 대기 중인 이메일 요청들을 처리"""
        try:
            # 대기 중인 요청들을 가져와서 처리
            logger.info(f"대화 {conversation_id}에 대한 대기 중인 이메일 요청 확인 중...")
            processed_queue_ids = email_queue_manager.process_pending_requests_for_conversation(conversation_id)
            logger.info(f"발견된 처리 대상 요청 수: {len(processed_queue_ids)}")
            
            if processed_queue_ids:
                logger.info(f"처리할 이메일 요청 {len(processed_queue_ids)}개 발견: {processed_queue_ids}")
                
                for queue_id in processed_queue_ids:
                    try:
                        # 큐 상태 정보를 가져와서 이메일 전송
                        queue_data = email_queue_manager.get_status(queue_id)
                        if queue_data:
                            email = queue_data.get("email")
                            logger.info(f"큐에서 이메일 전송 처리 중: queue_id={queue_id}, email={email}")
                            
                            # 실제 이메일 전송
                            await self.send_to_email(conversation_id, email)
                            
                            # 성공으로 표시
                            email_queue_manager.mark_as_sent(queue_id)
                            logger.info(f"큐에서 이메일 전송 완료: queue_id={queue_id}")
                    
                    except Exception as e:
                        logger.error(f"큐에서 이메일 전송 실패: queue_id={queue_id}, error={str(e)}")
                        email_queue_manager.mark_as_failed(queue_id, str(e))
        
        except Exception as e:
            logger.error(f"대기 중인 이메일 요청 처리 중 오류: conversation_id={conversation_id}, error={str(e)}")

    # async def _background_process(conversation_id: int, model: str = LLM_MODEL):
    #     async for chunk in llm_client.process_query(model):
    #         data = json.loads(chunk)

    #         if data.get("status") == "submitted" and "job_id" in data and conversation_id:
    #             job_id = data.get("job_id")
    #             conversation_repository.update(conversation_id=conversation_id, latest_job_id=job_id)

    #         yield chunk


# 싱글톤 인스턴스 생성
chat_service = ChatService()
