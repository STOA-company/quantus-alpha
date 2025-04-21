import asyncio
import json
import logging
import time
from typing import AsyncGenerator, List, Optional

import httpx

from .config import llm_config
from .constants import LLM_MODEL
from .llm_client import llm_client
from .models import Conversation, Message
from .repository import conversation_repository, message_repository

logger = logging.getLogger(__name__)


class ChatService:
    def create_conversation(self, title: str, user_id: int) -> Conversation:
        return conversation_repository.create(title, user_id)

    def get_conversation(self, conversation_id: str) -> Optional[Conversation]:
        return conversation_repository.get_by_id(conversation_id)

    def get_conversation_list(self, user_id: int) -> List[Conversation]:
        return conversation_repository.get_by_user_id(user_id)

    def get_messages(self, conversation_id: str) -> List[Message]:
        return message_repository.get_by_conversation_id(conversation_id)

    def add_message(
        self, conversation_id: str, content: str, role: str, root_message_id: Optional[int] = None
    ) -> Optional[Message]:
        return conversation_repository.add_message(conversation_id, content, role, root_message_id)

    def update_conversation(self, conversation_id: str, title: str) -> Conversation:
        return conversation_repository.update(conversation_id, title)

    def delete_conversation(self, conversation_id: str) -> bool:
        return conversation_repository.delete(conversation_id)

    def get_tasks(self, message_id: int) -> List[str]:
        return message_repository.get_tasks(message_id)

    async def _poll_result(self, job_id: str, conversation_id: str, model: str) -> None:
        """백그라운드에서 작업 결과 폴링"""
        polling_interval = 3.0  # 기본 폴링 간격
        max_timeout = 550  # nginx 설정과 동기화 (600초보다 약간 적게 설정)

        start_time = time.time()

        try:
            async with httpx.AsyncClient(timeout=llm_config.timeout) as client:
                while (time.time() - start_time) < max_timeout:
                    # 폴링 간격 조정 - 시간이 지날수록 간격 늘림
                    elapsed_time = time.time() - start_time
                    if elapsed_time > 60:  # 1분 이상 지난 경우
                        polling_interval = 4.0
                    if elapsed_time > 180:  # 3분 이상 지난 경우
                        polling_interval = 5.0

                    await asyncio.sleep(polling_interval)

                    # 상태 확인
                    response = await client.get(f"{llm_config.base_url}/{job_id}")

                    if response.status_code != 200:
                        logger.warning(f"결과 폴링 중 오류: {response.status_code}")
                        continue

                    result_data = response.json()
                    status = result_data.get("status")

                    # 응답 상세 로깅 추가
                    logger.debug(f"백그라운드 폴링 응답: {status}, 데이터: {json.dumps(result_data)[:200]}...")

                    # 오류 확인
                    if status == "ERROR" or result_data.get("error"):
                        logger.error(f"LLM 작업 처리 중 오류: {result_data.get('error')}")
                        break

                    # 완료 확인
                    if status == "SUCCESS" or status == "COMPLETED":
                        # 대화에 응답 추가
                        conversation = await conversation_repository.get_by_id(conversation_id)
                        if conversation:
                            # result 객체 내의 result 필드에서 최종 답변 추출
                            result_obj = result_data.get("result", {})
                            if isinstance(result_obj, dict):
                                final_result = result_obj.get("result", "")
                                if final_result:
                                    conversation.add_message(final_result, "assistant")
                                    await conversation_repository.update(conversation)
                                    logger.info(f"대화 응답 저장 완료: {conversation_id}, 결과 길이: {len(final_result)}")
                            else:
                                # 기존 방식 (역호환성 유지)
                                str_result = str(result_obj) if result_obj else ""
                                if str_result:
                                    conversation.add_message(str_result, "assistant")
                                    await conversation_repository.update(conversation)
                                    logger.info(f"대화 응답 저장 완료: {conversation_id}, 결과 길이: {len(str_result)}")
                        break

                    logger.debug(f"백그라운드 폴링 지속 중 (경과 시간: {int(elapsed_time)}초)")

                # 최대 시간을 초과한 경우
                if (time.time() - start_time) >= max_timeout:
                    logger.warning(f"최대 대기 시간 초과: {job_id}")

        except Exception as e:
            logger.error(f"결과 폴링 중 예외 발생: {str(e)}")

    async def process_query(self, query: str, model: str = LLM_MODEL) -> AsyncGenerator[str, None]:
        """LLM 스트리밍 요청 처리"""
        async for chunk in llm_client.process_query(query, model):
            yield chunk


# 싱글톤 인스턴스 생성
chat_service = ChatService()
