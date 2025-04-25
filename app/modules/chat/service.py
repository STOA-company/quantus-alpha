import json
import logging
from typing import AsyncGenerator, List, Optional

import httpx

from .config import llm_config
from .constants import LLM_MODEL
from .llm_client import llm_client
from .models import Conversation, Message
from .repository import conversation_repository, message_repository

logger = logging.getLogger(__name__)


class ChatService:
    def create_conversation(self, first_message: str, user_id: int) -> Conversation:
        conversation = conversation_repository.create(first_message, user_id)

        conversation_repository.add_message(conversation_id=conversation.id, content=first_message, role="user")

        return conversation

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
        # 캐시된 작업 ID 확인
        job_id = None
        if conversation_id:
            conversation = self.get_conversation(conversation_id)
            if conversation and conversation.latest_job_id:
                job_id = conversation.latest_job_id

        # 기존 작업 ID가 있으면 해당 작업을 계속 사용
        if job_id:
            try:
                # 기존 작업 상태 확인 로직을 구현하는 대신,
                # 그냥 llm_client에 기존 job_id 전달
                # 이미 완료된 작업이면 해당 결과를 바로 가져올 수 있음
                async for chunk in llm_client.process_query(query, model, job_id):
                    yield chunk
                return
            except Exception:
                # 기존 작업 사용 중 에러 발생 시 새 작업 시작
                pass

        # 새 작업 시작
        async for chunk in llm_client.process_query(query, model):
            data = json.loads(chunk)

            if data.get("status") == "submitted" and "job_id" in data and conversation_id:
                job_id = data.get("job_id")
                conversation_repository.update(conversation_id=conversation_id, latest_job_id=job_id)

            yield chunk

    def get_status(self, conversation_id: int) -> str:
        latest_job_id = conversation_repository.get_latest_job_id(conversation_id)

        if not latest_job_id:
            return "success"

        response = httpx.get(f"{llm_config.base_url}/{latest_job_id}", headers={"Access-Key": llm_config.api_key})
        status = response.json().get("status").lower()
        return status


# 싱글톤 인스턴스 생성
chat_service = ChatService()
