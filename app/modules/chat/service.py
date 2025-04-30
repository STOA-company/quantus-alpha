import json
import logging
from typing import AsyncGenerator, List, Optional

import httpx

from app.modules.chat.infrastructure.config import llm_config
from app.modules.chat.infrastructure.constants import LLM_MODEL
from app.modules.chat.llm_client import llm_client
from app.modules.chat.models import Conversation, Message
from app.modules.chat.repository import conversation_repository, message_repository

logger = logging.getLogger(__name__)


class ChatService:
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

    def get_status(self, conversation_id: int) -> str:
        latest_job_id = conversation_repository.get_latest_job_id(conversation_id)

        if not latest_job_id:
            return "success"

        response = httpx.get(f"{llm_config.base_url}/{latest_job_id}", headers={"Access-Key": llm_config.api_key})
        status = response.json().get("status").lower()
        return status

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
                conversation_id=conversation_id, content=analysis_history, role="system", root_message_id=root_message_id
            )
            return message.id, analysis_history
        return None, None


# 싱글톤 인스턴스 생성
chat_service = ChatService()
