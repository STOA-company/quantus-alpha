import logging
from typing import AsyncGenerator, Dict, Optional

from .llm_client import llm_client
from .models import Conversation
from .rabbitmq import rabbitmq_client
from .repository import conversation_repository
from .schemas import MessageQueueItem

logger = logging.getLogger(__name__)


class ChatService:
    """채팅 서비스 로직 구현"""

    async def initialize(self):
        """필요한 리소스 초기화"""
        await rabbitmq_client.initialize()

    async def create_conversation(self, model: str = "gpt4mi") -> Conversation:
        """새 대화 생성"""
        conversation = Conversation(model=model)
        await conversation_repository.create(conversation)
        return conversation

    async def send_message(self, query: str, model: str = "gpt4mi", conversation_id: Optional[str] = None) -> Dict:
        """메시지 전송 및 처리"""
        # 대화가 없으면 새로 생성
        if not conversation_id:
            conversation = await self.create_conversation(model)
            conversation_id = conversation.id
        else:
            conversation = await conversation_repository.get_by_id(conversation_id)
            if not conversation:
                conversation = Conversation(id=conversation_id, model=model)
                await conversation_repository.create(conversation)

        # 사용자 메시지 추가
        conversation.add_message(query, "user")
        await conversation_repository.update(conversation)

        # MQ에 메시지 전송
        message_data = MessageQueueItem(conversation_id=conversation_id, query=query, model=model)

        try:
            await rabbitmq_client.initialize()
            message_id = await rabbitmq_client.publish_message(message_data.model_dump())

            return {"conversation_id": conversation_id, "message_id": message_id, "status": "processing"}
        except Exception as e:
            logger.error(f"메시지 큐 전송 오류: {str(e)}")
            return {"conversation_id": conversation_id, "status": "error", "error": str(e)}

    async def process_query(self, query: str, model: str = "gpt4mi") -> AsyncGenerator[str, None]:
        """LLM 스트리밍 요청 처리"""
        async for chunk in llm_client.process_query(query, model):
            yield chunk

    async def get_conversation(self, conversation_id: str) -> Optional[Conversation]:
        """대화 조회"""
        return await conversation_repository.get_by_id(conversation_id)


# 싱글톤 인스턴스 생성
chat_service = ChatService()
