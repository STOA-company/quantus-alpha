from abc import ABC, abstractmethod
from typing import List, Optional, Union

from .models import Conversation, Message


class ConversationRepository(ABC):
    """대화 저장소 인터페이스"""

    @abstractmethod
    async def create(self, conversation: Conversation) -> Conversation:
        """새 대화 생성"""
        pass

    @abstractmethod
    async def get_by_id(self, conversation_id: str) -> Optional[Conversation]:
        """ID로 대화 조회"""
        pass

    @abstractmethod
    async def update(self, conversation: Conversation) -> Conversation:
        """대화 업데이트"""
        pass

    @abstractmethod
    async def delete(self, conversation_id: str) -> bool:
        """대화 삭제"""
        pass


class MessageRepository(ABC):
    """메시지 저장소 인터페이스"""

    @abstractmethod
    async def create(self, message: Message) -> Message:
        """새 메시지 생성"""
        pass

    @abstractmethod
    async def get_by_id(self, message_id: str) -> Optional[Message]:
        """ID로 메시지 조회"""
        pass

    @abstractmethod
    async def get_by_conversation_id(self, conversation_id: str) -> List[Message]:
        """대화 ID로 메시지 목록 조회"""
        pass

    @abstractmethod
    async def update(self, message: Message) -> Message:
        """메시지 업데이트"""
        pass

    @abstractmethod
    async def delete(self, message_id: str) -> bool:
        """메시지 삭제"""
        pass


class InMemoryRepository(ConversationRepository, MessageRepository):
    """메모리 기반 임시 저장소 구현"""

    def __init__(self):
        self._conversations = {}
        self._messages = {}

    async def create(self, item: Union[Conversation, Message]) -> Union[Conversation, Message]:
        """항목 생성"""
        if isinstance(item, Conversation):
            self._conversations[item.id] = item
        else:
            self._messages[item.id] = item
        return item

    async def get_by_id(self, item_id: str) -> Optional[Union[Conversation, Message]]:
        """ID로 항목 조회"""
        return self._conversations.get(item_id) or self._messages.get(item_id)

    async def get_by_conversation_id(self, conversation_id: str) -> List[Message]:
        """대화 ID로 메시지 목록 조회"""
        return [msg for msg in self._messages.values() if msg.conversation_id == conversation_id]

    async def update(self, item: Union[Conversation, Message]) -> Union[Conversation, Message]:
        """항목 업데이트"""
        if isinstance(item, Conversation):
            self._conversations[item.id] = item
        else:
            self._messages[item.id] = item
        return item

    async def delete(self, item_id: str) -> bool:
        """항목 삭제"""
        if item_id in self._conversations:
            del self._conversations[item_id]
            return True
        elif item_id in self._messages:
            del self._messages[item_id]
            return True
        return False


# 싱글톤 인스턴스 생성
conversation_repository = InMemoryRepository()
message_repository = conversation_repository  # 동일한 인스턴스 공유
