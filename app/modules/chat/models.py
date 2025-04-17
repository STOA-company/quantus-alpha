import uuid
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class Message(BaseModel):
    """메시지 도메인 모델"""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    conversation_id: str
    content: str
    role: str  # 'user' 또는 'assistant'
    created_at: datetime = Field(default_factory=datetime.now)
    metadata: Dict = Field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True


class Conversation(BaseModel):
    """대화 도메인 모델"""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    title: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    messages: List[Message] = Field(default_factory=list)
    model: str = "gpt4mi"
    metadata: Dict = Field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True

    def add_message(self, content: str, role: str) -> Message:
        """대화에 새 메시지 추가"""
        message = Message(
            conversation_id=self.id,
            content=content,
            role=role,
        )
        self.messages.append(message)
        self.updated_at = datetime.now()
        return message
