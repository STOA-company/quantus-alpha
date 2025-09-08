from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class Message(BaseModel):
    """메시지 도메인 모델"""

    id: Optional[int] = None
    conversation_id: int
    content: str
    role: str  # 'user', 'assistant', 'system'
    title: Optional[str] = None  # progress 메시지용 제목
    root_message_id: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.now)

    class Config:
        arbitrary_types_allowed = True


class Conversation(BaseModel):
    """대화 도메인 모델"""

    id: Optional[int] = None
    title: Optional[str] = None
    preview: Optional[str] = None
    user_id: Optional[int] = None
    latest_job_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    messages: List[Message] = Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True

    def add_message(
        self,
        content: str,
        role: str,
        id: Optional[int] = None,
        root_message_id: Optional[int] = None,
    ) -> Message:
        """대화에 새 메시지 추가"""
        message = Message(
            id=id,
            conversation_id=self.id,
            content=content,
            role=role,
            root_message_id=root_message_id,
        )
        self.messages.append(message)
        self.updated_at = datetime.now()
        return message


class Feedback(BaseModel):
    """피드백 도메인 모델"""

    response_id: int
    user_id: int
    is_liked: bool
    feedback: str
