from sqlalchemy import BigInteger, Boolean, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.models_base import BaseMixin, ServiceBase


class ChatMessage(BaseMixin, ServiceBase):
    """메시지 데이터베이스 모델"""

    __tablename__ = "chat_message"

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    conversation_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("chat_conversation.id", ondelete="CASCADE"), nullable=False, index=True
    )
    content: Mapped[String] = mapped_column(Text, nullable=False)
    role: Mapped[String] = mapped_column(String(length=20), nullable=False)  # 'user', 'assistant', 'system'
    root_message_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("chat_message.id", ondelete="CASCADE"), nullable=True, index=True
    )

    # 관계 설정
    conversation = relationship("ChatConversation", back_populates="messages")
    feedback = relationship("ChatFeedback", back_populates="response")

    def __repr__(self) -> str:
        return f"ChatMessage(id={self.id!r}, role={self.role!r})"


class ChatConversation(BaseMixin, ServiceBase):
    """대화 데이터베이스 모델"""

    __tablename__ = "chat_conversation"

    id: Mapped[BigInteger] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    title: Mapped[String] = mapped_column(String(length=255), nullable=True)
    preview: Mapped[String] = mapped_column(String(length=255), nullable=True)
    user_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("alphafinder_user.id", ondelete="CASCADE"), nullable=False, index=True
    )
    latest_job_id: Mapped[String] = mapped_column(String(length=100), nullable=True)

    messages = relationship("ChatMessage", back_populates="conversation", cascade="all, delete-orphan")
    user = relationship("AlphafinderUser", back_populates="conversations")

    def __repr__(self) -> str:
        return f"ChatConversation(id={self.id!r}, title={self.title!r})"


class ChatFeedback(BaseMixin, ServiceBase):
    """대화 피드백 데이터베이스 모델"""

    __tablename__ = "chat_feedback"

    response_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("chat_message.id", ondelete="CASCADE"), nullable=False, index=True, primary_key=True
    )
    user_id: Mapped[BigInteger] = mapped_column(
        BigInteger, ForeignKey("alphafinder_user.id", ondelete="CASCADE"), nullable=False, index=True
    )
    is_liked: Mapped[Boolean] = mapped_column(Boolean, nullable=False)
    feedback: Mapped[String] = mapped_column(String(length=255), nullable=False)

    response = relationship("ChatMessage", back_populates="feedback")
    user = relationship("AlphafinderUser", back_populates="feedback")

    def __repr__(self) -> str:
        return f"ChatFeedback(response_id={self.response_id!r}, user_id={self.user_id!r})"
