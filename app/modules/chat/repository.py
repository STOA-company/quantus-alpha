from typing import List, Optional

from app.database.crud import database_service
from app.models.models_chat import ChatConversation, ChatMessage
from app.modules.chat.models import Conversation, Message


class ConversationRepository:
    def create(self, title: str, user_id: int) -> Conversation:
        result = database_service._insert(
            "chat_conversation",
            {
                "title": title,
                "user_id": user_id,
            },
        )

        conversation = Conversation(
            id=result.lastrowid,
            title=title,
            user_id=user_id,
        )

        return conversation

    def get_by_id(self, conversation_id: int) -> Optional[Conversation]:
        result = database_service._select("chat_conversation", id=conversation_id)
        if not result:
            return None

        conversation_data = result[0]
        db_conversation = ChatConversation(
            id=conversation_data.id,
            title=conversation_data.title,
            user_id=conversation_data.user_id,
            latest_job_id=conversation_data.latest_job_id,
            created_at=conversation_data.created_at,
            updated_at=conversation_data.updated_at,
        )

        messages = message_repository.get_by_conversation_id(conversation_id)

        domain_conversation = self._to_domain(db_conversation, messages)
        return domain_conversation

    def get_by_user_id(self, user_id: int) -> List[Conversation]:
        results = database_service._select("chat_conversation", order="created_at", ascending=False, user_id=user_id)

        conversations = []
        for data in results:
            db_conversation = ChatConversation(
                id=data.id,
                title=data.title,
                user_id=data.user_id,
                latest_job_id=data.latest_job_id,
                created_at=data.created_at,
                updated_at=data.updated_at,
            )

            messages = message_repository.get_by_conversation_id(data.id)

            conversations.append(self._to_domain(db_conversation, messages))

        return conversations

    def update(
        self, conversation_id: int, title: Optional[str] = None, latest_job_id: Optional[str] = None
    ) -> Conversation:
        update_sets = {}
        if title:
            update_sets["title"] = title
        if latest_job_id:
            update_sets["latest_job_id"] = latest_job_id

        database_service._update(
            "chat_conversation",
            update_sets,
            id=conversation_id,
        )

        conversation = self.get_by_id(conversation_id)

        return conversation

    def delete(self, conversation_id: int) -> bool:
        result = database_service._delete("chat_conversation", id=conversation_id)
        return result.rowcount > 0

    def add_message(
        self, conversation_id: int, content: str, role: str, root_message_id: Optional[int] = None
    ) -> Optional[Message]:
        conversation = self.get_by_id(conversation_id)
        if not conversation:
            return None

        message = Message(
            conversation_id=conversation_id,
            content=content,
            role=role,
            root_message_id=root_message_id,
        )

        created_message = message_repository.create(message)

        if role == "user":
            database_service._update(
                "chat_message",
                {"root_message_id": created_message.id},
                id=created_message.id,
            )

        database_service._update(
            "chat_conversation",
            {"updated_at": created_message.created_at},
            id=conversation_id,
        )

        return created_message

    def get_latest_job_id(self, conversation_id: int) -> str:
        result = database_service._select("chat_conversation", id=conversation_id)
        if not result:
            return None

        return result[0].latest_job_id

    def _to_domain(self, db_conversation: ChatConversation, messages: List[ChatMessage] = []) -> Conversation:
        return Conversation(
            id=db_conversation.id,
            title=db_conversation.title,
            user_id=db_conversation.user_id,
            latest_job_id=db_conversation.latest_job_id,
            created_at=db_conversation.created_at,
            updated_at=db_conversation.updated_at,
            messages=messages,
        )

    def _to_db(self, conversation: Conversation) -> ChatConversation:
        return ChatConversation(
            id=conversation.id,
            title=conversation.title,
            user_id=conversation.user_id,
            latest_job_id=conversation.latest_job_id,
            created_at=conversation.created_at,
            updated_at=conversation.updated_at,
        )


class MessageRepository:
    def create(self, message: Message) -> Message:
        db_message = self._to_db(message)

        result = database_service._insert(
            "chat_message",
            {
                "conversation_id": db_message.conversation_id,
                "content": db_message.content,
                "role": db_message.role,
                "root_message_id": db_message.root_message_id,
            },
        )

        message.id = result.lastrowid
        return message

    def get_by_id(self, message_id: int) -> Optional[Message]:
        result = database_service._select("chat_message", id=message_id)
        if not result:
            return None

        message_data = result[0]
        db_message = ChatMessage(
            id=message_data.id,
            conversation_id=message_data.conversation_id,
            content=message_data.content,
            role=message_data.role,
            created_at=message_data.created_at,
            updated_at=message_data.updated_at,
        )

        return self._to_domain(db_message)

    def get_by_conversation_id(self, conversation_id: int) -> List[Message]:
        results = database_service._select(
            "chat_message",
            order="created_at",
            ascending=True,
            conversation_id=conversation_id,
            role__in=["user", "assistant"],
        )

        messages = []
        for data in results:
            db_message = ChatMessage(
                id=data.id,
                conversation_id=data.conversation_id,
                content=data.content,
                role=data.role,
                created_at=data.created_at,
                updated_at=data.updated_at,
                root_message_id=data.root_message_id,
            )
            messages.append(self._to_domain(db_message))

        return messages

    def get_tasks(self, message_id: int) -> List[str]:
        results = database_service._select(
            "chat_message", order="created_at", ascending=True, root_message_id=message_id, role="system"
        )

        if not results:
            return []

        tasks = results[0].content.split("\n")

        return tasks

    def update(self, message: Message) -> Message:
        db_message = self._to_db(message)

        database_service._update(
            "chat_message",
            {
                "content": db_message.content,
                "role": db_message.role,
            },
            id=db_message.id,
        )

        return message

    def delete(self, message_id: int) -> bool:
        result = database_service._delete("chat_message", id=message_id)
        return result.rowcount > 0

    def _to_domain(self, db_message: ChatMessage) -> Message:
        return Message(
            id=db_message.id,
            conversation_id=db_message.conversation_id,
            content=db_message.content,
            role=db_message.role,
            root_message_id=db_message.root_message_id,
            created_at=db_message.created_at,
        )

    def _to_db(self, message: Message) -> ChatMessage:
        return ChatMessage(
            id=message.id,
            conversation_id=message.conversation_id,
            content=message.content,
            role=message.role,
            root_message_id=message.root_message_id,
            created_at=message.created_at,
        )


conversation_repository = ConversationRepository()
message_repository = MessageRepository()
