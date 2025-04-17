import os

from pydantic import BaseModel


class LLMConfig(BaseModel):
    """LLM API 설정"""

    base_url: str = os.getenv("LLM_API_BASE_URL", "https://devback.alphafinder.dev/ai/research")
    timeout: int = int(os.getenv("LLM_API_TIMEOUT", "180"))
    mock_enabled: bool = False  # 모의 응답 비활성화
    retry_attempts: int = int(os.getenv("LLM_REQUEST_RETRY_ATTEMPTS", "3"))
    retry_delay: float = float(os.getenv("LLM_REQUEST_RETRY_DELAY", "1.5"))


class RabbitMQConfig(BaseModel):
    """RabbitMQ 설정"""

    host: str = os.getenv("RABBITMQ_HOST", "localhost")
    port: int = int(os.getenv("RABBITMQ_PORT", "5672"))
    user: str = os.getenv("RABBITMQ_USER", "admin")
    password: str = os.getenv("RABBITMQ_PASSWORD", "admin123")
    vhost: str = os.getenv("RABBITMQ_VHOST", "/")
    queue_name: str = os.getenv("RABBITMQ_CHAT_QUEUE", "chat_requests")
    result_exchange: str = os.getenv("RABBITMQ_RESULT_EXCHANGE", "chat_results")


# 설정 객체 초기화
llm_config = LLMConfig()
rabbitmq_config = RabbitMQConfig()
