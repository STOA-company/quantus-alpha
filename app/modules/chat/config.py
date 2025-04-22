import os

from pydantic import BaseModel


class LLMConfig(BaseModel):
    """LLM API 설정"""

    base_url: str = os.getenv("LLM_API_BASE_URL", "https://devback.alphafinder.dev/ai/research")
    api_key: str = os.getenv("LLM_API_KEY", "")
    timeout: int = int(os.getenv("LLM_API_TIMEOUT", "300"))
    mock_enabled: bool = False  # 모의 응답 비활성화
    retry_attempts: int = int(os.getenv("LLM_REQUEST_RETRY_ATTEMPTS", "3"))
    retry_delay: float = float(os.getenv("LLM_REQUEST_RETRY_DELAY", "1.5"))


# 설정 객체 초기화
llm_config = LLMConfig()
