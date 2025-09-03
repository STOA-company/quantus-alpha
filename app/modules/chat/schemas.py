from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    """채팅 요청 스키마"""

    query: str = Field(..., description="사용자 질문 또는 요청")
    model: str = Field("gpt4mi", description="사용할 LLM 모델")


class ResearchDetail(BaseModel):
    """연구 상세 정보 스키마"""

    research_plan: Dict[str, Any] = Field(default_factory=dict)
    collected_data: List[Dict[str, Any]] = Field(default_factory=list)
    evaluations: List[Dict[str, Any]] = Field(default_factory=list)
    tools_used: List[Dict[str, Any]] = Field(default_factory=list)


class ChatResponse(BaseModel):
    """채팅 응답 스키마"""

    status: str = Field(..., description="응답 상태 (success, error 등)")
    query: str = Field(..., description="원본 사용자 질문")
    result: str = Field(..., description="LLM 응답 텍스트")
    iterations: int = Field(0, description="분석 반복 횟수")
    analysis_history: List[Dict[str, Any]] = Field(default_factory=list)
    feedback_history: List[Dict[str, Any]] = Field(default_factory=list)
    detail: Optional[ResearchDetail] = None


class ErrorResponse(BaseModel):
    """오류 응답 스키마"""

    status: str = "error"
    message: str
    error_code: Optional[str] = None


class MessageQueueItem(BaseModel):
    """메시지 큐 아이템 스키마"""

    conversation_id: str
    query: str
    model: str = "gpt4mi"
    client_id: Optional[str] = None


class SendToEmailRequest(BaseModel):
    """이메일 전송 요청 스키마"""

    conversation_id: str
    email: str