import asyncio
import logging
from typing import AsyncGenerator, Dict, Optional

import httpx

from .config import llm_config
from .llm_client import llm_client
from .models import Conversation
from .repository import conversation_repository
from .schemas import ChatRequest

logger = logging.getLogger(__name__)


class ChatService:
    """채팅 서비스 로직 구현 (RabbitMQ 없는 버전)"""

    async def create_conversation(self, model: str) -> Conversation:
        """새 대화 생성"""
        conversation = Conversation(model=model)
        await conversation_repository.create(conversation)
        return conversation

    async def send_message(self, query: str, model: str = "gpt4mi", conversation_id: Optional[str] = None) -> Dict:
        """메시지 전송 및 처리 (직접 LLM 호출)"""
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

        try:
            # LLM API에 직접 요청 전송
            request_data = ChatRequest(query=query, model=model)
            async with httpx.AsyncClient(timeout=llm_config.timeout) as client:
                response = await client.post(
                    llm_config.base_url, json=request_data.model_dump(), headers={"Content-Type": "application/json"}
                )

                if response.status_code != 200:
                    return {
                        "conversation_id": conversation_id,
                        "status": "error",
                        "error": f"API 오류: {response.status_code}",
                    }

                response_data = response.json()
                job_id = response_data.get("job_id")

                if not job_id:
                    return {
                        "conversation_id": conversation_id,
                        "status": "error",
                        "error": "API 응답에 job_id가 없습니다",
                    }

                # 비동기로 결과 폴링 시작 (백그라운드 작업)
                asyncio.create_task(self._poll_result(job_id, conversation_id, model))

                return {"conversation_id": conversation_id, "job_id": job_id, "status": "processing"}

        except Exception as e:
            logger.error(f"LLM API 요청 오류: {str(e)}")
            return {"conversation_id": conversation_id, "status": "error", "error": str(e)}

    async def _poll_result(self, job_id: str, conversation_id: str, model: str) -> None:
        """백그라운드에서 작업 결과 폴링"""
        polling_count = 0
        max_polling = 30
        polling_interval = 1.0

        try:
            async with httpx.AsyncClient(timeout=llm_config.timeout) as client:
                while polling_count < max_polling:
                    polling_count += 1

                    # 폴링 간격 조정
                    if polling_count > 5:
                        polling_interval = 2.0
                    if polling_count > 15:
                        polling_interval = 3.0

                    await asyncio.sleep(polling_interval)

                    # 상태 확인
                    response = await client.get(f"{llm_config.base_url}/{job_id}")

                    if response.status_code != 200:
                        logger.warning(f"결과 폴링 중 오류: {response.status_code}")
                        continue

                    result_data = response.json()
                    status = result_data.get("status")

                    # 오류 확인
                    if status == "ERROR" or result_data.get("error"):
                        logger.error(f"LLM 작업 처리 중 오류: {result_data.get('error')}")
                        break

                    # 완료 확인
                    if status == "COMPLETED":
                        # 대화에 응답 추가
                        conversation = await conversation_repository.get_by_id(conversation_id)
                        if conversation:
                            result = result_data.get("result", {}).get("result", "")
                            if result:
                                conversation.add_message(result, "assistant")
                                await conversation_repository.update(conversation)
                                logger.info(f"대화 응답 저장 완료: {conversation_id}")
                        break

                if polling_count >= max_polling:
                    logger.warning(f"최대 폴링 횟수 초과: {job_id}")

        except Exception as e:
            logger.error(f"결과 폴링 중 예외 발생: {str(e)}")

    async def get_message_result(self, job_id: str) -> Dict:
        """비동기 작업 결과 조회"""
        try:
            async with httpx.AsyncClient(timeout=llm_config.timeout) as client:
                response = await client.get(f"{llm_config.base_url}/{job_id}")

                if response.status_code != 200:
                    return {"status": "error", "message": f"API 오류: {response.status_code}"}

                return response.json()

        except Exception as e:
            logger.error(f"결과 조회 중 오류: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def process_query(self, query: str, model: str = "gpt4mi") -> AsyncGenerator[str, None]:
        """LLM 스트리밍 요청 처리"""
        async for chunk in llm_client.process_query(query, model):
            yield chunk

    async def get_conversation(self, conversation_id: str) -> Optional[Conversation]:
        """대화 조회"""
        return await conversation_repository.get_by_id(conversation_id)


# 싱글톤 인스턴스 생성
chat_service = ChatService()
