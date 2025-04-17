import asyncio
import json
import logging
from typing import AsyncGenerator, Optional

import httpx

from .config import llm_config
from .schemas import ChatRequest, ErrorResponse

logger = logging.getLogger(__name__)


class LLMClient:
    """외부 LLM API와 통신하는 클라이언트"""

    def __init__(self, base_url: Optional[str] = None, timeout: Optional[int] = None):
        self.base_url = base_url or llm_config.base_url
        self.timeout = timeout or llm_config.timeout

    async def process_query(self, query: str, model: str = "gpt4mi") -> AsyncGenerator[str, None]:
        """LLM API에 요청을 전송하고 응답을 스트리밍으로 반환"""
        request_data = ChatRequest(query=query, model=model)

        if llm_config.mock_enabled:
            # 테스트용 모의 응답 생성
            async for chunk in self._mock_streaming_response(query):
                yield chunk
            return

        retry_count = 0
        while retry_count < llm_config.retry_attempts:
            try:
                # 1. 초기 요청을 보내 job_id 획득
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.post(
                        self.base_url, json=request_data.model_dump(), headers={"Content-Type": "application/json"}
                    )

                    if response.status_code != 200:
                        error_text = response.text
                        logger.error(f"LLM API 초기 요청 오류: {response.status_code} - {error_text}")
                        error_message = f"LLM 서비스 오류: {response.status_code}"
                        yield json.dumps(ErrorResponse(message=error_message).model_dump())
                        return

                    # job_id 추출
                    response_data = response.json()
                    job_id = response_data.get("job_id")

                    if not job_id:
                        logger.error(f"LLM API 응답에 job_id가 없음: {response_data}")
                        error_message = "LLM 서비스 응답 형식 오류"
                        yield json.dumps(ErrorResponse(message=error_message).model_dump())
                        return

                    logger.info(f"작업 요청 성공: {job_id}")

                    # 첫 번째 응답으로 "생각 중..." 메시지 반환
                    yield "생각 중..."

                    # 2. job_id로 주기적으로 폴링하여 결과 확인
                    polling_count = 0
                    max_polling = 30  # 최대 폴링 횟수
                    polling_interval = 1.0  # 폴링 간격 (초)

                    previous_result = ""  # 이전 결과 저장

                    while polling_count < max_polling:
                        polling_count += 1

                        # 폴링 간격을 약간 늘려 서버 부하 감소
                        if polling_count > 5:
                            polling_interval = 2.0
                        if polling_count > 15:
                            polling_interval = 3.0

                        await asyncio.sleep(polling_interval)

                        # 상태 확인 요청
                        status_response = await client.get(f"{self.base_url}/{job_id}")

                        if status_response.status_code != 200:
                            logger.warning(f"폴링 중 오류 발생: {status_response.status_code}")
                            continue

                        status_data = status_response.json()
                        status = status_data.get("status")

                        # 오류 체크
                        if status == "ERROR" or status_data.get("error"):
                            error_msg = status_data.get("error", "알 수 없는 오류")
                            logger.error(f"작업 처리 중 오류: {error_msg}")
                            yield json.dumps(ErrorResponse(message=f"LLM 서비스 오류: {error_msg}").model_dump())
                            return

                        # 완료 체크
                        if status == "COMPLETED":
                            result = status_data.get("result", {}).get("result", "")
                            if result and result != previous_result:
                                # 마지막 결과가 도착했으므로 전체 응답을 반환
                                yield result
                            break

                        # 진행 중인 경우 부분 결과 확인
                        step_info = status_data.get("step_info", {})
                        if step_info and isinstance(step_info, dict):
                            step_message = step_info.get("message", "")
                            if step_message and step_message != previous_result:
                                previous_result = step_message
                                yield step_message

                        logger.debug(f"폴링 {polling_count}/{max_polling}: 상태 = {status}")

                    # 최대 폴링 횟수를 초과한 경우
                    if polling_count >= max_polling:
                        logger.warning(f"최대 폴링 횟수 초과: {job_id}")
                        yield json.dumps(ErrorResponse(message="응답 시간 초과").model_dump())

                    return

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                retry_count += 1
                logger.warning(f"LLM API 연결 오류 (재시도 {retry_count}/{llm_config.retry_attempts}): {str(e)}")
                if retry_count >= llm_config.retry_attempts:
                    error_message = "LLM 서비스에 연결할 수 없습니다. 잠시 후 다시 시도해주세요."
                    yield json.dumps(ErrorResponse(message=error_message).model_dump())
                    return
                await asyncio.sleep(llm_config.retry_delay * retry_count)
            except Exception as e:
                logger.error(f"LLM API 요청 중 예외 발생: {str(e)}")
                error_message = f"LLM 서비스 오류: {str(e)}"
                yield json.dumps(ErrorResponse(message=error_message).model_dump())
                return

    async def _mock_streaming_response(self, query: str) -> AsyncGenerator[str, None]:
        """테스트를 위한 모의 스트리밍 응답 생성"""
        chunks = [
            "안녕하세요! ",
            "질문에 대한 답변을 생성 중입니다. ",
            f"'{query}'에 대한 분석을 수행 중입니다. ",
            "이것은 모의 응답으로, ",
            "실제 LLM API가 연결되면 해당 서비스의 응답이 스트리밍됩니다.",
        ]

        for chunk in chunks:
            await asyncio.sleep(1)  # 각 청크 사이에 지연 추가
            yield chunk


# 싱글톤 인스턴스 생성
llm_client = LLMClient()
