import asyncio
import json
import logging
import time
from typing import AsyncGenerator

import httpx

from app.modules.chat.infrastructure.config import llm_config
from app.modules.chat.infrastructure.constants import LLM_MODEL
from app.modules.chat.schemas import ChatRequest

logger = logging.getLogger(__name__)


class LLMClient:
    """외부 LLM API와 통신하는 클라이언트"""

    def __init__(self):
        self.base_url = llm_config.base_url
        self.timeout = llm_config.timeout
        self.api_key = llm_config.api_key

    async def process_query(self, query: str, model: str = LLM_MODEL) -> AsyncGenerator[str, None]:
        """LLM API에 요청을 전송하고 응답을 스트리밍으로 반환"""
        request_data = ChatRequest(query=query, model=model)

        if llm_config.mock_enabled:
            async for chunk in self._mock_streaming_response(query):
                yield chunk
            return

        retry_count = 0
        while retry_count < llm_config.retry_attempts:
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    headers = {"Content-Type": "application/json", "Access-Key": self.api_key}
                    response = await client.post(self.base_url, json=request_data.model_dump(), headers=headers)

                    if response.status_code != 200:
                        error_text = response.text
                        logger.error(f"LLM API 초기 요청 오류: {response.status_code} - {error_text}")
                        error_message = f"LLM 서비스 오류: {response.status_code}"
                        msg = {"status": "failed", "content": error_message}
                        yield json.dumps(msg, ensure_ascii=False)
                        return

                    response_data = response.json()
                    job_id = response_data.get("job_id")

                    if not job_id:
                        logger.error(f"LLM API 응답에 job_id가 없음: {response_data}")
                        error_message = "LLM 서비스 응답 형식 오류"
                        msg = {"status": "failed", "content": error_message}
                        yield json.dumps(msg, ensure_ascii=False)
                        return

                    logger.info(f"작업 요청 성공: {job_id}")

                    initial_msg = {
                        "status": "submitted",
                        "title" : "연구 시작",
                        "content": "주요 뉴스, 공시, 기업 이슈 등을 종합 분석하여 질문에 대한 답변을 준비하고 있습니다.",
                        "job_id": job_id,
                    }
                    yield json.dumps(initial_msg, ensure_ascii=False)

                    # 통합 폴링 메서드 사용
                    max_timeout = self.timeout - 50  # nginx 설정과 동기화 (1800 약간 적게 설정)
                    
                    async for poll_result in self.poll_job_with_heartbeat(
                        job_id=job_id,
                        heartbeat_callback=None,  # process_query에서는 하트비트 불필요
                        max_timeout=max_timeout,
                        polling_interval=3.0
                    ):
                        # 결과를 JSON 문자열로 변환하여 yield
                        yield json.dumps(poll_result, ensure_ascii=False)
                        
                        # 완료되면 종료
                        if poll_result.get("status") in ["success", "failed"]:
                            return

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                retry_count += 1
                logger.warning(f"LLM API 연결 오류 (재시도 {retry_count}/{llm_config.retry_attempts}): {str(e)}")
                if retry_count >= llm_config.retry_attempts:
                    error_message = "LLM 서비스에 연결할 수 없습니다. 잠시 후 다시 시도해주세요."
                    msg = {"status": "failed", "content": error_message}
                    yield json.dumps(msg, ensure_ascii=False)
                    return
                await asyncio.sleep(llm_config.retry_delay * retry_count)
            except Exception as e:
                logger.error(f"LLM API 요청 중 예외 발생: {str(e)}")
                error_message = f"LLM 서비스 오류: {str(e)}"
                msg = {"status": "failed", "content": error_message}
                yield json.dumps(msg, ensure_ascii=False)
                return

    async def _mock_streaming_response(self, query: str) -> AsyncGenerator[str, None]:
        """테스트를 위한 모의 스트리밍 응답 생성"""
        mock_job_id = f"mock_{int(time.time())}"
        initial_msg = {
            "status": "submitted",
            "content": "주요 뉴스, 공시, 기업 이슈 등을 종합 분석하여 질문에 대한 답변을 준비하고 있습니다.",
            "job_id": mock_job_id,
        }
        yield json.dumps(initial_msg, ensure_ascii=False)
        await asyncio.sleep(1.0)

        # 진행 상황 메시지
        progress_messages = [
            "listening the query from request",
            "generating the research plans",
            "processing sub-question 1/4",
            "processing sub-question 2/4",
            "processing sub-question 3/4",
            "processing sub-question 4/4",
            "reviewing the research result",
        ]

        for msg in progress_messages:
            progress_msg = {"status": "progress", "content": msg}
            await asyncio.sleep(0.8)
            yield json.dumps(progress_msg, ensure_ascii=False)

        # 최종 응답
        if "투자" in query or "금융" in query or "주식" in query:
            final_content = "투자는 미래의 수익을 기대하고 자산을 활용하는 행위입니다. 주식 투자의 기본 전략에는 크게 가치 투자와 성장 투자가 있습니다. 가치 투자는 기업의 내재가치보다 저평가된 주식을 발굴하는 전략으로, 워렌 버핏이 대표적인 가치 투자자로 알려져 있습니다."
        elif "프로그래밍" in query or "코딩" in query or "개발" in query:
            final_content = "프로그래밍은 컴퓨터가 이해할 수 있는 언어로 명령을 작성하는 과정입니다. 주요 프로그래밍 언어에는 Python, JavaScript, Java, C++, Go 등이 있으며, 각 언어마다 고유한 특징과 활용 분야가 있습니다."
        else:
            final_content = f"'{query}'에 관한 질문에 답변드리겠습니다. 이 주제는 매우 흥미로운 분야입니다. 스트리밍은 사용자 경험을 향상시키는 중요한 기술입니다. 특히 대용량 텍스트를 생성하는 LLM 모델에서는 더욱 중요합니다."

        await asyncio.sleep(1.5)
        final_msg = {"status": "success", "content": final_content}
        yield json.dumps(final_msg, ensure_ascii=False)

    async def poll_job_with_heartbeat(
        self, 
        job_id: str, 
        heartbeat_callback=None,
        max_timeout: int = 300,
        polling_interval: float = 3.0
    ) -> AsyncGenerator[dict, None]:
        """하트비트 기능이 포함된 통합 폴링 메서드"""
        start_time = time.time()
        previous_result = ""
        
        while (time.time() - start_time) < max_timeout:
            # 폴링 간격 조정 - 시간이 지날수록 간격 늘림
            elapsed_time = time.time() - start_time
            current_interval = polling_interval
            if elapsed_time > 60:  # 1분 이상 지난 경우
                current_interval = 4.0
            if elapsed_time > 180:  # 3분 이상 지난 경우
                current_interval = 5.0

            await asyncio.sleep(current_interval)

            # 하트비트 갱신 (콜백이 제공된 경우)
            if heartbeat_callback:
                heartbeat_callback()

            # 상태 확인 요청
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    status_response = await client.get(
                        f"{self.base_url}/{job_id}", 
                        headers={"Access-Key": self.api_key}
                    )

                    if status_response.status_code != 200:
                        logger.warning(f"폴링 중 오류 발생: {status_response.status_code}")
                        continue

                    status_data = status_response.json()
                    status = status_data.get("status")

                    # 응답 상세 로깅 추가
                    logger.debug(f"LLM 응답 상태: {status}, 데이터: {json.dumps(status_data)[:200]}...")

                    # 오류 체크
                    if status == "ERROR" or status_data.get("error"):
                        error_msg = status_data.get("error", "알 수 없는 오류")
                        logger.error(f"LLM 작업 처리 중 오류: {error_msg}")
                        yield {
                            "status": "failed", 
                            "content": f"LLM 서비스 오류: {error_msg}",
                            "error": error_msg
                        }
                        return

                    # 진행 중인 경우 부분 결과 확인
                    step_info = status_data.get("step_info", {})
                    if step_info and isinstance(step_info, dict):
                        step_title = step_info.get("title", "")
                        step_message = step_info.get("message", "")
                        if step_message and step_message != previous_result:
                            previous_result = step_message
                            yield {
                                "status": "progress", 
                                "title": step_title,
                                "content": step_message,
                                "step_info": step_info
                            }

                    # 완료 체크
                    if status == "SUCCESS" or status == "COMPLETED":
                        # result 객체 내의 result 필드에서 최종 답변 추출
                        result_obj = status_data.get("result", {})
                        if isinstance(result_obj, dict):
                            final_result = result_obj.get("result", "")
                            logger.info(
                                f"LLM 응답 완료: job_id={job_id}, 결과 길이={len(final_result) if final_result else 0}"
                            )

                            if not final_result:
                                logger.warning("완료 상태이지만 결과가 비어있습니다")
                                final_result = "응답을 생성하는 중 문제가 발생했습니다. 다시 시도해주세요."

                            # 최종 결과가 이전 결과와 다른 경우에만 반환
                            if final_result != previous_result:
                                yield {
                                    "status": "success", 
                                    "content": final_result,
                                    "result": result_obj
                                }
                            else:
                                logger.warning("이전 결과와 동일한 결과를 수신했습니다")

                            # 결과 전송 후 종료
                            return
                        else:
                            logger.warning(f"예상치 못한 result 형식: {type(result_obj)}")
                            if result_obj:  # 문자열이거나 다른 형식인 경우
                                yield {
                                    "status": "success", 
                                    "content": str(result_obj),
                                    "result": result_obj
                                }
                                return

                    logger.debug(f"폴링 지속 중 (경과 시간: {int(elapsed_time)}초)")

            except httpx.RequestError as e:
                logger.error(f"폴링 네트워크 오류: {str(e)}")
                continue
            except Exception as e:
                logger.error(f"폴링 중 예외: {str(e)}")
                continue

        # 최대 시간을 초과한 경우
        logger.warning(f"최대 대기 시간 초과: {job_id}")
        yield {
            "status": "failed", 
            "content": "응답이 생성이 길어지고 있습니다. 잠시만 기다려주세요.",
            "timeout": True
        }

    def get_final_response(self, job_id: str) -> tuple[str, list[str]]:
        response = httpx.get(f"{self.base_url}/{job_id}", headers={"Access-Key": self.api_key})
        data = response.json()
        result = data.get("result", {})

        final_response = result.get("result", "")
        if final_response == "" and result.get("status") == "error":
            final_response = "응답을 생성하는 중 문제가 발생했습니다. 다시 시도해주세요."
        
        analysis_history = result.get("analysis_history", [])

        return final_response, analysis_history


# 싱글톤 인스턴스 생성
llm_client = LLMClient()
