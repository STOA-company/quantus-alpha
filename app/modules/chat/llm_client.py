import asyncio
import json
import logging
import time
from typing import AsyncGenerator, Optional

import httpx

from .config import llm_config
from .constants import LLM_MODEL
from .schemas import ChatRequest, ErrorResponse

logger = logging.getLogger(__name__)


class LLMClient:
    """외부 LLM API와 통신하는 클라이언트"""

    def __init__(self, base_url: Optional[str] = None, timeout: Optional[int] = None):
        self.base_url = base_url or llm_config.base_url
        self.timeout = timeout or llm_config.timeout

    async def process_query(self, query: str, model: str = LLM_MODEL) -> AsyncGenerator[str, None]:
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
                    polling_interval = 3.0  # 기본 폴링 간격
                    max_timeout = 550  # nginx 설정과 동기화 (600초보다 약간 적게 설정)

                    start_time = time.time()
                    previous_result = ""  # 이전 결과 저장

                    while (time.time() - start_time) < max_timeout:
                        # 폴링 간격 조정 - 시간이 지날수록 간격 늘림
                        elapsed_time = time.time() - start_time
                        if elapsed_time > 60:  # 1분 이상 지난 경우
                            polling_interval = 4.0
                        if elapsed_time > 180:  # 3분 이상 지난 경우
                            polling_interval = 5.0

                        await asyncio.sleep(polling_interval)

                        # 상태 확인 요청
                        status_response = await client.get(f"{self.base_url}/{job_id}")

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
                            yield json.dumps(ErrorResponse(message=f"LLM 서비스 오류: {error_msg}").model_dump())
                            return

                        # 진행 중인 경우 부분 결과 확인
                        step_info = status_data.get("step_info", {})
                        if step_info and isinstance(step_info, dict):
                            step_message = step_info.get("message", "")
                            if step_message and step_message != previous_result:
                                previous_result = step_message
                                yield step_message

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
                                    yield final_result
                                else:
                                    logger.warning("이전 결과와 동일한 결과를 수신했습니다")

                                # 결과 전송 후 종료
                                return
                            else:
                                logger.warning(f"예상치 못한 result 형식: {type(result_obj)}")
                                if result_obj:  # 문자열이거나 다른 형식인 경우
                                    yield str(result_obj)
                                    return

                        logger.debug(f"폴링 지속 중 (경과 시간: {int(elapsed_time)}초)")

                    # 최대 시간을 초과한 경우
                    logger.warning(f"최대 대기 시간 초과: {job_id}")
                    yield "응답 시간이 초과되었습니다. 다시 시도해주세요."
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

        # 금융 투자 관련 긴 응답 준비
        if "투자" in query or "금융" in query or "주식" in query:
            chunks = [
                "안녕하세요! ",
                "투자에 관한 질문에 답변드리겠습니다. ",
                "투자는 미래의 수익을 기대하고 자산을 활용하는 행위입니다. ",
                "주식 투자의 기본 전략에는 크게 가치 투자와 성장 투자가 있습니다. ",
                "가치 투자는 기업의 내재가치보다 저평가된 주식을 발굴하는 전략으로, ",
                "워렌 버핏이 대표적인 가치 투자자로 알려져 있습니다. ",
                "성장 투자는 빠르게 성장하는 기업에 투자하는 전략으로, ",
                "높은 성장률과 시장 점유율 확대에 중점을 둡니다. ",
                "ETF(상장지수펀드)는 다양한 종목에 분산 투자할 수 있는 효과적인 방법으로, ",
                "특히 초보 투자자에게 추천됩니다. ",
                "자산 배분은 주식, 채권, 현금, 부동산 등 다양한 자산 클래스에 자금을 분산하는 것으로, ",
                "위험을 줄이고 안정적인 수익을 추구하는 데 중요합니다. ",
                "투자 성공을 위해서는 장기적 관점, 분산 투자, 정기적인 리밸런싱이 핵심입니다. ",
                "또한 시장 타이밍보다는 '타임 인 마켓(time in market)'이 중요하다는 점을 기억하세요. ",
                "이상으로 투자에 관한 기본적인 설명을 마치겠습니다.",
            ]

        # 프로그래밍 관련 긴 응답 준비
        elif "프로그래밍" in query or "코딩" in query or "개발" in query:
            chunks = [
                "안녕하세요! ",
                "프로그래밍에 관한 질문에 답변드리겠습니다. ",
                "프로그래밍은 컴퓨터가 이해할 수 있는 언어로 명령을 작성하는 과정입니다. ",
                "주요 프로그래밍 언어에는 Python, JavaScript, Java, C++, Go 등이 있으며, ",
                "각 언어마다 고유한 특징과 활용 분야가 있습니다. ",
                "Python은 데이터 분석, 인공지능, 웹 개발에 널리 사용되며, ",
                "JavaScript는 웹 프론트엔드 개발에 필수적인 언어입니다. ",
                "프로그래밍 학습을 시작할 때는 기본 개념인 변수, 조건문, 반복문, 함수를 먼저 이해해야 합니다. ",
                "객체지향 프로그래밍(OOP)은 코드를 객체 단위로 구조화하는 패러다임으로, ",
                "캡슐화, 상속, 다형성, 추상화의 4가지 주요 원칙이 있습니다. ",
                "효율적인 코드 작성을 위해 알고리즘과 자료구조에 대한 이해도 중요합니다. ",
                "프로그래밍 실력을 향상시키려면 꾸준한, 실습과 프로젝트 경험이 필수적입니다. ",
                "또한 버전 관리 시스템(Git)과 협업 도구 사용법을 익히는 것도 중요합니다. ",
                "이상으로 프로그래밍에 관한 기본적인 설명을 마치겠습니다.",
            ]

        # 기타 일반적인 질문에 대한 응답
        else:
            chunks = [
                "안녕하세요! ",
                f"'{query}'에 관한 질문에 답변드리겠습니다. ",
                "이 주제는 매우 흥미로운 분야입니다. ",
                "현재 이 응답은 모의 응답 모드에서 생성된 것으로, ",
                "실제 데이터를 기반으로 하지 않았습니다. ",
                "하지만 스트리밍 API가 올바르게 작동하는지 테스트하기 위한 긴 응답을 생성하고 있습니다. ",
                "스트리밍은 사용자 경험을 향상시키는 중요한 기술입니다. ",
                "특히 대용량 텍스트를 생성하는 LLM 모델에서는 더욱 중요합니다. ",
                "사용자는 전체 응답이 완성되기를 기다리지 않고 점진적으로 응답을 받아볼 수 있습니다. ",
                "이는 대기 시간을 줄이고 상호작용성을 높이는 효과가 있습니다. ",
                "스트리밍 구현에는 Server-Sent Events(SSE) 또는 WebSocket 등의 기술이 활용됩니다. ",
                "이상으로 모의 응답 테스트를 마치겠습니다. 감사합니다!",
            ]

        for chunk in chunks:
            await asyncio.sleep(0.5)  # 각 청크 사이에 지연 시간 단축
            yield chunk


# 싱글톤 인스턴스 생성
llm_client = LLMClient()
