import json
import logging

import aio_pika

from .config import rabbitmq_config
from .llm_client import llm_client
from .rabbitmq import rabbitmq_client
from .repository import conversation_repository

logger = logging.getLogger(__name__)


class ChatWorker:
    """채팅 요청을 처리하는 워커"""

    def __init__(self):
        self.running = False

    async def start(self):
        """워커 시작"""
        if self.running:
            return

        self.running = True
        logger.info("채팅 워커 시작")

        # RabbitMQ 클라이언트 초기화
        await rabbitmq_client.initialize()

        # 메시지 처리 콜백 함수를 큐에 등록
        await rabbitmq_client.consume_messages(rabbitmq_config.queue_name, self.process_message)

    async def stop(self):
        """워커 중지"""
        if not self.running:
            return

        self.running = False
        logger.info("채팅 워커 중지")

        # RabbitMQ 연결 종료
        await rabbitmq_client.close()

    async def process_message(self, message: aio_pika.IncomingMessage):
        """메시지 처리"""
        try:
            # 메시지 바디를 JSON으로 파싱
            message_body = message.body.decode()
            message_data = json.loads(message_body)

            logger.info(f"메시지 처리 시작: {message.message_id}")

            # 대화 ID 및 쿼리 추출
            conversation_id = message_data.get("conversation_id")
            query = message_data.get("query")
            model = message_data.get("model", "gpt4mi")

            if not conversation_id or not query:
                logger.error(f"잘못된 메시지 형식: {message_data}")
                return

            # 대화 가져오기
            conversation = await conversation_repository.get_by_id(conversation_id)
            if not conversation:
                logger.error(f"대화를 찾을 수 없음: {conversation_id}")
                return

            # LLM 호출 및 응답 처리
            accumulated_response = ""
            async for chunk in llm_client.process_query(query, model):
                try:
                    # 텍스트 청크일 경우
                    accumulated_response += chunk
                except Exception as e:
                    logger.error(f"응답 청크 처리 오류: {str(e)}")

            # 대화에 응답 추가
            if accumulated_response:
                conversation.add_message(accumulated_response, "assistant")
                await conversation_repository.update(conversation)
                logger.info(f"응답 저장 완료: {conversation_id}")
            else:
                logger.warning(f"빈 응답: {conversation_id}")

        except json.JSONDecodeError as e:
            logger.error(f"메시지 JSON 파싱 오류: {str(e)}")
        except Exception as e:
            logger.error(f"메시지 처리 중 예외 발생: {str(e)}", exc_info=True)


# 싱글톤 인스턴스 생성
chat_worker = ChatWorker()


async def start_worker():
    """워커 시작 함수"""
    await chat_worker.start()


async def stop_worker():
    """워커 중지 함수"""
    await chat_worker.stop()
