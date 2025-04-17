import json
import logging
import uuid
from typing import Any, Callable, Dict, Optional

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from aio_pika.pool import Pool

from .config import rabbitmq_config

logger = logging.getLogger(__name__)


class RabbitMQClient:
    """RabbitMQ 클라이언트"""

    def __init__(self):
        self._connection_pool: Optional[Pool] = None
        self._channel_pool: Optional[Pool] = None
        self._initialized = False

    async def initialize(self):
        """연결 및 채널 풀 초기화"""
        if self._initialized:
            return

        # 연결 풀 설정
        self._connection_pool = Pool(self._get_connection, max_size=10)

        # 채널 풀 설정
        self._channel_pool = Pool(self._get_channel, max_size=10)

        # 필요한 큐와 교환기 선언
        async with self._connection_pool.acquire() as connection:  # noqa
            async with self._channel_pool.acquire() as channel:
                # 메인 요청 큐 선언
                await channel.declare_queue(rabbitmq_config.queue_name, durable=True)

                # 결과 교환기 선언
                await channel.declare_exchange(rabbitmq_config.result_exchange, type="direct", durable=True)

        self._initialized = True
        logger.info("RabbitMQ 연결 초기화 완료")

    async def _get_connection(self) -> AbstractRobustConnection:
        """RabbitMQ 연결 생성"""
        connection_string = (
            f"amqp://{rabbitmq_config.user}:{rabbitmq_config.password}@"
            f"{rabbitmq_config.host}:{rabbitmq_config.port}/{rabbitmq_config.vhost}"
        )

        connection = await aio_pika.connect_robust(connection_string)
        return connection

    async def _get_channel(self) -> AbstractChannel:
        """채널 생성"""
        async with self._connection_pool.acquire() as connection:
            return await connection.channel()

    async def publish_message(self, message_data: Dict[str, Any], routing_key: str = None) -> str:
        """메시지 발행"""
        if not self._initialized:
            await self.initialize()

        message_id = str(uuid.uuid4())

        async with self._channel_pool.acquire() as channel:
            # 기본 라우팅 키가 없으면 요청 큐 사용
            if not routing_key:
                routing_key = rabbitmq_config.queue_name

            # 메시지 속성 설정
            properties = {
                "message_id": message_id,
                "content_type": "application/json",
                "delivery_mode": aio_pika.DeliveryMode.PERSISTENT,
            }

            # 메시지 발행
            await channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(message_data).encode(), **properties), routing_key=routing_key
            )

            logger.debug(f"메시지 발행 완료: {message_id}")
            return message_id

    async def consume_messages(self, queue_name: str, callback: Callable):
        """메시지 소비"""
        if not self._initialized:
            await self.initialize()

        async with self._connection_pool.acquire() as connection:  # noqa
            async with self._channel_pool.acquire() as channel:
                queue = await channel.declare_queue(queue_name, durable=True)

                async with queue.iterator() as queue_iter:
                    logger.info(f"큐 {queue_name}에서 메시지 소비 시작")
                    async for message in queue_iter:
                        async with message.process():
                            try:
                                await callback(message)
                            except Exception as e:
                                logger.error(f"메시지 처리 중 오류: {str(e)}")

    async def close(self):
        """리소스 정리"""
        if self._channel_pool:
            await self._channel_pool.close()

        if self._connection_pool:
            await self._connection_pool.close()

        self._initialized = False
        logger.info("RabbitMQ 연결 종료")


# 싱글톤 인스턴스 생성
rabbitmq_client = RabbitMQClient()
