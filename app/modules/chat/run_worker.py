import asyncio
import signal
import sys

from app.core.logger import setup_logger
from app.modules.chat.worker import start_worker, stop_worker

# 로그 설정
logger = setup_logger(__name__)

# 종료 이벤트
shutdown_event = asyncio.Event()


async def shutdown():
    """워커 종료 처리"""
    logger.info("워커 종료 처리 중...")
    shutdown_event.set()
    await stop_worker()
    logger.info("워커가 정상적으로 종료되었습니다.")


def handle_signal(signum, frame):
    """시그널 핸들러"""
    logger.info(f"시그널 수신: {signum}")
    if not shutdown_event.is_set():
        asyncio.create_task(shutdown())


async def main():
    """메인 함수"""
    # 시그널 핸들러 등록
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        logger.info("채팅 워커 실행")
        await start_worker()

        # 종료 이벤트 대기
        await shutdown_event.wait()
    except Exception as e:
        logger.error(f"워커 실행 중 오류 발생: {str(e)}", exc_info=True)
        await shutdown()
    finally:
        logger.info("워커 종료")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("키보드 인터럽트로 종료")
    except Exception as e:
        logger.error(f"예기치 않은 오류: {str(e)}", exc_info=True)
        sys.exit(1)
