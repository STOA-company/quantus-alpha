import asyncio
from app.database.crud import database_service
from app.modules.user.service import UserService
from app.core.logger import setup_logger

# 로깅 설정
logger = setup_logger(__name__)


async def main():
    """모든 사용자에 대해 screener_init 함수를 실행합니다."""
    logger.info("모든 사용자에 대한 screener 초기화를 시작합니다...")

    # 사용자 서비스 초기화
    user_service = UserService()

    # 모든 사용자 가져오기
    users = database_service._select(table="alphafinder_user")
    logger.info(f"총 {len(users)}명의 사용자에 대해 screener 초기화를 수행합니다.")

    # 각 사용자에 대해 screener 초기화 실행
    for i, user in enumerate(users):
        logger.info(f"[{i+1}/{len(users)}] 사용자 ID: {user.id} - screener 초기화 중...")
        try:
            await user_service.screener_init(user_id=user.id)
            logger.info(f"[{i+1}/{len(users)}] 사용자 ID: {user.id} - screener 초기화 완료")
        except Exception as e:
            logger.error(f"[{i+1}/{len(users)}] 사용자 ID: {user.id} - screener 초기화 실패: {str(e)}")

    logger.info("모든 사용자에 대한 screener 초기화가 완료되었습니다.")


if __name__ == "__main__":
    # 비동기 이벤트 루프 실행
    asyncio.run(main())
