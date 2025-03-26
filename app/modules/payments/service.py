import requests

from app.core.config import settings
from datetime import datetime, timedelta
from app.database.crud import database_service
from app.core.extra.LoggerBox import LoggerBox

logger = LoggerBox().get_logger(__name__)


class PaymentService:
    def __init__(self):
        self.toss_secret_key = settings.TOSS_SECRET_KEY
        self.toss_api_url = "https://api.tosspayments.com/v1"
        self.db = database_service

    def store_toss_payments_history(
        self,
        payment_key: str,
        order_id: str,
        receipt: dict,
        user_id: int,
    ):
        self.db._insert(
            table="toss_payment_history",
            sets={
                "user_id": user_id,
                "payment_key": payment_key,
                "order_id": order_id,
                "receipt": receipt,
            },
        )

    def user_subscription_update(self, user_id: int, period: int):
        subscription_end = datetime.now().date() + timedelta(days=period)
        self.db._update(
            table="alphafinder_user",
            sets={"is_subscribed": True, "subscription_end": subscription_end},
            id=user_id,
        )
        return {"subscription_end": subscription_end}

    def verify_toss_payment(self, payment_key: str, order_id: str, receipt: dict):
        """Toss API를 통해 결제 정보를 가져옵니다."""
        # Basic 인증을 위한 헤더 (Secret Key와 빈 문자열을 Base64로 인코딩)
        import base64

        auth_string = base64.b64encode(f"{self.toss_secret_key}:".encode()).decode()

        headers = {"Authorization": f"Basic {auth_string}"}
        # Toss API에 결제 정보 요청
        response = requests.get(f"{self.toss_api_url}/payments/{payment_key}", headers=headers)

        if response.status_code != 200:
            logger.error(f"Toss API 요청 실패: {response.status_code}")
            return False

        # JSON 응답 파싱
        payment_data = response.json()

        # 주문번호, 금액, 상태 검증
        if payment_data.get("orderId") != order_id:
            logger.error(f"주문번호 불일치: {payment_data.get('orderId')} != {order_id}")
            return False

        if payment_data.get("totalAmount") != receipt.get("amount"):
            logger.error(f"금액 불일치: {payment_data.get('totalAmount')} != {receipt.get('amount')}")
            return False

        if payment_data.get("status") != "DONE":
            logger.error(f"결제 상태 불일치: {payment_data.get('status')} != DONE")
            return False

        # 모든 검증 통과
        return True
