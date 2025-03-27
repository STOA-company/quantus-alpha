import base64
import json
import requests

from app.core.config import settings
from datetime import datetime, timedelta
from app.database.crud import database_service
from app.core.extra.LoggerBox import LoggerBox
from http.client import HTTPSConnection as https_conn

from app.modules.payments.schema import StoreCoupon, StorePaymentsHistory, UpdateUserSubscription
from app.utils.date_utils import now_kr

logger = LoggerBox().get_logger(__name__)


class PaymentService:
    def __init__(self):
        self.toss_secret_key = settings.TOSS_SECRET_KEY
        self.toss_payment_key = settings.TOSS_PAYMENT_KEY
        self.toss_api_url = "https://api.tosspayments.com/v1"
        self.db = database_service

    def confirm_toss_payments(
        self, payment_key: str, order_id: str, amount: int, user_id: int, payment_company: str, product_type: str
    ) -> bool:
        ## toss payments request
        response = self.request_confirm_toss_payments(payment_key, order_id, amount)
        if response is None or "code" in response:
            error_msg = response.get("message", "Unknown error") if response else "No response"
            logger.error(f"결제 승인 실패: {payment_key}, {order_id}, {amount}, 오류: {error_msg}")
            raise Exception(f"결제 승인 실패: {payment_key}, {order_id}, {amount}, 오류: {error_msg}")

        ## 결제 영수증 저장
        receipt_id = self.store_toss_receipt(payment_key, order_id, response)

        ## 결제 정보 확인
        payment_data = self.verify_toss_payment(payment_key, order_id, amount)
        if not payment_data:
            logger.error(f"결제 정보 확인 실패: payment_key={payment_key}, order_id={order_id}, amount={amount}")
            raise Exception(f"결제 정보 확인 실패: payment_key={payment_key}, order_id={order_id}, amount={amount}")

        product_name = payment_data.get("orderName")
        product_price = payment_data.get("totalAmount")

        ## 결제 정보 저장
        level, period_days = self.get_level_and_period_days(product_name, product_price, product_type)
        store_payments_history = StorePaymentsHistory(
            receipt_id=receipt_id,
            user_id=user_id,
            level=level,
            period_days=period_days,
            paid_amount=amount,
            payment_method=response.get("paymentMethod"),
            payment_company=payment_company,
        )

        self.store_payments_history(store_payments_history)

        # 쿠폰 저장 또는 멤버십 시작
        if product_type == "membership":
            self.user_subscription_update(user_id, period_days, level, product_name)
        elif product_type == "coupon":
            self.store_coupon(
                StoreCoupon(
                    user_id=user_id,
                    coupon_name=product_name,
                    issued_at=now_kr().date(),
                    expired_at=now_kr().date() + timedelta(days=period_days),
                )
            )

        return True

    def request_confirm_toss_payments(self, payment_key: str, order_id: str, amount: int):
        ## toss payments request
        try:
            payload = json.dumps(
                {
                    "paymentKey": payment_key,
                    "orderId": order_id,
                    "amount": amount,
                }
            )
            auth_string = base64.b64encode(f"{self.toss_secret_key}:".encode()).decode()
            conn = https_conn("api.tosspayments.com")
            headers = {"Authorization": f"Basic {auth_string}", "Content-Type": "application/json"}
            conn.request("POST", "/v1/payments/confirm", payload, headers)
            json_str = conn.getresponse().read().decode("utf-8")
            json_data = json.loads(json_str)
            return json_data
        except Exception as e:
            logger.error(f"Toss API 요청 실패: {e}")
            raise e

    def store_toss_receipt(
        self,
        payment_key: str,
        order_id: str,
        receipt: dict,
    ):
        result = self.db._insert(
            table="toss_receipt",
            sets={
                "payment_key": payment_key,
                "order_id": order_id,
                "receipt": receipt,
            },
        )
        receipt_id = result.lastrowid

        return receipt_id

    def get_level_and_period_days(self, product_name: str, product_price: int, product_type: str):
        data = self.db._select(
            table="alphafinder_price",
            columns=["level", "period_days"],
            name=product_name,
            price=product_price,
            price_type=product_type,
            is_active=True,
        )
        if data:
            level = data[0].level
            period_days = data[0].period_days
        else:
            logger.error(f"상품 정보 조회 실패: {product_name}, {product_price}, {product_type}")
            raise Exception(f"상품 정보 조회 실패: {product_name}, {product_price}, {product_type}")
        return level, period_days

    def store_payments_history(
        self,
        store_payments_history: StorePaymentsHistory,
    ):
        self.db._insert(
            table="alphafinder_payment_history",
            sets={
                "receipt_id": store_payments_history.receipt_id,
                "user_id": store_payments_history.user_id,
                "level": store_payments_history.level,
                "period_days": store_payments_history.period_days,
                "paid_amount": store_payments_history.paid_amount,
                "payment_method": store_payments_history.payment_method,
                "payment_company": store_payments_history.payment_company,
            },
        )

    def user_subscription_update(self, user_id: int, period: int, level: int, product_name: str):
        data = self.db._select(
            table="alphafinder_user",
            columns=["is_subscribed", "subscription_end"],
            id=user_id,
        )
        if not data:
            raise Exception(f"유저 정보 조회 실패: {user_id}")
        is_subscribed = data[0].is_subscribed
        subscription_end = data[0].subscription_end

        if is_subscribed and subscription_end <= now_kr().date():
            update_user_subscription = UpdateUserSubscription(
                subscription_end=subscription_end + timedelta(days=period),
                recent_payment_date=now_kr().date(),
                subscription_level=level,
            )
            self.update_subscription(user_id, update_user_subscription)
        else:
            update_user_subscription = UpdateUserSubscription(
                is_subscribed=True,
                subscription_start=now_kr().date(),
                subscription_end=now_kr().date() + timedelta(days=period),
                recent_payment_date=now_kr().date(),
                subscription_level=level,
                product_name=product_name,
            )
            self.update_subscription(user_id, update_user_subscription)

        return {"subscription_end": subscription_end}

    def update_subscription(self, user_id: int, update_user_subscription: UpdateUserSubscription):
        self.db._update(
            table="alphafinder_user",
            sets={**update_user_subscription.model_dump()},
            id=user_id,
        )

    def store_coupon(self, coupon: StoreCoupon):
        self.db._insert(
            table="alphafinder_coupon_box",
            sets={**coupon.model_dump()},
        )

    def verify_toss_payment(self, payment_key: str, order_id: str, amount: int):
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

        if payment_data.get("totalAmount") != amount:
            logger.error(f"금액 불일치: {payment_data.get('totalAmount')} != {amount}")
            return False

        if payment_data.get("status") != "DONE":
            logger.error(f"결제 상태 불일치: {payment_data.get('status')} != DONE")
            return False

        # 모든 검증 통과
        return payment_data

    def get_membership(self, user_id: int):
        data = self.db._select(
            table="alphafinder_user",
            columns=["is_subscribed", "subscription_end"],
            id=user_id,
        )
        if not data:
            raise Exception(f"유저 정보 조회 실패: {user_id}")
        return data

    def get_coupon_list_by_user_id(self, user_id: int):
        coupon_list = self.db._select(
            table="alphafinder_coupon_box",
            columns=["id", "coupon_name", "issued_at", "expired_at", "coupon_status"],
            user_id=user_id,
            coupon_status__in=["inactive", "expired"],
            expired_at__gte=datetime.now().date(),
        )
        return coupon_list

    def check_coupon_number(self, coupon_number: str):
        data = self.db._select(
            table="alphafinder_coupon",
            columns=["id", "coupon_name", "coupon_period_days"],
            coupon_num=coupon_number,
            is_active=True,
        )
        if not data:
            return False
        return data[0]
