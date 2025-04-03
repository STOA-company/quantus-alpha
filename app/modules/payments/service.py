import base64
import json
from typing import List
from fastapi import HTTPException
import requests

from app.core.config import settings
from datetime import timedelta
from app.database.crud import JoinInfo, database_service
from app.core.extra.LoggerBox import LoggerBox
from http.client import HTTPSConnection as https_conn

from app.models.models_users import AlphafinderUser
from app.modules.payments.schema import (
    PriceTemplate,
    StoreCoupon,
    StorePaymentsHistory,
    StoreUserUsingHistory,
    UpdateUserSubscription,
)
from app.utils.date_utils import now_kr

logger = LoggerBox().get_logger(__name__)


class PaymentService:
    def __init__(self):
        self.toss_secret_key = settings.TOSS_SECRET_KEY
        self.toss_api_url = "https://api.tosspayments.com/v1"
        self.db = database_service

    def get_price_template(self) -> List[PriceTemplate]:
        data = self.get_price()
        price_template = []
        for price in data:
            price_template.append(
                PriceTemplate(
                    id=price.id,
                    name=price.name,
                    original_price=price.price,
                    event_price=price.event_price,
                    period_days=price.period_days,
                )
            )
        return price_template

    def get_price(self):
        data = self.db._select(
            table="alphafinder_price",
            columns=["id", "name", "price", "event_price", "period_days"],
        )
        return data

    def confirm_toss_payments(
        self,
        payment_key: str,
        order_id: str,
        amount: int,
        current_user: AlphafinderUser,
        payment_company: str,
        product_id: int,
    ) -> bool:
        ## toss payments request
        response = self.request_confirm_toss_payments(payment_key, order_id, amount)
        if response is None or "code" in response:
            error_msg = response.get("message", "Unknown error") if response else "No response"
            logger.error(f"결제 승인 실패: {payment_key}, {order_id}, {amount}, 오류: {error_msg}")
            raise Exception(f"결제 승인 실패: {payment_key}, {order_id}, {amount}, 오류: {error_msg}")

        try:
            ## 결제 영수증 저장
            receipt_id = self.store_toss_receipt(payment_key, order_id, response)
        except Exception as e:
            logger.error(f"결제 처리 중 오류 발생: {e}")
            self.cancel_toss_payments(payment_key, "결제 영수증 처리 중 오류 발생")
            raise e

        user_id = current_user.id
        # try:
        #     ## 결제 정보 확인
        #     payment_data = self.verify_toss_payment(payment_key, order_id, amount)
        #     if not payment_data:
        #         logger.error(f"결제 정보 확인 실패: payment_key={payment_key}, order_id={order_id}, amount={amount}")
        #         raise Exception(f"결제 정보 확인 실패: payment_key={payment_key}, order_id={order_id}, amount={amount}")
        # except Exception as e:
        #     logger.error(f"결제 정보 확인 중 오류 발생: {e}")
        #     self.cancel_toss_payments(payment_key, "결제 정보 확인 중 오류 발생")
        #     raise e

        try:
            ## 결제 정보 저장
            product_name, level, period_days, product_type = self.get_level_and_period_days_product_type(product_id)
            store_payments_history = StorePaymentsHistory(
                receipt_id=receipt_id,
                user_id=user_id,
                level=level,
                period_days=period_days,
                paid_amount=amount,
                payment_method=response.get("method"),
                payment_company=payment_company,
            )

            payment_history_id = self.store_payments_history(store_payments_history)
        except Exception as e:
            logger.error(f"결제 정보 저장 중 오류 발생: {e}")
            self.cancel_toss_payments(payment_key, "결제 정보 저장 중 오류 발생")
            raise e

        try:
            # 쿠폰 저장 또는 멤버십 시작
            if product_type == "membership":
                if current_user.is_subscribed:
                    user_using_history_id = self.store_user_using_history(
                        StoreUserUsingHistory(
                            user_id=user_id,
                            start_date=current_user.subscription_end + timedelta(days=1),
                            end_date=current_user.subscription_end + timedelta(days=(period_days + 1)),
                            product_name=product_name,
                            product_type=product_type,
                            product_relation_id=payment_history_id,
                        )
                    )
                else:
                    user_using_history_id = self.store_user_using_history(
                        StoreUserUsingHistory(
                            user_id=user_id,
                            start_date=now_kr().date(),
                            end_date=now_kr().date() + timedelta(days=period_days),
                            product_name=product_name,
                            product_type=product_type,
                            product_relation_id=payment_history_id,
                        )
                    )
                self.user_subscription_update(user_id, period_days, level, product_name, user_using_history_id)

            elif product_type == "coupon":
                self.store_coupon(
                    StoreCoupon(
                        user_id=user_id,
                        coupon_name=product_name,
                        issued_at=now_kr().date(),
                        expired_at=now_kr().date() + timedelta(days=365),
                        coupon_status="inactive",
                    )
                )
        except Exception as e:
            logger.error(f"결제 처리 중 오류 발생: {e}")
            self.cancel_toss_payments(payment_key, "결제 처리 중 오류 발생")
            raise e

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

    def get_level_and_period_days_product_type(self, product_id: int):
        data = self.db._select(
            table="alphafinder_price",
            columns=["name", "level", "period_days", "price_type"],
            id=product_id,
            is_active=True,
        )
        if data:
            product_name = data[0].name
            level = data[0].level
            period_days = data[0].period_days
            product_type = data[0].price_type
        else:
            logger.error(f"상품 정보 조회 실패: {product_id}")
            raise Exception(f"상품 정보 조회 실패: {product_id}")
        return product_name, level, period_days, product_type

    def store_payments_history(
        self,
        store_payments_history: StorePaymentsHistory,
    ):
        data = self.db._insert(
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
        return data.lastrowid

    def user_subscription_update(
        self, user_id: int, period: int, level: int, product_name: str, user_using_history_id: int
    ):
        data = self.db._select(
            table="alphafinder_user",
            columns=["is_subscribed", "subscription_end", "subscription_start", "using_history_id"],
            id=user_id,
        )
        if not data:
            raise Exception(f"유저 정보 조회 실패: {user_id}")
        is_subscribed = data[0].is_subscribed
        subscription_end = data[0].subscription_end
        subscription_start = data[0].subscription_start
        using_history_id = data[0].using_history_id
        if is_subscribed:
            update_user_subscription = UpdateUserSubscription(
                is_subscribed=True,
                subscription_start=subscription_start,
                subscription_end=subscription_end + timedelta(days=(period + 1)),
                recent_payment_date=now_kr().date(),
                subscription_level=level,
                subscription_name=product_name,
                using_history_id=using_history_id,
            )
            self.update_is_extended(using_history_id)
        else:
            update_user_subscription = UpdateUserSubscription(
                is_subscribed=True,
                subscription_start=now_kr().date(),
                subscription_end=now_kr().date() + timedelta(days=period),
                recent_payment_date=now_kr().date(),
                subscription_level=level,
                subscription_name=product_name,
                using_history_id=user_using_history_id,
            )
        self.update_subscription(user_id, update_user_subscription)

        return {"subscription_end": subscription_end}

    def store_user_using_history(self, user_using_history: StoreUserUsingHistory):
        sets = {
            "user_id": user_using_history.user_id,
            "start_date": user_using_history.start_date,
            "end_date": user_using_history.end_date,
            "product_name": user_using_history.product_name,
            "product_type": user_using_history.product_type,
        }

        # Add product_relation_id if it's provided
        if user_using_history.product_relation_id is not None:
            sets["product_relation_id"] = user_using_history.product_relation_id

        data = self.db._insert(
            table="user_using_history",
            sets=sets,
        )
        return data.lastrowid

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

    def cancel_toss_payments(self, payment_key: str, cancel_reason: str):
        auth_string = base64.b64encode(f"{self.toss_secret_key}:".encode()).decode()
        headers = {"Authorization": f"Basic {auth_string}", "Content-Type": "application/json"}
        payload = json.dumps({"cancelReason": cancel_reason})
        response = requests.post(f"{self.toss_api_url}/payments/{payment_key}/cancel", headers=headers, data=payload)
        if response.status_code != 200:
            logger.error(f"Toss API 요청 실패: {response.status_code}")
            return False
        return True

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
            order="issued_at",
            ascending=True,
            user_id=user_id,
            coupon_status__in=["inactive", "expired"],
        )
        return coupon_list

    # def get_coupon_by_coupon_id(self, coupon_id: int):
    #     coupon_list = self.db._select(
    #         table="alphafinder_coupon_box",
    #         columns=["id", "coupon_name", "issued_at", "expired_at", "coupon_status"],
    #         order="issued_at",
    #         ascending=False,
    #         user_id=user_id,
    #         coupon_status__in=["inactive", "expired"],
    #     )
    #     return coupon_list

    def check_coupon_used(self, coupon_number: str, user_id: int):
        """
        쿠폰 번호가 이미 사용되었는지 확인합니다.

        Args:
            coupon_number: 확인할 쿠폰 번호

        Returns:
            bool: 이미 사용된 쿠폰이면 True, 그렇지 않으면 False
        """

        # alphafinder_coupon과 alphafinder_coupon_box 테이블을 조인하여 쿠폰 사용 여부 확인
        join_info = JoinInfo(
            primary_table="alphafinder_coupon",
            secondary_table="alphafinder_coupon_box",
            primary_column="id",
            secondary_column="coupon_id",
            columns=["coupon_status"],  # coupon_box 테이블의 컬럼
            is_outer=False,  # INNER JOIN 사용
            secondary_condition={"user_id": user_id},
        )

        data = self.db._select(
            table="alphafinder_coupon",
            columns=["id", "coupon_num"],
            join_info=join_info,
            coupon_num=coupon_number,  # 쿠폰 번호로 필터링
        )

        # 데이터가 있으면(쿠폰이 이미 등록되어 있으면) True 반환
        return len(data) > 0

    def check_coupon_number(
        self,
        coupon_number: str,
    ):
        """
        유효한 쿠폰 번호인지 확인합니다.

        Args:
            coupon_number: 확인할 쿠폰 번호

        Returns:
            Row | False: 유효한 쿠폰이면 쿠폰 정보가 담긴 Row 객체, 그렇지 않으면 False
        """
        condition = {"coupon_num": coupon_number, "is_active": True}
        data = self.db._select(
            table="alphafinder_coupon",
            columns=["id", "coupon_name", "coupon_period_days", "expired_at"],
            **condition,
        )
        if not data:
            return False
        return data[0]

    def use_coupon(self, user_id: int, coupon_id: int) -> bool:
        # 이미 사용중인 멤버십이 있는지 확인
        data = self.get_membership(user_id)
        if data[0].is_subscribed:
            raise HTTPException(status_code=409, detail="이미 사용중인 멤버십이 있습니다.")

        # 쿠폰이 사용한 상태인지 확인
        coupon_data = self.get_coupon_by_coupon_id(coupon_id)
        if coupon_data.coupon_status == "active":
            raise HTTPException(status_code=422, detail="이미 사용중인 쿠폰입니다.")
        elif coupon_data.coupon_status == "expired":
            raise HTTPException(status_code=410, detail="쿠폰의 유효기간이 지났습니다.")

        if coupon_data.user_id != user_id:
            raise HTTPException(status_code=403, detail="쿠폰을 사용할 수 없습니다.")

        # 쿠폰 사용
        self.update_coupon_status(coupon_id, "active")

        # 유저 사용 이력 업데이트
        if data[0].is_subscribed:
            pass
        else:
            user_using_history_id = self.store_user_using_history(
                StoreUserUsingHistory(
                    user_id=user_id,
                    start_date=now_kr().date(),
                    end_date=now_kr().date() + timedelta(days=coupon_data.period_days),
                    product_name=coupon_data.coupon_name,
                    product_type="coupon",
                )
            )

        # 유저 정보 업데이트
        self.user_subscription_update(
            user_id, coupon_data.period_days, coupon_data.level, coupon_data.coupon_name, user_using_history_id
        )

        return True

    def update_coupon_status(self, coupon_id: int, coupon_status: str):
        self.db._update(
            table="alphafinder_coupon_box",
            sets={"coupon_status": coupon_status},
            id=coupon_id,
        )

    def get_coupon_by_coupon_id(self, coupon_id: int):
        join_info = JoinInfo(
            primary_table="alphafinder_coupon_box",
            secondary_table="alphafinder_price",
            primary_column="coupon_name",
            secondary_column="name",
            columns=["period_days", "level"],
        )

        data = self.db._select(
            table="alphafinder_coupon_box",
            columns=["id", "user_id", "coupon_name", "issued_at", "expired_at", "coupon_status", "period_days", "level"],
            id=coupon_id,
            join_info=join_info,
        )
        return data[0]

    #####################################
    def cancel_membership(self, user_id: int):
        self.db._update(
            table="alphafinder_user",
            sets={
                "is_subscribed": False,
                "subscription_end": None,
                "subscription_start": None,
                "recent_payment_date": None,
                "subscription_level": 1,
                "subscription_name": None,
                "using_history_id": None,
            },
            id=user_id,
        )

    def get_user_using_history_by_user_id(self, user_id: int):
        data = self.db._select(
            table="user_using_history",
            columns=["id", "start_date", "end_date", "product_name", "product_type", "product_relation_id", "refund_at"],
            user_id=user_id,
        )
        return data

    def count_used_days(self, user_using_history):
        """
        사용자의 서비스 사용 일수를 계산합니다.

        Args:
            user_using_history: 사용자의 서비스 사용 이력 목록

        Returns:
            int: 총 사용 일수
        """
        used_days = 0
        for history in user_using_history:
            # 시작일
            start_date = history.start_date.date()

            # 종료일 계산 (환불된 경우와 아닌 경우 구분)
            if history.refund_at is None:
                # 환불되지 않은 경우
                # 구독 종료일이 현재보다 미래라면 현재까지만 계산
                if history.end_date.date() > now_kr().date():
                    end_date = now_kr().date()
                else:
                    end_date = history.end_date.date()
            else:
                # 환불된 경우 환불 날짜까지만 계산
                end_date = history.refund_at.date()

            # 사용 일수 계산 (종료일 포함하여 계산하므로 +1)
            days = (end_date - start_date).days + 1

            # 음수가 나오지 않도록 보정
            used_days += max(0, days)

        return used_days

    def get_product_type_by_user_using_history(self, user_using_history, user_subscription_history_id: int):
        for history in user_using_history:
            if history.id == user_subscription_history_id:
                return history
        return None

    def get_product_type_by_user_using_history_id(self, user_using_history_id: int):
        data = self.db._select(
            table="user_using_history",
            columns=["product_type"],
            id=user_using_history_id,
        )
        return data[0].product_type

    def check_is_extended(self, product_relation_id: int):
        data = self.db._select(
            table="alphafinder_payment_history",
            columns=["is_extended"],
            id=product_relation_id,
        )
        # Handle case when no data is found
        if not data:
            return False
        return data[0].is_extended

    def get_price_template_by_name(self, subscription_name: str):
        data = self.db._select(
            table="alphafinder_price",
            columns=["id", "level", "period_days", "price", "event_price"],
            name=subscription_name,
        )
        return data[0]

    def refund_payments(self, current_user: AlphafinderUser, using_history_id: int):
        ####
        # 사용 내역 확인하는 로직 추가하기
        ###

        #
        payment_history = self.db._select(
            table="alphafinder_payment_history",
            columns=["receipt_id"],
            id=using_history_id,
        )
        if payment_history[0].refund_at is not None:
            raise HTTPException(status_code=422, detail="이미 환불된 결제입니다.")
        elif payment_history is None:
            raise HTTPException(status_code=404, detail="결제 내역이 없습니다.")

        receipt_id = payment_history[0].receipt_id

        if payment_history[0].payment_company == "toss":
            # 토스 결제 환불
            payment_key = self.db._select(
                table="toss_receipt",
                columns=["payment_key"],
                id=receipt_id,
            )
            self.cancel_toss_payments(payment_key=payment_key[0].payment_key, cancel_reason="환불 요청")

        elif payment_history[0].payment_company == "kakao":
            # 카카오 결제 환불
            pass

        self.db._update(
            table="user_using_history",
            sets={"refund_at": now_kr()},
            id=using_history_id,
        )

        if current_user.subscription_end > now_kr().date():
            self.db._update(
                table="alphafinder_user",
                sets={
                    "is_subscribed": False,
                    "subscription_end": now_kr().date(),
                    "subscription_level": 1,
                },
                id=current_user.id,
            )

        return True

    def update_is_extended(self, user_using_history_id: int):
        payment_history_id = self.db._select(
            table="user_using_history",
            columns=["product_relation_id"],
            id=user_using_history_id,
        )
        self.db._update(
            table="alphafinder_payment_history",
            sets={"is_extended": True},
            id=payment_history_id[0].product_relation_id,
        )
