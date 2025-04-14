from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class PaymentReceipt(BaseModel):
    type: str
    receipt: dict


class TradePayments(BaseModel):
    trade_company: str = Field(description="toss", example="toss")
    amount: int = Field(description="결제 금액", example=1000)
    order_id: str = Field(description="주문 번호", example="a4CWyWY5m89PNh7xJwhk1")
    payment_key: str = Field(description="결제 키", example="5EnNZRJGvaBX7zk2yd8ydw26XvwXkLrx9POLqKQjmAw4b0e1")
    product_id: int = Field(description="상품 아이디", example=1)

    class Config:
        schema_extra = {
            "example": {
                "trade_company": "toss",
                "amount": 1000,
                "order_id": "a4CWyWY5m89PNh7xJwhk1",
                "payment_key": "5EnNZRJGvaBX7zk2yd8ydw26XvwXkLrx9POLqKQjmAw4b0e1",
                "product_id": 1,
            }
        }


class ResponseMembership(BaseModel):
    name: str
    status: bool
    start_date: datetime
    end_date: datetime
    remaining_days: int
    used_days: int
    product_type: str
    is_extended: Optional[bool] = None
    product_id: Optional[int] = None
    product_amount: Optional[int] = None


class RequestCouponNumber(BaseModel):
    coupon_number: str


class Coupon(BaseModel):
    id: int
    coupon_name: str
    issued_at: date
    expired_at: date
    coupon_status: str


class StorePaymentsHistory(BaseModel):
    receipt_id: int
    user_id: int
    level: int
    period_days: int
    paid_amount: int
    payment_method: str
    payment_company: str
    refund_at: Optional[str] = None


class StoreCoupon(BaseModel):
    user_id: int
    coupon_name: str
    coupon_id: Optional[int] = None
    issued_at: datetime
    expired_at: datetime
    coupon_status: Optional[str] = None


class CouponId(BaseModel):
    coupon_id: int


class PriceTemplateItem(BaseModel):
    id: int = Field(description="가격 템플릿의 고유 식별자")
    name: str = Field(description="가격 템플릿의 이름")
    original_price: int = Field(description="원래 가격 (원 단위)")
    event_price: Optional[int] = Field(default=None, description="이벤트 가격 (원 단위), 설정하지 않으면 None")
    period_days: int = Field(description="구독 기간 (일 단위)")
    is_event: bool = Field(description="이벤트 배너 표시 여부")
    event_type: Optional[str] = Field(default=None, description="이벤트 타입")
    event_end_date: Optional[int] = Field(default=None, description="이벤트 종료까지 남은 기간")


class ResponsePriceTemplate(BaseModel):
    is_banner: bool = Field(description="배너 표시 여부")
    banner: dict = Field(description="배너 정보")
    price_template: list[PriceTemplateItem] = Field(description="가격 템플릿 아이템 리스트")


class UpdateUserSubscription(BaseModel):
    is_subscribed: Optional[bool] = None
    subscription_start: Optional[datetime] = None
    subscription_end: datetime
    recent_payment_date: datetime
    subscription_level: Optional[int] = None
    subscription_name: Optional[str] = None
    using_history_id: Optional[int] = None


class StoreUserUsingHistory(BaseModel):
    user_id: int
    start_date: datetime
    end_date: datetime
    product_name: str
    product_type: str
    product_relation_id: Optional[int] = None


class ResponseUserUsingHistory(BaseModel):
    history_id: int
    start_date: datetime
    end_date: datetime
    product_name: str
    product_type: str
    is_refunded: bool


class ResponsePayment(BaseModel):
    payment_id: int
    product_name: str
    paid_amount: int
    payment_method: str
    # payment_company: str
    is_refunded: bool
    created_at: datetime


# Server is listening...

# 3/26/2025, 4:44:25 PM

# POST: /v1/payments/confirm

# 3/26/2025, 4:44:36 PM

# {"mId":"tgen_docs","lastTransactionKey":"txrd_a01jq8pj1npp3axzqyjb793vrww","paymentKey":"tgen_20250326164402D0r26","orderId":"MC4xNDc3MTM2NjQwNTMw","orderName":"토스 티셔츠 외 2건","taxExemptionAmount":0,"status":"DONE","requestedAt":"2025-03-26T16:44:02+09:00","approvedAt":"2025-03-26T16:44:35+09:00","useEscrow":false,"cultureExpense":false,"card":null,"virtualAccount":null,"transfer":null,"mobilePhone":null,"giftCertificate":null,"cashReceipt":null,"cashReceipts":null,"discount":null,"cancels":null,"secret":"ps_yL0qZ4G1VOyeKaOJ1dO8oWb2MQYg","type":"NORMAL","easyPay":{"provider":"카카오페이","amount":50000,"discountAmount":0},"country":"KR","failure":null,"isPartialCancelable":true,"receipt":{"url":"https://dashboard.tosspayments.com/receipt/redirection?transactionId=tgen_20250326164402D0r26&ref=PX"},"checkout":{"url":"https://api.tosspayments.com/v1/payments/tgen_20250326164402D0r26/checkout"},"currency":"KRW","totalAmount":50000,"balanceAmount":50000,"suppliedAmount":45455,"vat":4545,"taxFreeAmount":0,"method":"간편결제","version":"2022-11-16","metadata":null}
