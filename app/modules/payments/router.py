from typing import List
from fastapi import APIRouter, Depends, HTTPException
from app.modules.common.schemas import BaseResponse
from app.modules.payments.service import PaymentService
from app.modules.payments.schema import Coupon, RequestCouponNumber, ResponseMembership, TossPaymentReceipt
from app.models.models_users import AlphafinderUser
from app.utils.oauth_utils import get_current_user
from app.modules.payments.mapping import price_mapping

router = APIRouter()


@router.post("/toss/confirm", response_model=BaseResponse[bool], summary="토스 결제 확인")
def confirm_toss_payments(
    toss_payment_receipt: TossPaymentReceipt,
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    payment_key = toss_payment_receipt.payment_key
    order_id = toss_payment_receipt.order_id
    amount = toss_payment_receipt.amount
    if current_user is None:
        raise HTTPException(status_code=400, detail="로그인이 필요합니다.")
    else:
        user_id = current_user.id
        email = current_user.email

    is_verified = payment_service.verify_toss_payment(payment_key, order_id, amount)
    if not is_verified:
        raise HTTPException(status_code=400, detail="결제 정보 검증에 실패했습니다.")

    period = price_mapping.get(amount, "")
    if period == "":
        raise HTTPException(status_code=400, detail="결제 금액이 올바르지 않습니다.")

    payment_service.store_toss_payments_history(
        payment_key=payment_key,
        order_id=order_id,
        amount=amount,
        user_id=user_id,
        email=email,
    )

    subscription_end = payment_service.user_subscription_update(user_id, period)
    return {"subscription_end": subscription_end}


# 멤버십 관리 페이지
@router.post("/toss/membership", summary="멤버십 관리 화면 사용권 확인", response_model=ResponseMembership)
def check_toss_membership(
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    pass


# 쿠폰함 페이지
## 쿠폰 확인
@router.get("/coupon", response_model=BaseResponse[List[Coupon]], summary="보유중인 쿠폰 확인")
def check_coupon(
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    coupon_list = payment_service.get_coupon_list_by_user_id(current_user.id)
    result = [Coupon(**coupon) for coupon in coupon_list]
    return result


## 쿠폰 등록
@router.post("/coupon/register", response_model=BaseResponse[bool], summary="쿠폰 등록")
def register_coupon(
    coupon_number: RequestCouponNumber,
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    coupon_number = coupon_number.coupon_number
    is_registered = payment_service.register_coupon(current_user.id, coupon_number)

    return BaseResponse(data=is_registered)


# 쿠폰 사용
@router.post("/coupon/use", response_model=BaseResponse[bool], summary="쿠폰 사용")
def use_coupon(
    current_user: AlphafinderUser = Depends(get_current_user),
):
    pass


# 쿠폰 사용 취소
@router.patch("/coupon/cancel", response_model=BaseResponse[bool], summary="쿠폰 사용 취소")
def cancel_coupon(
    current_user: AlphafinderUser = Depends(get_current_user),
):
    if current_user is None:
        raise HTTPException(status_code=400, detail="로그인이 필요합니다.")


@router.post("/toss/refund", response_model=BaseResponse[bool], summary="토스 결제 환불")
def refund_toss_payments(
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    pass
