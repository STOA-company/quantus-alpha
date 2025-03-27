from datetime import timedelta
from typing import List
from fastapi import APIRouter, Depends, HTTPException
from app.modules.common.schemas import BaseResponse
from app.modules.payments.service import PaymentService
from app.modules.payments.schema import (
    Coupon,
    RequestCouponNumber,
    ResponseMembership,
    StoreCoupon,
    TradePayments,
    CouponId,
)
from app.models.models_users import AlphafinderUser
from app.utils.date_utils import now_kr
from app.utils.oauth_utils import get_current_user

router = APIRouter()


@router.post("/toss/confirm", response_model=BaseResponse[bool], summary="토스 결제 확인")
def confirm_toss_payments(
    trade_payments: TradePayments,
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    else:
        user_id = current_user.id

    payment_key = trade_payments.payment_key
    order_id = trade_payments.order_id
    amount = trade_payments.amount
    payment_company = trade_payments.trade_company
    product_type = trade_payments.product_type

    is_confirmed = payment_service.confirm_toss_payments(
        payment_key, order_id, amount, user_id, payment_company, product_type
    )

    return BaseResponse(status_code=200, message="결제 확인 성공", data=is_confirmed)


# 멤버십 관리 페이지
@router.get("/toss/membership", summary="멤버십 관리 화면 사용권 확인", response_model=BaseResponse[ResponseMembership])
def check_toss_membership(
    current_user: AlphafinderUser = Depends(get_current_user),
):
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    remaining_days = (current_user.subscription_end - now_kr().date()).days
    data = ResponseMembership(
        name=current_user.subscription_name,
        status=current_user.is_subscribed,
        start_date=current_user.subscription_start,
        end_date=current_user.subscription_end,
        remaining_days=remaining_days,
        # used_days=0,
    )
    return BaseResponse(status_code=200, message="멤버십 정보 조회 성공", data=data)


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

    # 튜플 결과를 명시적으로 Coupon 모델에 맞게 변환
    result = []
    for coupon in coupon_list:
        # coupon은 (id, coupon_name, issued_at, expired_at, coupon_status) 형태의 튜플
        result.append(
            Coupon(
                id=coupon.id,
                coupon_name=coupon.coupon_name,
                issued_at=coupon.issued_at.date(),  # datetime -> date 변환
                expired_at=coupon.expired_at.date(),  # datetime -> date 변환
                coupon_status=coupon.coupon_status,
            )
        )

    return BaseResponse(status_code=200, message="쿠폰 목록 조회 성공", data=result)


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
    user_id = current_user.id
    # 사용한 쿠폰인지 확인
    is_used = payment_service.check_coupon_used(coupon_number, user_id)
    if is_used:
        raise HTTPException(status_code=409, detail="이미 사용한 쿠폰입니다.")

    # 유효한 쿠폰인지 확인
    coupon_data = payment_service.check_coupon_number(coupon_number)
    if not coupon_data:
        raise HTTPException(status_code=404, detail="쿠폰 번호가 유효하지 않습니다.")

    # 쿠폰의 유효기간이 지났는지 확인
    if coupon_data.expired_at.date() < now_kr().date():
        raise HTTPException(status_code=410, detail="쿠폰의 유효기간이 지났습니다.")

    # 쿠폰 등록
    payment_service.store_coupon(
        StoreCoupon(
            user_id=user_id,
            coupon_name=coupon_data.coupon_name,
            coupon_id=coupon_data.id,
            issued_at=now_kr().date(),
            expired_at=now_kr().date() + timedelta(days=365),
            coupon_status="inactive",
        )
    )

    return BaseResponse(status_code=200, message="쿠폰 등록 성공", data=True)


# 쿠폰 사용
@router.post("/coupon/use", response_model=BaseResponse[bool], summary="쿠폰 사용")
def use_coupon(
    coupon_id: CouponId,
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    if current_user is None:
        raise HTTPException(status_code=400, detail="로그인이 필요합니다.")
    coupon_id = coupon_id.coupon_id
    is_used = payment_service.use_coupon(current_user.id, coupon_id)

    return BaseResponse(status_code=200, message="쿠폰 사용 성공", data=is_used)


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
