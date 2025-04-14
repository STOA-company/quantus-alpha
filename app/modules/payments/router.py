from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query

from app.batches.run_expiration import coupon_expiration, subscription_expiration
from app.core.config import settings
from app.models.models_users import AlphafinderUser
from app.modules.common.enum import TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.payments.schema import (
    Coupon,
    CouponId,
    RequestCouponNumber,
    ResponseMembership,
    ResponsePriceTemplate,
    ResponseUserUsingHistory,
    StoreCoupon,
    TradePayments,
)
from app.modules.payments.service import PaymentService
from app.utils.date_utils import now_kr
from app.utils.oauth_utils import get_current_user

router = APIRouter()


# 가격 정보
@router.get("/price_template", response_model=BaseResponse[ResponsePriceTemplate], summary="가격 정보 조회")
def get_price_template(
    payment_service: PaymentService = Depends(PaymentService),
):
    price_template = payment_service.get_price_template()
    return BaseResponse(status_code=200, message="가격 정보 조회 성공", data=price_template)


@router.post("/toss/confirm", response_model=BaseResponse[bool], summary="토스 결제 확인")
def confirm_toss_payments(
    trade_payments: TradePayments,
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    payment_key = trade_payments.payment_key
    order_id = trade_payments.order_id
    amount = trade_payments.amount
    payment_company = trade_payments.trade_company
    product_id = trade_payments.product_id

    is_confirmed = payment_service.confirm_toss_payments(
        payment_key, order_id, amount, current_user, payment_company, product_id
    )

    return BaseResponse(status_code=200, message="결제 확인 성공", data=is_confirmed)


# 멤버십 관리 페이지
@router.get("/toss/membership", summary="멤버십 관리 화면 사용권 확인", response_model=BaseResponse[ResponseMembership])
def check_toss_membership(
    lang: Optional[TranslateCountry] = Query(TranslateCountry.KO, description="언어 설정 (ko/en)"),
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    if current_user.subscription_name is None:
        data = None
    else:
        remaining_days = (current_user.subscription_end - now_kr().date()).days
        user_using_history = payment_service.get_user_using_history_by_user_id(current_user.id)
        used_days = payment_service.count_used_days(user_using_history)
        current_using_history = payment_service.get_product_type_by_user_using_history(
            user_using_history, current_user.using_history_id
        )

        if current_using_history and current_using_history.product_type == "membership":
            # Check if product_relation_id exists before calling check_is_extended
            is_extended = False
            if hasattr(current_using_history, "product_relation_id") and current_using_history.product_relation_id:
                is_extended = payment_service.check_is_extended(current_using_history.product_relation_id)

            # Get pricing info if available
            try:
                price_template = payment_service.get_price_template_by_name(current_using_history.product_name)
                product_id = price_template.id if price_template else None
                product_amount = price_template.price if price_template else None
            except Exception:
                product_id = None
                product_amount = None
        else:
            is_extended = None
            product_id = None
            product_amount = None

        data = ResponseMembership(
            name=current_using_history.product_name,
            status=current_user.is_subscribed,
            start_date=current_user.subscription_start,
            end_date=current_user.subscription_end,
            remaining_days=remaining_days,
            used_days=used_days,
            product_type=current_using_history.product_type,
            is_extended=is_extended,
            product_id=product_id,
            product_amount=product_amount,
        )
    return BaseResponse(status_code=200, message="멤버십 정보 조회 성공", data=data)


# # 멤버십 구독 취소
# @router.patch("/membership/cancel", summary="멤버십 구독 취소 / test용 / subscription_status False로 변경")
# def cancel_toss_membership(
#     current_user: AlphafinderUser = Depends(get_current_user),
#     payment_service: PaymentService = Depends(PaymentService),
# ):
#     if current_user is None:
#         raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

#     payment_service.cancel_membership(current_user.id)
#     return BaseResponse(status_code=200, message="멤버십 구독 취소 성공", data=True)


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

    result = []
    for coupon in coupon_list:
        result.append(
            Coupon(
                id=coupon.id,
                coupon_name=coupon.coupon_name,
                issued_at=coupon.issued_at.date(),
                expired_at=coupon.expired_at.date(),
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
    if settings.ENV == "stage":
        # if settings.ENV == "dev":
        today = now_kr().date()
        if today < datetime(2025, 4, 15).date():
            raise HTTPException(status_code=425, detail="쿠폰 사용 기간이 아직 안 됐습니다.")

    is_used = payment_service.use_coupon(current_user.id, coupon_id)

    return BaseResponse(status_code=200, message="쿠폰 사용 성공", data=is_used)


# # 쿠폰 사용 취소
# @router.patch("/coupon/status", summary="쿠폰 사용 취소 / 테스트용 / coupon_status inactive로 변경")
# def cancel_coupon(
#     coupon_id: int = Query(..., description="쿠폰 ID"),
#     coupon_status: str = Query("inactive", description="쿠폰 상태 (active: 사용중, inactive: 미사용, expired: 만료)"),
#     payment_service: PaymentService = Depends(PaymentService),
# ):
#     if coupon_status not in ["active", "inactive", "expired"]:
#         raise HTTPException(status_code=400, detail="coupon_status는 active 또는 inactive 또는 expired만 가능합니다.")

#     payment_service.update_coupon_status(coupon_id, coupon_status)
#     return BaseResponse(status_code=200, message="쿠폰 사용 취소 성공", data=True)


# 사용 내역 조회
@router.get("/usage_history", response_model=BaseResponse[List[ResponseUserUsingHistory]], summary="사용 내역 조회")
def get_usage_history(
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    user_using_history = payment_service.get_user_using_history_by_user_id(current_user.id)
    result = []
    for history in user_using_history:
        result.append(
            ResponseUserUsingHistory(
                history_id=history.id,
                start_date=history.start_date,
                end_date=history.end_date,
                product_name=history.product_name,
                product_type=history.product_type,
                is_refunded=history.refund_at is not None,
            )
        )
    return BaseResponse(status_code=200, message="사용 내역 조회 성공", data=result)


###############################
@router.get("/subscription/expiration", summary="구독 만료 처리")
def run_subscription_expiration(x_api_key: str = Header(None)):
    if x_api_key != "GoNVDA2xToTheMoon!":
        raise HTTPException(status_code=403, detail="Unauthorized access")
    subscription_expiration()
    return BaseResponse(status_code=200, message="success", data=True)


@router.get("/coupon/expiration", summary="쿠폰 만료 처리")
def run_coupon_expiration(x_api_key: str = Header(None)):
    if x_api_key != "GoNVDA2xToTheMoon!":
        raise HTTPException(status_code=403, detail="Unauthorized access")
    coupon_expiration()
    return BaseResponse(status_code=200, message="success", data=True)
