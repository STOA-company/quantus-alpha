from fastapi import APIRouter, Depends, HTTPException, Query

from app.models.models_users import AlphafinderUser
from app.modules.common.schemas import BaseResponse
from app.modules.payments.schema import Coupon, ResponsePayment, ResponseUserUsingHistory
from app.modules.payments.service import PaymentService
from app.utils.oauth_utils import get_current_user

router = APIRouter()


# 쿠폰 지급
@router.get("/coupon/issue", summary="쿠폰 지급 / admin 전용")
def issue_coupon(
    user_id: int = Query(..., description="지급받을 사용자 ID"),
    coupon_id: int = Query(..., description="쿠폰 ID"),
    payment_service: PaymentService = Depends(PaymentService),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    """
    쿠폰 지급 기능
    쿠폰 ID : 1 ==> 1개월권
    쿠폰 ID : 2 ==> 1년권
    """
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    if current_user.subscription_level != 9:
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")

    payment_service.issue_coupon(coupon_id, user_id)
    return BaseResponse(status_code=200, message="쿠폰 지급 성공", data=True)


# 보유한 쿠폰 확인
@router.get("/coupon/check", summary="보유한 쿠폰 확인 / admin 전용")
def check_coupon(
    user_id: int = Query(..., description="쿠폰 확인할 사용자 ID"),
    payment_service: PaymentService = Depends(PaymentService),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    if current_user.subscription_level != 9:
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")

    coupon_list = payment_service.get_coupon_list_by_user_id(user_id)
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

    return BaseResponse(status_code=200, message="쿠폰 확인 성공", data=result)


# 쿠폰 삭제
@router.get("/coupon/delete", summary="쿠폰 삭제 / admin 전용")
def delete_coupon(
    coupon_id: int = Query(..., description="지우고 싶은 쿠폰 ID"),
    payment_service: PaymentService = Depends(PaymentService),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    if current_user.subscription_level != 9:
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")

    payment_service.delete_coupon(coupon_id)
    return BaseResponse(status_code=200, message="쿠폰 삭제 성공", data=True)


# 사용 내역 확인
@router.get("/history/check", summary="사용 내역 확인 / admin 전용")
def check_history(
    user_id: int = Query(..., description="결제 내역 확인할 사용자 ID"),
    payment_service: PaymentService = Depends(PaymentService),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    if current_user.subscription_level != 9:
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")

    history_list = payment_service.get_user_using_history_by_user_id(user_id)
    result = []
    for history in history_list:
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

    return BaseResponse(status_code=200, message="사용 내역 확인 성공", data=result)


# 결제 내역 확인
@router.get("/payment/check", summary="결제 내역 확인 / admin 전용")
def check_payment(
    user_id: int = Query(..., description="결제 내역 확인할 사용자 ID"),
    payment_service: PaymentService = Depends(PaymentService),
    current_user: AlphafinderUser = Depends(get_current_user),
):
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    if current_user.subscription_level != 9:
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")

    payment_list = payment_service.get_payment_list_by_user_id(user_id)
    product_name = payment_service.get_product_name_by_level_period_days(
        payment_list[0].level, payment_list[0].period_days
    )

    result = []
    for payment in payment_list:
        result.append(
            ResponsePayment(
                payment_id=payment.id,
                product_name=product_name[0].name,
                paid_amount=payment.paid_amount,
                payment_method=payment.payment_method,
                is_refunded=payment.refund_at is not None,
                created_at=payment.created_at,
            )
        )

    return BaseResponse(status_code=200, message="결제 내역 확인 성공", data=result)


# 결제 환불
@router.post("/refund", response_model=BaseResponse[bool], summary="토스 결제 환불")
def refund_toss_payments(
    payment_id: int = Query(..., description="결제 내역 ID"),
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    if current_user is None:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    if current_user.subscription_level != 9:
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")

    is_refunded = payment_service.refund_payments(current_user, payment_id=payment_id)
    return BaseResponse(status_code=200, message="결제 환불 성공", data=is_refunded)
