from fastapi import APIRouter, Depends, HTTPException
from app.modules.payments.service import PaymentService
from app.modules.payments.schema import TossPaymentReceipt
from app.models.models_users import AlphafinderUser
from app.utils.oauth_utils import get_current_user
from app.modules.payments.mapping import price_mapping

router = APIRouter()


@router.post("/toss/confirm")
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
