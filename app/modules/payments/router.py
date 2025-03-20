from fastapi import APIRouter, Depends
from app.modules.payments.service import PaymentService
from app.modules.payments.schema import TossPaymentReceipt
from app.models.models_users import AlphafinderUser
from app.utils.oauth_utils import get_current_user

router = APIRouter()


@router.post("/toss/confirm")
def confirm_toss_payments(
    toss_payment_receipt: TossPaymentReceipt,
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    payment_service.store_toss_payments_history(
        toss_payment_receipt.payment_key,
        toss_payment_receipt.order_id,
        toss_payment_receipt.amount,
        current_user.id,
        current_user.email,
    )

    amount = toss_payment_receipt.amount
    period = 365
    if amount == 19000:
        period = 30
    subscription_end = payment_service.user_subscription_update(current_user.id, period)
    return {"subscription_end": subscription_end}
