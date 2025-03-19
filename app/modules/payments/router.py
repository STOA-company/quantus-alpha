from fastapi import APIRouter, Depends
from app.modules.payments.service import PaymentService
from app.modules.payments.schema import TossPaymentReceipt
from app.models.models_users import AlphafinderUser
from app.utils.oauth_utils import get_current_user

router = APIRouter()


@router.get("/toss")
def get_toss_payments_receipt(
    toss_payment_receipt: TossPaymentReceipt,
    current_user: AlphafinderUser = Depends(get_current_user),
    payment_service: PaymentService = Depends(PaymentService),
):
    payment_service.store_toss_payments_history(
        toss_payment_receipt.payment_key, toss_payment_receipt.order_id, toss_payment_receipt.price, current_user.id
    )
    payment_service.user_subscription_update(current_user.id, toss_payment_receipt.timedelta)
    return {"status": "success"}
