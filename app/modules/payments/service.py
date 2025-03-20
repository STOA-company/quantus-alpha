from app.core.config import settings
from datetime import datetime, timedelta
from app.database.crud import database_service


class PaymentService:
    def __init__(self):
        self.toss_secret_key = settings.TOSS_SECRET_KEY
        self.db = database_service

    def store_toss_payments_history(
        self,
        payment_key: str,
        order_id: str,
        amount: int,
        user_id: int,
        email: str,
    ):
        # payment_method = get_payment_method(payment_key)
        self.db._insert(
            table="toss_payment_history",
            sets={
                "payment_key": payment_key,
                "order_id": order_id,
                "amount": amount,
                "user_id": user_id,
                "email": email,
                # "payment_method": payment_method,
            },
        )

    def user_subscription_update(self, user_id: int, period: int):
        subscription_end = datetime.now().date() + timedelta(days=period)
        self.db._update(
            table="alphafinder_user",
            sets={"is_subscribed": True, "subscription_end": subscription_end},
            id=user_id,
        )
        return {"subscription_end": subscription_end}
