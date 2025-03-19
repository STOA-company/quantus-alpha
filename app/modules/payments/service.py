from app.core.config import settings
from datetime import datetime
from app.database.crud import database_service
# from app.utils.payment_utils import get_payment_method


class PaymentService:
    def __init__(self):
        self.toss_secret_key = settings.TOSS_SECRET_KEY
        self.db = database_service

    def store_toss_payments_history(self, payment_key: str, order_id: str, amount: str, user_id: int):
        # payment_method = get_payment_method(payment_key)
        self.db._insert(
            table="toss_payment_history",
            data={
                "payment_key": payment_key,
                "order_id": order_id,
                "amount": amount,
                "user_id": user_id,
                # "payment_method": payment_method
            },
        )

    def user_subscription_update(self, user_id: int, timedelta: int):
        self.db._update(
            table="alphafinder_user",
            data={"is_subscribed": True, "subscription_end": datetime.now().date() + timedelta(days=timedelta)},
            where={"id": user_id},
        )
