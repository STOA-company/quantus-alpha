from pydantic import BaseModel


class TossPaymentReceipt(BaseModel):
    payment_key: str
    order_id: str
    timedelta: int
    price: int
