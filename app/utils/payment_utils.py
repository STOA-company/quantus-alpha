import httpx


def get_payment_method(payment_key: str):
    with httpx.Client() as client:
        url = f"https://api.tosspayments.com/v1/payments/{payment_key}"
        response = client.get(url)
        return response.json()["method"]
