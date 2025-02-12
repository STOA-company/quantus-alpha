from app.database.crud import database
from typing import List
from fastapi import UploadFile


def get_user_by_email(email: str):
    users = database._select(table="alphafinder_user", columns=["id", "email", "nickname"], email=email, limit=1)
    return users[0] if users else None


def create_user(email: str):
    database._insert(
        table="alphafinder_user",
        sets={
            "email": email,
        },
    )


def delete_user(id: int):
    database._delete(table="alphafinder_user", id=id)


def update_user(id: int, nickname: str = None, profile_image: UploadFile = None, favorite_stock: List[str] = None):
    database._update(
        table="alphafinder_user",
        sets={
            "nickname": nickname,
            "profile_image": profile_image,
        },
        id=id,
    )

    for ticker in favorite_stock:
        add_favorite_stock(id, ticker)


def add_favorite_stock(id: int, ticker: str):
    if database._select(table="user_stock_interest", user_id=id, ticker=ticker, limit=1):
        return
    database._insert(
        table="user_stock_interest",
        sets={
            "user_id": id,
            "ticker": ticker,
        },
    )


def delete_favorite_stock(id: int, ticker: str):
    database._delete(table="user_stock_interest", user_id=id, ticker=ticker)
