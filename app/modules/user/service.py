from app.database.crud import database
from typing import List
from fastapi import UploadFile


def get_user_by_email(email: str):
    user = database._select(table="alphafinder_user", columns=["id", "email", "nickname"], email=email, limit=1)
    return user[0]


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
        database._insert(
            table="user_stock_interest",
            sets={
                "user_id": id,
                "ticker": ticker,
            },
        )
