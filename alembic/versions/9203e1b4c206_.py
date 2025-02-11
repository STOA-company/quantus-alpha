"""empty message

Revision ID: 9203e1b4c206
Revises: 5da5feb1a030
Create Date: 2025-02-11 14:15:17.284423

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = "9203e1b4c206"
down_revision: Union[str, None] = "5da5feb1a030"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 외래 키 제약 조건 삭제
    op.drop_constraint("user_stock_interest_ibfk_1", "user_stock_interest", type_="foreignkey")

    # 컬럼 변경
    op.alter_column(
        "user_stock_interest",
        "ticker",
        existing_type=mysql.VARCHAR(length=100),
        type_=sa.String(length=20),
        existing_nullable=False,
    )  # NOT NULL로 변경

    # 인덱스 삭제
    op.drop_index("ix_user_stock_interest_ticker", table_name="user_stock_interest")

    # user_id에 대한 외래 키 제약 조건 추가
    op.create_foreign_key(
        "fk_user_stock_interest_user_id",
        "user_stock_interest",
        "alphafinder_user",
        ["user_id"],
        ["id"],
        ondelete="CASCADE",
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # 외래 키 제약 조건 삭제
    op.drop_constraint("fk_user_stock_interest_user_id", "user_stock_interest", type_="foreignkey")

    # 인덱스 다시 추가
    op.create_index("ix_user_stock_interest_ticker", "user_stock_interest", ["ticker"], unique=False)

    # 컬럼 변경
    op.alter_column(
        "user_stock_interest",
        "ticker",
        existing_type=sa.String(length=20),
        type_=mysql.VARCHAR(length=100),
        existing_nullable=True,
    )
    # ### end Alembic commands ###
