"""upgrade stock trend - add column change sign

Revision ID: 5b109e7dcf82
Revises: 86386d4872b2
Create Date: 2024-12-27 11:10:18.302828

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5b109e7dcf82"
down_revision: Union[str, None] = "86386d4872b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("stock_trend", sa.Column("change_sign", sa.Integer, nullable=True, comment="전일 대비 등락 부호"))


def downgrade() -> None:
    op.drop_column("stock_trend", "change_sign")
