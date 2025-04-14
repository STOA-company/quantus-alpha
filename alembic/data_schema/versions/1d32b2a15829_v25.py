"""add stock_trend column ctry

Revision ID: 1d32b2a15829
Revises: 72a289c55933
Create Date: 2025-01-03 16:04:23.527040

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "1d32b2a15829"
down_revision: Union[str, None] = "72a289c55933"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("stock_trend", sa.Column("ctry", sa.String(20), nullable=True))


def downgrade() -> None:
    op.drop_column("stock_trend", "ctry")
