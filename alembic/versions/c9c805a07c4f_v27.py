"""v25

Revision ID: c9c805a07c4f
Revises: 72a289c55933
Create Date: 2025-01-04 13:30:10.569746

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c9c805a07c4f"
down_revision: Union[str, None] = "ff900c578387"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.alter_column(
        "stock_trend", "ko_name", new_column_name="kr_name", existing_type=sa.String(length=100), existing_nullable=True
    )


def downgrade():
    op.alter_column(
        "stock_trend", "kr_name", new_column_name="ko_name", existing_type=sa.String(length=100), existing_nullable=True
    )
