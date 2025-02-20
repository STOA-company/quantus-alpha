"""rename stock trend columns

Revision ID: b08a3c3fe21c
Revises: 82de6c02275a
Create Date: 2024-12-30 19:10:44.056360

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b08a3c3fe21c"
down_revision: Union[str, None] = "82de6c02275a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # 등락률 컬럼 이름 변경
    op.alter_column("stock_trend", "change_1m", new_column_name="change_rt", existing_type=sa.Float())
    op.alter_column("stock_trend", "change_1mo", new_column_name="change_1m", existing_type=sa.Float())
    op.alter_column("stock_trend", "change_6mo", new_column_name="change_6m", existing_type=sa.Float())

    # 거래량 컬럼 이름 변경
    op.alter_column("stock_trend", "volume_1m", new_column_name="volume_rt", existing_type=sa.Float())
    op.alter_column("stock_trend", "volume_1mo", new_column_name="volume_1m", existing_type=sa.Float())
    op.alter_column("stock_trend", "volume_6mo", new_column_name="volume_6m", existing_type=sa.Float())

    # 거래대금 컬럼 이름 변경
    op.alter_column("stock_trend", "volume_change_1m", new_column_name="volume_change_rt", existing_type=sa.Float())
    op.alter_column("stock_trend", "volume_change_1mo", new_column_name="volume_change_1m", existing_type=sa.Float())
    op.alter_column("stock_trend", "volume_change_6mo", new_column_name="volume_change_6m", existing_type=sa.Float())


def downgrade():
    # 등락률 컬럼 이름 복구
    op.alter_column("stock_trend", "change_rt", new_column_name="change_1m", existing_type=sa.Float())
    op.alter_column("stock_trend", "change_1m", new_column_name="change_1mo", existing_type=sa.Float())
    op.alter_column("stock_trend", "change_6m", new_column_name="change_6mo", existing_type=sa.Float())

    # 거래량 컬럼 이름 복구
    op.alter_column("stock_trend", "volume_rt", new_column_name="volume_1m", existing_type=sa.Float())
    op.alter_column("stock_trend", "volume_1m", new_column_name="volume_1mo", existing_type=sa.Float())
    op.alter_column("stock_trend", "volume_6m", new_column_name="volume_6mo", existing_type=sa.Float())

    # 거래대금 컬럼 이름 복구
    op.alter_column("stock_trend", "volume_change_rt", new_column_name="volume_change_1m", existing_type=sa.Float())
    op.alter_column("stock_trend", "volume_change_1m", new_column_name="volume_change_1mo", existing_type=sa.Float())
    op.alter_column("stock_trend", "volume_change_6m", new_column_name="volume_change_6mo", existing_type=sa.Float())
