"""empty message

Revision ID: 1e97d805d8c3
Revises: 33001328a26b, 85f10612cbe2, 89b358b81f13
Create Date: 2025-03-11 10:40:32.725570

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1e97d805d8c3'
down_revision: Union[str, None] = ('33001328a26b', '85f10612cbe2', '89b358b81f13')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
