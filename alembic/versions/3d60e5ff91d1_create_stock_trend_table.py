"""create stock trend table

Revision ID: 3d60e5ff91d1
Revises: 46cee821f04c
Create Date: 2024-12-26 17:15:06.796097

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3d60e5ff91d1'
down_revision: Union[str, None] = '46cee821f04c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'stock_trend',
        sa.Column('ticker', sa.String(20), primary_key=True, nullable=False, comment="종목 코드"),
        sa.Column('last_updated', sa.DateTime, nullable=False, index=True, comment="마지막 업데이트 시간"),
        sa.Column('ko_name', sa.String(100), nullable=True, comment="종목 한글명"),
        sa.Column('en_name', sa.String(100), nullable=True, comment="종목 영문명"),
        sa.Column('market', sa.String(10), nullable=False, index=True, comment="시장 구분"),
        
        # 현재가 관련
        sa.Column('current_price', sa.Float, nullable=False, comment="현재가"),
        sa.Column('prev_close', sa.Float, nullable=False, comment="전일종가"),
        
        # 등락률
        sa.Column('change_1m', sa.Float, nullable=True, comment="실시간 등락률"),
        sa.Column('change_1d', sa.Float, nullable=True, comment="1일 등락률"),
        sa.Column('change_1w', sa.Float, nullable=True, comment="1주 등락률"),
        sa.Column('change_1mo', sa.Float, nullable=True, comment="1개월 등락률"),
        sa.Column('change_6mo', sa.Float, nullable=True, comment="6개월 등락률"),
        sa.Column('change_1y', sa.Float, nullable=True, comment="1년 등락률"),
        
        # 거래량
        sa.Column('volume_1m', sa.Float, nullable=True, comment="1분 거래량 비율"),
        sa.Column('volume_1d', sa.Float, nullable=True, comment="1일 거래량 비율"),
        sa.Column('volume_1w', sa.Float, nullable=True, comment="1주 거래량 비율"),
        sa.Column('volume_1mo', sa.Float, nullable=True, comment="1개월 거래량 비율"),
        sa.Column('volume_6mo', sa.Float, nullable=True, comment="6개월 거래량 비율"),
        sa.Column('volume_1y', sa.Float, nullable=True, comment="1년 거래량 비율"),
        
        # 거래대금
        sa.Column('volume_change_1m', sa.Float, nullable=True, comment="1분 거래대금"),
        sa.Column('volume_change_1d', sa.Float, nullable=True, comment="1일 거래대금"),
        sa.Column('volume_change_1w', sa.Float, nullable=True, comment="1주 거래대금"),
        sa.Column('volume_change_1mo', sa.Float, nullable=True, comment="1개월 거래대금"),
        sa.Column('volume_change_6mo', sa.Float, nullable=True, comment="6개월 거래대금"),
        sa.Column('volume_change_1y', sa.Float, nullable=True, comment="1년 거래대금"),
    )

def downgrade() -> None:
    op.drop_table('stock_trend')