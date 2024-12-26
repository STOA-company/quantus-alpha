"""create stock price table

Revision ID: f581b6f1cf5f
Revises: 3d60e5ff91d1
Create Date: 2024-12-26 20:53:40.754626

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f581b6f1cf5f'
down_revision: Union[str, None] = '3d60e5ff91d1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'stock_price',
        sa.Column('ticker', sa.String(20), primary_key=True, nullable=False, comment="종목 코드"),
        sa.Column('market', sa.String(10), nullable=False, index=True, comment="시장 구분"),
        sa.Column('kr_name', sa.String(100), nullable=True, comment="한글 종목명"),
        sa.Column('en_name', sa.String(100), nullable=True, comment="영문 종목명"),
        sa.Column('execution_time', sa.DateTime, nullable=False, comment="종목 체결 시간"),
        sa.Column('current_price', sa.Float, nullable=False, comment="종목 현재 가격"),
        
        # OHLCV 데이터
        sa.Column('open', sa.Float, nullable=False, comment="시가"),
        sa.Column('high', sa.Float, nullable=False, comment="고가"),
        sa.Column('low', sa.Float, nullable=False, comment="저가"),
        sa.Column('close', sa.Float, nullable=False, comment="종가"),
        sa.Column('volume', sa.Integer, nullable=False, comment="거래량"),
        
        # 전일 대비
        sa.Column('change_sign', sa.Integer, nullable=True, comment="전일 대비 등락 부호"),
        sa.Column('price_change', sa.Float, nullable=True, comment="전일 대비"),
        sa.Column('change_rate', sa.Float, nullable=True, comment="등락률"),
        
        sa.Column('volume_change', sa.Float, nullable=True, comment="거래대금"),
    )
    
    # 인덱스 생성
    op.create_index('ix_stock_price_market', 'stock_price', ['market'])

def downgrade() -> None:
    # 인덱스 삭제
    op.drop_index('ix_stock_price_market', 'stock_price')
    
    # 테이블 삭제
    op.drop_table('stock_price')
