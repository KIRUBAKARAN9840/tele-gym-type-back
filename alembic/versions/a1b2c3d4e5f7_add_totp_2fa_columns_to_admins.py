"""add_totp_2fa_columns_to_admins

Revision ID: a1b2c3d4e5f7
Revises: f6a7b8c9d0e1
Create Date: 2026-02-28

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a1b2c3d4e5f7'
down_revision: Union[str, Sequence[str], None] = 'f6a7b8c9d0e1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('admins', sa.Column('totp_secret', sa.String(255), nullable=True), schema='fittbot_admins')
    op.add_column('admins', sa.Column('totp_enabled', sa.Boolean(), server_default=sa.false(), nullable=False), schema='fittbot_admins')
    op.add_column('admins', sa.Column('totp_backup_codes', sa.Text(), nullable=True), schema='fittbot_admins')


def downgrade() -> None:
    op.drop_column('admins', 'totp_backup_codes', schema='fittbot_admins')
    op.drop_column('admins', 'totp_enabled', schema='fittbot_admins')
    op.drop_column('admins', 'totp_secret', schema='fittbot_admins')
