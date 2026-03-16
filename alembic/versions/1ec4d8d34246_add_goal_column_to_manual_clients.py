"""add_goal_column_to_manual_clients

Revision ID: 1ec4d8d34246
Revises: f6g7h8i9j0k1
Create Date: 2026-01-02 13:24:22.072401

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1ec4d8d34246'
down_revision: Union[str, Sequence[str], None] = 'f6g7h8i9j0k1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('manual_clients', sa.Column('goal', sa.String(50), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('manual_clients', 'goal')
