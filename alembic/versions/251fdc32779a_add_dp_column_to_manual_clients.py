"""add_dp_column_to_manual_clients

Revision ID: 251fdc32779a
Revises: 1ec4d8d34246
Create Date: 2026-01-02 17:40:30.063246

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '251fdc32779a'
down_revision: Union[str, Sequence[str], None] = '1ec4d8d34246'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('manual_clients', sa.Column('dp', sa.String(500), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('manual_clients', 'dp')
