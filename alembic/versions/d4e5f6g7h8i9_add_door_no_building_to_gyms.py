"""Add door_no and building columns to gyms table

Revision ID: d4e5f6g7h8i9
Revises: c3d4e5f6g7h8
Create Date: 2025-12-22

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd4e5f6g7h8i9'
down_revision: Union[str, Sequence[str], None] = 'c3d4e5f6g7h8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add door_no and building columns to gyms table."""
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col['name'] for col in inspector.get_columns('gyms')]

    if 'door_no' not in columns:
        op.add_column(
            'gyms',
            sa.Column('door_no', sa.String(length=50), nullable=True)
        )

    if 'building' not in columns:
        op.add_column(
            'gyms',
            sa.Column('building', sa.String(length=255), nullable=True)
        )


def downgrade() -> None:
    """Remove door_no and building columns from gyms table."""
    op.drop_column('gyms', 'building')
    op.drop_column('gyms', 'door_no')
