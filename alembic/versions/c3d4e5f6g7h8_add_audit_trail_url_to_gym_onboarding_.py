"""Add audit_trail_url to gym_onboarding_esign

Revision ID: c3d4e5f6g7h8
Revises: b61e0494267a
Create Date: 2025-12-22

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6g7h8'
down_revision: Union[str, Sequence[str], None] = 'b61e0494267a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add audit_trail_url column to gym_onboarding_esign table."""
    # Check if column already exists to make migration idempotent
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col['name'] for col in inspector.get_columns('gym_onboarding_esign')]

    if 'audit_trail_url' not in columns:
        op.add_column(
            'gym_onboarding_esign',
            sa.Column('audit_trail_url', sa.String(length=500), nullable=True)
        )


def downgrade() -> None:
    """Remove audit_trail_url column from gym_onboarding_esign table."""
    op.drop_column('gym_onboarding_esign', 'audit_trail_url')
