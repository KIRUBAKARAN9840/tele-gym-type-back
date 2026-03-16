"""fittbot_models_sync

Revision ID: b93585c1d9d0
Revises:
Create Date: 2025-12-16 12:24:11.909969

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'b93585c1d9d0'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Only add the joined_at column - all tables already exist
    op.add_column('gym_import_data', sa.Column('joined_at', sa.Date(), nullable=True))


def downgrade() -> None:
    op.drop_column('gym_import_data', 'joined_at')
