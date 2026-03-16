"""create_client_and_owner_modal_tracker_tables

Revision ID: b2c3d4e5f6g7
Revises: a1b2c3d4e5f6
Create Date: 2025-12-16

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6g7'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create client_modal_tracker table
    op.create_table('client_modal_tracker',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('client_id', sa.Integer(), nullable=False),
        sa.Column('last_modal_index', sa.Integer(), nullable=False, default=0),
        sa.Column('last_shown_date', sa.Date(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['client_id'], ['clients.client_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_client_modal_tracker_client_id', 'client_modal_tracker', ['client_id'], unique=True)

    # Create owner_modal_tracker table
    op.create_table('owner_modal_tracker',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('gym_id', sa.Integer(), nullable=False),
        sa.Column('last_modal_index', sa.Integer(), nullable=False, default=0),
        sa.Column('last_shown_date', sa.Date(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['gym_id'], ['gyms.gym_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_owner_modal_tracker_gym_id', 'owner_modal_tracker', ['gym_id'], unique=True)


def downgrade() -> None:
    op.drop_index('ix_owner_modal_tracker_gym_id', table_name='owner_modal_tracker')
    op.drop_table('owner_modal_tracker')
    op.drop_index('ix_client_modal_tracker_client_id', table_name='client_modal_tracker')
    op.drop_table('client_modal_tracker')
