"""Create gym_agreements table for PDF agreement generation

Revision ID: e5f6g7h8i9j0
Revises: d4e5f6g7h8i9
Create Date: 2025-12-23

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5f6g7h8i9j0'
down_revision: Union[str, Sequence[str], None] = 'd4e5f6g7h8i9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create gym_agreements table for tracking prefilled PDF agreements."""
    op.create_table(
        'gym_agreements',
        sa.Column('agreement_id', sa.String(length=36), nullable=False),
        sa.Column('gym_id', sa.Integer(), nullable=False),
        sa.Column('owner_id', sa.Integer(), nullable=True),
        sa.Column('template_version', sa.String(length=20), nullable=False, server_default='v1'),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='PENDING'),
        sa.Column('prefill_json', sa.JSON(), nullable=True),
        sa.Column('s3_key_final', sa.Text(), nullable=True),
        sa.Column('pdf_sha256', sa.String(length=64), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('ready_at', sa.DateTime(), nullable=True),
        sa.Column('accepted_at', sa.DateTime(), nullable=True),
        sa.Column('accepted_by_name', sa.String(length=200), nullable=True),
        sa.Column('accepted_ip', sa.String(length=64), nullable=True),
        sa.Column('accepted_user_agent', sa.Text(), nullable=True),
        sa.Column('selfie_s3_key', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(['gym_id'], ['gyms.gym_id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['owner_id'], ['gym_owners.owner_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('agreement_id')
    )

    # Create indexes for common queries
    op.create_index('ix_gym_agreement_gym_id', 'gym_agreements', ['gym_id'])
    op.create_index('ix_gym_agreement_owner_id', 'gym_agreements', ['owner_id'])
    op.create_index('ix_gym_agreement_status', 'gym_agreements', ['status'])
    op.create_index('ix_gym_agreement_gym_status', 'gym_agreements', ['gym_id', 'status'])
    op.create_index('ix_gym_agreement_created', 'gym_agreements', ['created_at'])


def downgrade() -> None:
    """Drop gym_agreements table and indexes."""
    op.drop_index('ix_gym_agreement_created', table_name='gym_agreements')
    op.drop_index('ix_gym_agreement_gym_status', table_name='gym_agreements')
    op.drop_index('ix_gym_agreement_status', table_name='gym_agreements')
    op.drop_index('ix_gym_agreement_owner_id', table_name='gym_agreements')
    op.drop_index('ix_gym_agreement_gym_id', table_name='gym_agreements')
    op.drop_table('gym_agreements')
