"""Add new_offer table for gym-level promo flags

Revision ID: f6a7b8c9d0e1
Revises: d4e5f6g7h8i9
Create Date: 2026-01-27

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "f6a7b8c9d0e1"
down_revision: Union[str, Sequence[str], None] = "d4e5f6g7h8i9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create new_offer table if it does not exist."""
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    if "new_offer" not in inspector.get_table_names():
        op.create_table(
            "new_offer",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("gym_id", sa.Integer(), nullable=False),
            sa.Column("dailypass", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("session", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.ForeignKeyConstraint(["gym_id"], ["gyms.gym_id"], ondelete="CASCADE", onupdate="CASCADE"),
        )
        op.create_unique_constraint("uq_new_offer_gym_id", "new_offer", ["gym_id"])
        op.create_index("ix_new_offer_gym_id", "new_offer", ["gym_id"], unique=False)


def downgrade() -> None:
    """Drop new_offer table."""
    op.drop_index("ix_new_offer_gym_id", table_name="new_offer")
    op.drop_constraint("uq_new_offer_gym_id", "new_offer", type_="unique")
    op.drop_table("new_offer")

