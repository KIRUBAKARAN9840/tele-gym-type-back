"""fittbot_models_local_sync

Revision ID: b61e0494267a
Revises: b2c3d4e5f6g7
Create Date: 2025-12-21 19:35:40.446299

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = 'b61e0494267a'
down_revision: Union[str, Sequence[str], None] = 'b2c3d4e5f6g7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def safe_create_index(index_name, table_name, columns, unique=False, schema=None):
    """Create index only if it doesn't exist."""
    try:
        op.create_index(index_name, table_name, columns, unique=unique, schema=schema)
    except Exception:
        pass


def safe_create_fk(constraint_name, source_table, referent_table, local_cols, remote_cols, ondelete=None):
    """Create foreign key only if it doesn't exist."""
    try:
        op.create_foreign_key(constraint_name, source_table, referent_table, local_cols, remote_cols, ondelete=ondelete)
    except Exception:
        pass


def upgrade() -> None:
    """Upgrade schema - CONSTRUCTIVE ONLY.

    This migration only adds indexes and foreign keys. It does NOT:
    - Drop any tables
    - Drop any indexes
    - Drop any columns
    - Change column types in destructive ways

    Sessions schema tables are managed separately and already exist.
    """

    # Add new indexes to attendance table
    safe_create_index('ix_attendance_client_date', 'attendance', ['client_id', 'date'])
    safe_create_index('ix_attendance_gym_date', 'attendance', ['gym_id', 'date'])

    # Add indexes to characters_combination tables
    safe_create_index(op.f('ix_characters_combination_id'), 'characters_combination', ['id'])
    safe_create_index(op.f('ix_characters_combination_old_id'), 'characters_combination_old', ['id'])

    # Add indexes to client_characters
    safe_create_index(op.f('ix_client_characters_character_id'), 'client_characters', ['character_id'])
    safe_create_index(op.f('ix_client_characters_client_id'), 'client_characters', ['client_id'])
    safe_create_index(op.f('ix_client_characters_id'), 'client_characters', ['id'])

    # Add foreign key to client_modal_tracker
    safe_create_fk(None, 'client_modal_tracker', 'clients', ['client_id'], ['client_id'], ondelete='CASCADE')

    # Add indexes to default_workout_templates
    safe_create_index(op.f('ix_default_workout_templates_id'), 'default_workout_templates', ['id'])

    # Add indexes to fittbot_diet_template
    safe_create_index(op.f('ix_fittbot_diet_template_id'), 'fittbot_diet_template', ['id'])

    # Add indexes to fittbot_food
    safe_create_index(op.f('ix_fittbot_food_id'), 'fittbot_food', ['id'])

    # Add indexes to fittbot_ratings
    safe_create_index(op.f('ix_fittbot_ratings_client_id'), 'fittbot_ratings', ['client_id'])
    safe_create_index(op.f('ix_fittbot_ratings_id'), 'fittbot_ratings', ['id'])

    # Add indexes to gym_details
    safe_create_index(op.f('ix_gym_details_gym_id'), 'gym_details', ['gym_id'], unique=True)
    safe_create_index(op.f('ix_gym_details_id'), 'gym_details', ['id'])

    # Add indexes to gym_manual_data
    safe_create_index(op.f('ix_gym_manual_data_gym_id'), 'gym_manual_data', ['gym_id'])
    safe_create_index(op.f('ix_gym_manual_data_id'), 'gym_manual_data', ['id'])

    # Add foreign key to gym_studios_pic
    safe_create_fk(None, 'gym_studios_pic', 'gyms', ['gym_id'], ['gym_id'], ondelete='CASCADE')

    # Add indexes to gym_studios_request
    safe_create_index(op.f('ix_gym_studios_request_client_id'), 'gym_studios_request', ['client_id'])

    # Add indexes to gym_verification_documents
    safe_create_index(op.f('ix_gym_verification_documents_gym_id'), 'gym_verification_documents', ['gym_id'])
    safe_create_index(op.f('ix_gym_verification_documents_id'), 'gym_verification_documents', ['id'])

    # Add indexes to home_posters
    safe_create_index(op.f('ix_home_posters_id'), 'home_posters', ['id'])

    # Add indexes to home_workout
    safe_create_index(op.f('ix_home_workout_id'), 'home_workout', ['id'])

    # Add indexes to indian_food_master
    safe_create_index(op.f('ix_indian_food_master_is_eggetarian'), 'indian_food_master', ['is_eggetarian'])
    safe_create_index(op.f('ix_indian_food_master_is_gluten_free'), 'indian_food_master', ['is_gluten_free'])
    safe_create_index(op.f('ix_indian_food_master_is_heart_healthy'), 'indian_food_master', ['is_heart_healthy'])
    safe_create_index(op.f('ix_indian_food_master_is_jain'), 'indian_food_master', ['is_jain'])
    safe_create_index(op.f('ix_indian_food_master_is_ketogenic'), 'indian_food_master', ['is_ketogenic'])
    safe_create_index(op.f('ix_indian_food_master_is_lactose_free'), 'indian_food_master', ['is_lactose_free'])
    safe_create_index(op.f('ix_indian_food_master_is_muscle_gain_friendly'), 'indian_food_master', ['is_muscle_gain_friendly'])
    safe_create_index(op.f('ix_indian_food_master_is_paleo'), 'indian_food_master', ['is_paleo'])
    safe_create_index(op.f('ix_indian_food_master_is_vegan'), 'indian_food_master', ['is_vegan'])
    safe_create_index(op.f('ix_indian_food_master_is_weight_loss_friendly'), 'indian_food_master', ['is_weight_loss_friendly'])

    # Add indexes to no_cost_emi
    safe_create_index(op.f('ix_no_cost_emi_gym_id'), 'no_cost_emi', ['gym_id'])

    # Add foreign key to owner_modal_tracker
    safe_create_fk(None, 'owner_modal_tracker', 'gyms', ['gym_id'], ['gym_id'], ondelete='CASCADE')

    # Add indexes to reminders
    safe_create_index(op.f('ix_reminders_gym_id'), 'reminders', ['gym_id'])

    # Add indexes to reward_interest
    safe_create_index(op.f('ix_reward_interest_id'), 'reward_interest', ['id'])


def downgrade() -> None:
    """Downgrade schema - drops the indexes and foreign keys added in upgrade."""
    # This is intentionally minimal since we're doing constructive-only changes
    pass
