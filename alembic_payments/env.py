"""Alembic env for payments models only - targeting payments_staging"""

from logging.config import fileConfig
import sys
from pathlib import Path

from sqlalchemy import engine_from_config, pool, MetaData, Table
from alembic import context

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Alembic Config object
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import ONLY payments models
from app.models.database import Base

# Import all payments models to register them with Base.metadata
from app.fittbot_api.v1.payments.models import (
    CatalogProduct, Order, OrderItem, Entitlement, Checkin,
    Payment, Settlement, SettlementItem, FeesActuals, CommissionSchedule,
    PayoutBatch, PayoutEvent, PayoutLine, Beneficiary, Subscription,
    Refund, Dispute, Adjustment, WebhookEvent, IdempotencyKey
)
from app.fittbot_api.v1.payments.models.profits import PlatformEarning

# Create a new metadata with schema removed for comparison
# In MySQL, schema = database, so we compare against payments_staging directly
def create_schemaless_metadata():
    """Create metadata with 'payments' schema stripped (for MySQL comparison)"""
    new_metadata = MetaData()

    for table_name, table in Base.metadata.tables.items():
        # Only include tables with schema='payments'
        if table.schema == 'payments':
            # Copy table to new metadata without schema
            new_table = Table(
                table.name,
                new_metadata,
                *[c.copy() for c in table.columns],
                schema=None  # Remove schema for MySQL comparison
            )
            # Copy indexes
            for idx in table.indexes:
                idx_cols = [new_table.c[c.name] for c in idx.columns]
                new_metadata.tables[table.name].append_constraint(
                    type(idx)(
                        *idx_cols,
                        name=idx.name,
                        unique=idx.unique
                    )
                )
            # Copy constraints
            for constraint in table.constraints:
                if hasattr(constraint, 'columns') and len(constraint.columns) > 0:
                    if constraint.__class__.__name__ not in ['PrimaryKeyConstraint']:
                        try:
                            c_cols = [new_table.c[c.name] for c in constraint.columns]
                            new_metadata.tables[table.name].append_constraint(
                                type(constraint)(*c_cols, name=constraint.name)
                            )
                        except:
                            pass

    return new_metadata

target_metadata = create_schemaless_metadata()


def include_object(object, name, type_, reflected, compare_to):
    """Filter objects for comparison"""
    if type_ == "table":
        # Skip alembic_version and webhook tables not in our models
        if name in ["alembic_version", "webhook_processing_logs", "webhook_recovery_logs", "webhook_monitoring_stats"]:
            return False
        # Skip reflected tables not in our models
        if reflected and compare_to is None:
            return False
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
            compare_type=False,
            compare_server_default=False,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
