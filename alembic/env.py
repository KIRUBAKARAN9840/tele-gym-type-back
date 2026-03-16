from logging.config import fileConfig
import sys
from pathlib import Path

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# ✅ WORKAROUND for MySQL 'TABLENAME' KeyError during autogenerate
from sqlalchemy.dialects.mysql import base as mysql_base


def _ignore_mysql_bug_88718_96365(self, fkeys, connection):
    # Do nothing; avoid KeyError: 'TABLENAME' in reflection
    return


mysql_base.MySQLDialect._correct_for_mysql_bugs_88718_96365 = _ignore_mysql_bug_88718_96365

# ✅ WORKAROUND for NoSuchTableError during FK reflection
from sqlalchemy.engine import reflection
_original_reflect_table = reflection.Inspector.reflect_table


def _safe_reflect_table(self, table, *args, **kwargs):
    try:
        return _original_reflect_table(self, table, *args, **kwargs)
    except Exception as e:
        if "NoSuchTableError" in str(type(e)):
            return
        raise


reflection.Inspector.reflect_table = _safe_reflect_table
# ✅ END WORKAROUNDS


# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import ONLY fittbot_models for fittbot schema
from app.models.database import Base
from app.models import fittbot_models

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def include_object(object, name, type_, reflected, compare_to):
    """Filter objects for autogenerate - skip problematic reflected tables"""
    # Skip tables that are reflected from DB but not in our models
    if type_ == "table" and reflected and compare_to is None:
        return False
    return True


def run_migrations_online() -> None:
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
