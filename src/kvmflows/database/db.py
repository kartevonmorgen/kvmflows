from peewee import PostgresqlDatabase
import peewee_async
from loguru import logger
import asyncio
import sys

from src.kvmflows.config.config import config

# Set Windows-compatible event loop policy
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Create main database connection (for backward compatibility and initialization)
db = PostgresqlDatabase(
    config.db.name,
    user=config.db.user,
    password=config.db.password,
    host=config.db.host,
    port=config.db.port,
    # Add connection pooling and timeout settings
    autoconnect=True,
    thread_safe=True,
)

# Create async database using a simpler approach for Windows compatibility
# Use minimal connection pool for Windows
async_db = peewee_async.PooledPostgresqlDatabase(
    config.db.name,
    user=config.db.user,
    password=config.db.password,
    host=config.db.host,
    port=config.db.port,
    max_connections=5,  # Increase to handle concurrent operations
    min_connections=1,
)

# Register the async database
peewee_async.register_database(async_db)


async def create_database_if_not_exists():
    """Create the database if it doesn't exist."""
    # For database creation, we still need to use synchronous connection
    # since peewee-async doesn't support database creation
    postgres_db = PostgresqlDatabase(
        "postgres",
        user=config.db.user,
        password=config.db.password,
        host=config.db.host,
        port=config.db.port,
    )

    try:
        postgres_db.connect()
        cursor = postgres_db.execute_sql(
            "SELECT 1 FROM pg_database WHERE datname = %s;", (config.db.name,)
        )
        exists = cursor.fetchone()

        if not exists:
            # Database doesn't exist, so create it
            postgres_db.execute_sql(f"CREATE DATABASE {config.db.name};")
            logger.info(f"Database '{config.db.name}' created successfully.")
        else:
            logger.info(f"Database '{config.db.name}' already exists.")
    except Exception as e:
        logger.error(f"Error checking/creating database: {e}")
        raise
    finally:
        if not postgres_db.is_closed():
            postgres_db.close()


async def initialize_database(models):
    """
    Initialize the database and create tables for all models if they do not exist.
    Also applies update triggers for models that use UpdateAtTriggerMixin.
    Args:
        models (list): List of Peewee model classes to create tables for.
    """
    try:
        await create_database_if_not_exists()  # Ensure database exists

        # For now, use synchronous database operations for table creation
        # as peewee-async doesn't support DDL operations
        if db.is_closed():
            db.connect()

        with db.atomic():
            logger.info("Initializing database and creating tables...")
            db.create_tables(models, safe=True)
            logger.info("Tables created successfully.")

            # Apply triggers for models that use UpdateAtTriggerMixin
            from src.kvmflows.database.mixin.updated_at_trigger import (
                UpdateAtTriggerMixin,
            )

            for model in models:
                if issubclass(model, UpdateAtTriggerMixin):
                    try:
                        model.apply_update_trigger()  # Use sync method since we're in sync context
                        logger.info(f"Applied update trigger for {model.__name__}")
                    except AttributeError as e:
                        logger.warning(
                            f"Could not apply trigger for {model.__name__}: {e}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Error applying trigger for {model.__name__}: {e}"
                        )

        logger.info("Database initialization completed successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise


if __name__ == "__main__":
    # Example usage: Initialize the database with a list of models
    import asyncio
    from src.kvmflows.database.dummy import DummyModel

    async def main():
        await initialize_database([DummyModel])
        logger.info("Database initialized and tables created.")

    asyncio.run(main())
