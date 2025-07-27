from contextlib import contextmanager
from peewee import PostgresqlDatabase
from loguru import logger

from src.kvmflows.config.config import config


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


def create_database_if_not_exists():
    """Create the database if it doesn't exist."""
    # Connect to default postgres database first
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


def initialize_database(models):
    """
    Initialize the database and create tables for all models if they do not exist.
    Also applies update triggers for models that use UpdateAtTriggerMixin.
    Args:
        models (list): List of Peewee model classes to create tables for.
    """
    try:
        create_database_if_not_exists()  # Ensure database exists
        
        # Ensure main database connection is open
        if db.is_closed():
            db.connect()
        
        with db:
            logger.info("Initializing database and creating tables...")
            db.create_tables(models, safe=True)
            logger.info("Tables created successfully.")

            # Apply triggers for models that use UpdateAtTriggerMixin
            from src.kvmflows.database.mixin.updated_at_trigger import UpdateAtTriggerMixin

            for model in models:
                if issubclass(model, UpdateAtTriggerMixin):
                    try:
                        model.apply_update_trigger()
                        logger.info(f"Applied update trigger for {model.__name__}")
                    except AttributeError as e:
                        logger.warning(f"Could not apply trigger for {model.__name__}: {e}")
                    except Exception as e:
                        logger.error(f"Error applying trigger for {model.__name__}: {e}")
        
        logger.info("Database initialization completed successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise


if __name__ == "__main__":
    # Example usage: Initialize the database with a list of models
    from src.kvmflows.database.dummy import DummyModel

    initialize_database([DummyModel])
    logger.info("Database initialized and tables created.")
