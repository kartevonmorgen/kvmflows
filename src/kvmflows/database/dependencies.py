from contextlib import contextmanager
from typing import Generator
from fastapi import Depends
from peewee import PostgresqlDatabase
from loguru import logger

from src.kvmflows.config.config import config
from src.kvmflows.database.db import db


def ensure_database_connection() -> None:
    """Ensure database connection is active and usable."""
    try:
        if db.is_closed():
            db.connect()
            logger.debug("Database connection opened")
        
        # Test the connection
        db.execute_sql("SELECT 1")
        logger.debug("Database connection verified")
        
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        # Try to reconnect
        try:
            if not db.is_closed():
                db.close()
            db.connect()
            logger.info("Database reconnected successfully")
        except Exception as reconnect_error:
            logger.error(f"Failed to reconnect to database: {reconnect_error}")
            raise


@contextmanager
def database_transaction():
    """Context manager for database transactions."""
    ensure_database_connection()
    try:
        with db.atomic():
            yield db
    except Exception as e:
        logger.error(f"Database transaction error: {e}")
        raise


def get_db_connection() -> Generator[PostgresqlDatabase, None, None]:
    """FastAPI dependency for database connection with proper lifecycle management."""
    try:
        ensure_database_connection()
        yield db
    except Exception as e:
        logger.error(f"Database dependency error: {e}")
        raise


def get_db_transaction() -> Generator[PostgresqlDatabase, None, None]:
    """FastAPI dependency for database connection with transaction support."""
    try:
        with database_transaction() as db_conn:
            yield db_conn
    except Exception as e:
        logger.error(f"Database transaction dependency error: {e}")
        raise


# Type annotations for the dependencies
DatabaseDep = Depends(get_db_connection)
DatabaseTransactionDep = Depends(get_db_transaction)
