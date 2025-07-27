from contextlib import asynccontextmanager
from typing import Generator, AsyncGenerator
from fastapi import Depends
from peewee import PostgresqlDatabase
from loguru import logger

from src.kvmflows.database.db import db, async_db


async def ensure_database_connection() -> None:
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


@asynccontextmanager
async def database_transaction():
    """Context manager for database transactions."""
    await ensure_database_connection()
    try:
        # For peewee-async 1.1.0, we'll use a simpler approach
        # Transaction management is handled by the AioModel itself
        yield async_db
    except Exception as e:
        logger.error(f"Database transaction error: {e}")
        raise


def get_db_connection() -> Generator[PostgresqlDatabase, None, None]:
    """FastAPI dependency for database connection with proper lifecycle management."""
    try:
        # Use synchronous connection check for compatibility
        if db.is_closed():
            db.connect()

        # Test the connection
        db.execute_sql("SELECT 1")
        yield db
    except Exception as e:
        logger.error(f"Database dependency error: {e}")
        raise


async def get_async_db_connection() -> AsyncGenerator[object, None]:
    """FastAPI dependency for async database connection with proper lifecycle management."""
    try:
        await ensure_database_connection()
        yield async_db
    except Exception as e:
        logger.error(f"Async database dependency error: {e}")
        raise


async def get_async_db_transaction() -> AsyncGenerator[object, None]:
    """FastAPI dependency for async database connection with transaction support."""
    try:
        async with database_transaction() as db_conn:
            yield db_conn
    except Exception as e:
        logger.error(f"Async database transaction dependency error: {e}")
        raise


def get_db_transaction() -> Generator[PostgresqlDatabase, None, None]:
    """FastAPI dependency for database connection with transaction support."""
    try:
        if db.is_closed():
            db.connect()

        with db.atomic():
            yield db
    except Exception as e:
        logger.error(f"Database transaction dependency error: {e}")
        raise


# Type annotations for the dependencies
DatabaseDep = Depends(get_db_connection)
DatabaseTransactionDep = Depends(get_db_transaction)
AsyncDatabaseDep = Depends(get_async_db_connection)
AsyncDatabaseTransactionDep = Depends(get_async_db_transaction)
