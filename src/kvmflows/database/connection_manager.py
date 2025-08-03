"""
Database connection manager to prevent memory leaks.
"""

import asyncio
import weakref
from contextlib import asynccontextmanager
from typing import Optional, Set
from loguru import logger

from src.kvmflows.database.db import async_db


class ConnectionManager:
    """Manages database connections to prevent memory leaks."""

    def __init__(self):
        self._active_connections: Set[weakref.ref] = set()
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start_cleanup_task(self):
        """Start the periodic cleanup task."""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._periodic_cleanup())

    async def stop_cleanup_task(self):
        """Stop the periodic cleanup task."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def _periodic_cleanup(self):
        """Periodically clean up stale connections."""
        while True:
            try:
                await asyncio.sleep(60)  # Run every minute
                await self.cleanup_stale_connections()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic cleanup: {e}")

    async def cleanup_stale_connections(self):
        """Clean up stale database connections."""
        try:
            # Clean up dead references
            dead_refs = {ref for ref in self._active_connections if ref() is None}
            self._active_connections -= dead_refs

            # Force garbage collection
            import gc

            gc.collect()

            logger.debug(f"Cleaned up {len(dead_refs)} stale connection references")
        except Exception as e:
            logger.error(f"Error cleaning up connections: {e}")

    @asynccontextmanager
    async def get_connection(self):
        """Get a managed database connection."""
        connection = None
        try:
            # For now, just yield the global async_db
            # In the future, this could manage a pool
            connection = async_db

            # Track the connection
            conn_ref = weakref.ref(connection)
            self._active_connections.add(conn_ref)

            yield connection
        finally:
            # Cleanup is handled by the periodic task
            pass


# Global connection manager instance
connection_manager = ConnectionManager()


@asynccontextmanager
async def managed_db_connection():
    """Context manager for managed database connections."""
    async with connection_manager.get_connection() as conn:
        yield conn


async def initialize_connection_manager():
    """Initialize the connection manager."""
    await connection_manager.start_cleanup_task()
    logger.info("Database connection manager initialized")


async def shutdown_connection_manager():
    """Shutdown the connection manager."""
    await connection_manager.stop_cleanup_task()
    await connection_manager.cleanup_stale_connections()
    logger.info("Database connection manager shutdown")
