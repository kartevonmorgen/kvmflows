import asyncio
import gc

from loguru import logger


def async_job_wrapper(coro_func, job_name="async job"):
    """
    Generic wrapper to run an async coroutine as a scheduled job with logging.
    Args:
        coro_func: The coroutine function to run (no arguments)
        job_name: Name of the job for logging
    """

    def wrapper():
        logger.info(f"Starting scheduled {job_name}...")
        # Use try-finally to ensure proper cleanup
        try:
            asyncio.run(coro_func())
            logger.info(f"Scheduled {job_name} completed.")
        except Exception as e:
            logger.error(f"Error in scheduled {job_name}: {e}")
        finally:
            # Force garbage collection to prevent memory leaks
            gc.collect()

    return wrapper