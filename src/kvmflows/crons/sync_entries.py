import asyncio
import gc

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from kvmflows.flows.sync_entires import sync_entries
from src.kvmflows.config.config import config


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


def run_cron():
    """
    Set up and start a blocking scheduler to run jobs daily at specified times.
    Demonstrates passing parameters to scheduled async jobs.
    """
    scheduler = BlockingScheduler(timezone="UTC")

    if config.crons.sync_entries.enabled:
        logger.info("Sync entries cron job is enabled. Adding to scheduler...")
        scheduler.add_job(
            async_job_wrapper(sync_entries, job_name="sync_entries job"),
            CronTrigger(**config.crons.sync_entries.trigger.model_dump()),
        )


    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    run_cron()
