import asyncio
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from src.kvmflows.flows.update_entires import update_entries


def async_job_wrapper(coro_func, job_name="async job"):
    """
    Generic wrapper to run an async coroutine as a scheduled job with logging.
    Args:
        coro_func: The coroutine function to run (no arguments)
        job_name: Name of the job for logging
    """

    def wrapper():
        logger.info(f"Starting scheduled {job_name}...")
        asyncio.run(coro_func())
        logger.info(f"Scheduled {job_name} completed.")

    return wrapper


def run_cron():
    """
    Set up and start a blocking scheduler to run jobs daily at specified times.
    Demonstrates passing parameters to scheduled async jobs.
    """
    scheduler = BlockingScheduler(timezone="UTC")

    # Example: schedule update_entries at midnight UTC with no params
    scheduler.add_job(
        async_job_wrapper(update_entries, job_name="update_entries job"),
        CronTrigger(hour=0, minute=0),
    )

    scheduler.add_job(
        async_job_wrapper(
            lambda: update_entries(),
            job_name="update_entries with params",
        ),
        CronTrigger(hour=1, minute=0),
    )

    logger.info("Scheduler started. update_entries will run daily at midnight UTC.")
    logger.info(
        "Scheduler started. update_entries with params will run daily at 1am UTC."
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    run_cron()
