from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from src.kvmflows.flows.sync_recent_entries import sync_recent_entries
from src.kvmflows.config.config import config
from src.kvmflows.crons.utils import async_job_wrapper


def run_cron():
    scheduler = BlockingScheduler(timezone="UTC")
    if config.crons.sync_recent_entries.enabled:
        logger.info("Sync recent entries cron job is enabled. Adding to scheduler...")
        scheduler.add_job(
            async_job_wrapper(sync_recent_entries, job_name="sync_recent_entries job"),
            CronTrigger(**config.crons.sync_recent_entries.trigger.model_dump()),
        )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")

if __name__ == "__main__":
    run_cron()
