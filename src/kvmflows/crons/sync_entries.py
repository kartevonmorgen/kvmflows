from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from src.kvmflows.flows.sync_entires import sync_entries
from src.kvmflows.config.config import config
from src.kvmflows.crons.utils import async_job_wrapper


def run_cron():
    """
    Set up and start a blocking scheduler to run jobs specified times.
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
