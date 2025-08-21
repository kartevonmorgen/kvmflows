from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from src.kvmflows.flows.send_subscription_emails import send_subscription_emails
from src.kvmflows.models.subscription_interval import SubscriptionInterval
from src.kvmflows.models.subscription_types import EntrySubscriptionType
from src.kvmflows.config.config import config
from src.kvmflows.crons.utils import async_job_wrapper


def run_cron():
    """
    Set up and start a blocking scheduler to run jobs at specified times.
    """
    scheduler = BlockingScheduler(timezone="UTC")

    if config.crons.send_subscription_emails_hourly.enabled:
        logger.info("Hourly send email cron jobs are enabled. Adding to scheduler...")
        # Creates job
        scheduler.add_job(
            async_job_wrapper(
                lambda: send_subscription_emails(
                    SubscriptionInterval.HOURLY, EntrySubscriptionType.CREATES
                ),
                job_name="send_subscription_emails_hourly_creates job",
            ),
            CronTrigger(
                **config.crons.send_subscription_emails_hourly.trigger.model_dump()
            ),
        )
        # Updates job
        scheduler.add_job(
            async_job_wrapper(
                lambda: send_subscription_emails(
                    SubscriptionInterval.HOURLY, EntrySubscriptionType.UPDATES
                ),
                job_name="send_subscription_emails_hourly_updates job",
            ),
            CronTrigger(
                **config.crons.send_subscription_emails_hourly.trigger.model_dump()
            ),
        )

    if config.crons.send_subscription_emails_daily.enabled:
        logger.info("Daily send email cron jobs are enabled. Adding to scheduler...")
        # Creates job
        scheduler.add_job(
            async_job_wrapper(
                lambda: send_subscription_emails(
                    SubscriptionInterval.DAILY, EntrySubscriptionType.CREATES
                ),
                job_name="send_subscription_emails_daily_creates job",
            ),
            CronTrigger(
                **config.crons.send_subscription_emails_daily.trigger.model_dump()
            ),
        )
        # Updates job
        scheduler.add_job(
            async_job_wrapper(
                lambda: send_subscription_emails(
                    SubscriptionInterval.DAILY, EntrySubscriptionType.UPDATES
                ),
                job_name="send_subscription_emails_daily_updates job",
            ),
            CronTrigger(
                **config.crons.send_subscription_emails_daily.trigger.model_dump()
            ),
        )

    if config.crons.send_subscription_emails_weekly.enabled:
        logger.info("Weekly send email cron jobs are enabled. Adding to scheduler...")
        # Creates job
        scheduler.add_job(
            async_job_wrapper(
                lambda: send_subscription_emails(
                    SubscriptionInterval.WEEKLY, EntrySubscriptionType.CREATES
                ),
                job_name="send_subscription_emails_weekly_creates job",
            ),
            CronTrigger(
                **config.crons.send_subscription_emails_weekly.trigger.model_dump()
            ),
        )
        # Updates job
        scheduler.add_job(
            async_job_wrapper(
                lambda: send_subscription_emails(
                    SubscriptionInterval.WEEKLY, EntrySubscriptionType.UPDATES
                ),
                job_name="send_subscription_emails_weekly_updates job",
            ),
            CronTrigger(
                **config.crons.send_subscription_emails_weekly.trigger.model_dump()
            ),
        )

    if config.crons.send_subscription_emails_monthly.enabled:
        logger.info("Monthly send email cron jobs are enabled. Adding to scheduler...")
        # Creates job
        scheduler.add_job(
            async_job_wrapper(
                lambda: send_subscription_emails(
                    SubscriptionInterval.MONTHLY, EntrySubscriptionType.CREATES
                ),
                job_name="send_subscription_emails_monthly_creates job",
            ),
            CronTrigger(
                **config.crons.send_subscription_emails_monthly.trigger.model_dump()
            ),
        )
        # Updates job
        scheduler.add_job(
            async_job_wrapper(
                lambda: send_subscription_emails(
                    SubscriptionInterval.MONTHLY, EntrySubscriptionType.UPDATES
                ),
                job_name="send_subscription_emails_monthly_updates job",
            ),
            CronTrigger(
                **config.crons.send_subscription_emails_monthly.trigger.model_dump()
            ),
        )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    run_cron()
