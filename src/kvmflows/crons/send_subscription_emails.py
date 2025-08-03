from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from src.kvmflows.flows.send_subscription_emails import send_subscription_emails
from src.kvmflows.models.subscription_interval import SubscriptionInterval
from src.kvmflows.config.config import config
from src.kvmflows.crons.utils import async_job_wrapper


def run_cron():
    """
    Set up and start a blocking scheduler to run jobs at specified times.
    """
    scheduler = BlockingScheduler(timezone="UTC")

    if config.crons.send_subscription_emails_daily.enabled:
        logger.info("Daily send email cron job is enabled. Adding to scheduler...")
        scheduler.add_job(
            async_job_wrapper(lambda: send_subscription_emails(SubscriptionInterval.DAILY), job_name="send_subscription_emails_daily job"),
            CronTrigger(**config.crons.send_subscription_emails_daily.trigger.model_dump()),
        )
    
    if config.crons.send_subscription_emails_weekly.enabled:
        logger.info("Weekly send email cron job is enabled. Adding to scheduler...")
        scheduler.add_job(
            async_job_wrapper(lambda: send_subscription_emails(SubscriptionInterval.WEEKLY), job_name="send_subscription_emails_weekly job"),
            CronTrigger(**config.crons.send_subscription_emails_weekly.trigger.model_dump()),
        )
    
    if config.crons.send_subscription_emails_monthly.enabled:
        logger.info("Monthly send email cron job is enabled. Adding to scheduler...")
        scheduler.add_job(
            async_job_wrapper(lambda: send_subscription_emails(SubscriptionInterval.MONTHLY), job_name="send_subscription_emails_monthly job"),
            CronTrigger(**config.crons.send_subscription_emails_monthly.trigger.model_dump()),
        )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")