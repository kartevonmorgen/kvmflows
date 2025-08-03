"""
Email subscription service for sending periodic updates to users.

This module handles the complete flow of:
1. Fetching active subscriptions from the database
2. Finding relevant entries for each subscription
3. Rendering email templates with subscription data
4. Sending bulk emails via Mailgun
"""

from pathlib import Path
from typing import List
from datetime import datetime
from loguru import logger
from liquid import Template

from src.kvmflows.models.subscription import Subscription
from src.kvmflows.models.entries import Entry
from src.kvmflows.models.subscription_types import EntrySubscriptionType
from src.kvmflows.models.subscription_interval import SubscriptionInterval
from src.kvmflows.database.subscription import SubscriptionModel
from src.kvmflows.database.entry import Entry as EntryModel
from src.kvmflows.config.config import config
from src.kvmflows.mail.mailgun import MailgunSender, EmailMessage


def render_subscription_template(
    subscription: Subscription,
    entries: List[Entry],
    interval: str,
    domain: str,
    unsubscribe_link: str,
) -> str:
    """
    Render the subscription email template with the provided data.

    Args:
        subscription: The subscription object containing user preferences
        entries: List of entries to include in the email
        interval: The subscription interval (hourly, daily, weekly)
        domain: The domain for entry links
        unsubscribe_link: The unsubscribe link for the footer

    Returns:
        The rendered HTML/text content ready for email sending

    Raises:
        FileNotFoundError: If the template file doesn't exist
    """
    # Get the template path from config instead of hardcoded path
    template_path = Path(config.email.templates.subscription_email)

    if not template_path.exists():
        raise FileNotFoundError(f"Template not found at {template_path}")

    # Load template content
    with open(template_path, "r", encoding="utf-8") as f:
        template_content = f.read()

    # Initialize liquid template engine
    template = Template(template_content)

    # Prepare template variables
    context = _build_template_context(
        subscription=subscription,
        entries=entries,
        interval=interval,
        domain=domain,
        unsubscribe_link=unsubscribe_link,
    )

    # Render and return the email content
    return template.render(**context)


def _build_template_context(
    subscription: Subscription,
    entries: List[Entry],
    interval: str,
    domain: str,
    unsubscribe_link: str,
) -> dict:
    """
    Build the context dictionary for template rendering.

    Args:
        subscription: The subscription object
        entries: List of entries to include
        interval: Subscription interval
        domain: Domain for links
        unsubscribe_link: Unsubscribe URL

    Returns:
        Dictionary with template variables
    """
    return {
        "subscription": {
            "title": subscription.title,
            "id": subscription.id,
            "email": subscription.email,
        },
        "entries": [_format_entry_for_template(entry) for entry in entries],
        "interval": interval,
        "domain": domain,
        "unsubscribe_link": unsubscribe_link,
    }


def _format_entry_for_template(entry: Entry) -> dict:
    """
    Format a single entry for template rendering.

    Args:
        entry: The entry to format

    Returns:
        Dictionary with formatted entry data
    """
    # Build address line from available components
    address_parts = [entry.street or "", entry.zip or "", entry.city or ""]
    address_line = " ".join(part for part in address_parts if part).strip() or None

    return {
        "id": entry.id,
        "title": entry.title,
        "description": entry.description,
        "category": entry.categories[0] if entry.categories else None,
        "tags": ", ".join(entry.tags) if entry.tags else None,
        "address_line": address_line,
        "homepage": entry.homepage,
        "email": entry.email,
        "phone": entry.telephone,
    }


async def fetch_active_subscriptions() -> List[Subscription]:
    """
    Fetch all active subscriptions from the database.

    Returns:
        List of active subscription objects

    Note:
        Currently uses synchronous database queries. This can be optimized
        with proper async database operations in the future.
    """
    try:
        # Ensure database connection is available
        from src.kvmflows.database.db import db

        if db.is_closed():
            db.connect()

        # Query for all active subscriptions
        subscription_models = list(
            SubscriptionModel.select().where(SubscriptionModel.is_active)
        )

        # Convert database models to Pydantic objects
        subscriptions = [sub_model.to_pydantic() for sub_model in subscription_models]

        logger.info(f"Fetched {len(subscriptions)} active subscriptions")
        return subscriptions

    except Exception as e:
        logger.error(f"Error fetching active subscriptions: {e}")
        return []


async def fetch_entries_for_subscription(subscription: Subscription) -> List[Entry]:
    """
    Fetch entries that match a subscription's geographic bounds and time interval.

    Args:
        subscription: The subscription containing geographic and temporal filters

    Returns:
        List of entries matching the subscription criteria

    Note:
        Currently uses synchronous database queries. This can be optimized
        with proper async database operations in the future.
    """
    try:
        # Calculate time range based on subscription interval
        interval = SubscriptionInterval(subscription.interval)
        interval_datetimes = interval.passed_interval_datestime

        # Ensure database connection is available
        from src.kvmflows.database.db import db

        if db.is_closed():
            db.connect()

        # Query entries within geographic bounds and time range
        # Use updated_at for safer filtering as it reflects when entries were last modified
        entry_models = list(
            EntryModel.select().where(
                (EntryModel.lat >= subscription.lat_min)
                & (EntryModel.lat <= subscription.lat_max)
                & (EntryModel.lng >= subscription.lon_min)
                & (EntryModel.lng <= subscription.lon_max)
                & (
                    EntryModel.updated_at.is_null(False)
                )  # Ensure updated_at is not null
                # & (EntryModel.updated_at >= interval_datetimes.start_datetime)
                # & (EntryModel.updated_at < interval_datetimes.end_datetime)
            )
        )

        # Convert database models to Pydantic objects
        entries = [entry_model.to_pydantic() for entry_model in entry_models]

        logger.debug(
            f"Fetched {len(entries)} entries for subscription {subscription.id} "
            f"(lat: {subscription.lat_min}-{subscription.lat_max}, "
            f"lng: {subscription.lon_min}-{subscription.lon_max}, "
            f"time: {interval_datetimes.start_datetime} to {interval_datetimes.end_datetime})"
        )
        return entries

    except Exception as e:
        logger.error(f"Error fetching entries for subscription {subscription.id}: {e}")
        return []


async def send_subscription_emails():
    """
    Send subscription emails using the rendered template.

    This function implements the complete email subscription flow:
    1. Fetch all active subscriptions from the database
    2. For each subscription, fetch relevant entries based on location and time
    3. Render email templates with subscription and entry data
    4. Send emails in bulk using the Mailgun service

    The function tracks and logs success/failure statistics for monitoring.
    """
    logger.info("Starting subscription email sending process...")

    try:
        # Step 1: Fetch all active subscriptions
        subscriptions = await fetch_active_subscriptions()

        if not subscriptions:
            logger.info("No active subscriptions found")
            return

        logger.info(f"Processing {len(subscriptions)} active subscriptions")

        # Step 2-3: Process subscriptions and prepare emails
        email_messages, skipped_count = await _prepare_subscription_emails(
            subscriptions
        )

        # Step 4: Send emails in bulk if any were prepared
        if email_messages:
            await _send_bulk_emails(email_messages, skipped_count)
        else:
            logger.info("No emails to send - all subscriptions had no new entries")

    except Exception as e:
        logger.error(f"Critical error in send_subscription_emails: {e}")
        raise


async def _prepare_subscription_emails(
    subscriptions: List[Subscription],
) -> tuple[List[EmailMessage], int]:
    """
    Process subscriptions and prepare email messages for bulk sending.

    Args:
        subscriptions: List of active subscriptions to process

    Returns:
        Tuple of (email_messages, skipped_count)
    """
    email_messages = []
    skipped_count = 0

    for subscription in subscriptions:
        try:
            logger.debug(
                f"Processing subscription {subscription.id} for {subscription.email}"
            )

            # Fetch entries matching this subscription's criteria
            entries = await fetch_entries_for_subscription(subscription)

            if not entries:
                logger.debug(f"No new entries found for subscription {subscription.id}")
                skipped_count += 1
                continue

            logger.info(
                f"Found {len(entries)} new entries for subscription {subscription.id}"
            )

            # Create email message for this subscription
            message = _create_email_message(subscription, entries)
            email_messages.append(message)

        except Exception as e:
            logger.error(f"Error processing subscription {subscription.id}: {e}")
            skipped_count += 1
            continue

    return email_messages, skipped_count


def _create_email_message(
    subscription: Subscription, entries: List[Entry]
) -> EmailMessage:
    """
    Create an email message for a subscription with its entries.

    Args:
        subscription: The subscription to create email for
        entries: List of entries to include in the email

    Returns:
        EmailMessage ready for sending
    """
    # Render the email template with subscription data
    email_content = render_subscription_template(
        subscription=subscription,
        entries=entries,
        interval=subscription.interval,
        domain=config.email.domain,
        unsubscribe_link=config.email.unsubscribe_url.format(
            subscription_id=subscription.id
        ),
    )

    # Create and return the email message
    return EmailMessage(
        sender=config.email.area_subscription_creates.sender,
        to=subscription.email,
        subject=config.email.area_subscription_creates.subject,
        html=email_content,
    )


async def _send_bulk_emails(email_messages: List[EmailMessage], skipped_count: int):
    """
    Send a list of email messages in bulk and log results.

    Args:
        email_messages: List of emails to send
        skipped_count: Number of subscriptions that were skipped
    """
    logger.info(f"Sending {len(email_messages)} emails in bulk...")

    # Initialize the email sender
    sender = MailgunSender(domain=config.email.domain, api_key=config.email.api_key)

    try:
        # Send emails in bulk
        results = await sender.send_bulk_emails(email_messages)

        # Analyze and log results
        success_count, error_count = _analyze_email_results(results, email_messages)

        logger.success(
            f"Bulk email sending completed. "
            f"{success_count} successful, {error_count} failed, {skipped_count} skipped (no new entries)"
        )

    finally:
        # Always clean up the sender connection
        await sender.close_async()


def _analyze_email_results(
    results: List, email_messages: List[EmailMessage]
) -> tuple[int, int]:
    """
    Analyze email sending results and log individual failures.

    Args:
        results: List of results from bulk email sending
        email_messages: List of email messages that were sent

    Returns:
        Tuple of (success_count, error_count)
    """
    success_count = 0
    error_count = 0

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            error_count += 1
            logger.warning(f"Failed to send email to {email_messages[i].to}: {result}")
        else:
            success_count += 1
            logger.debug(f"Successfully sent email to {email_messages[i].to}")

    return success_count, error_count


def _create_mock_data():
    """Create mock data for testing the template rendering functionality."""
    # Create mock subscription data
    mock_subscription = Subscription(
        id="sub-123",
        title="Berlin Sustainability Events",
        email="test@example.com",
        lat_min=52.4,
        lon_min=13.3,
        lat_max=52.6,
        lon_max=13.5,
        interval="daily",
        subscription_type=EntrySubscriptionType.CREATES,
        created_at=datetime.now(),
    )

    # Create mock entries data
    mock_entries = [
        Entry(
            id="entry-001",
            created=datetime.now(),
            version=1,
            title="Green Community Garden",
            description="A wonderful community garden where locals grow organic vegetables and share knowledge about sustainable farming.",
            lat=52.5200,
            lng=13.4050,
            street="Alexanderplatz 1",
            zip="10178",
            city="Berlin",
            country="Germany",
            email="info@greengarden.org",
            telephone="+49 30 12345678",
            homepage="https://greengarden.org",
            license="CC0",
            categories=["garden", "sustainability"],
            tags=["organic", "community", "vegetables", "education"],
        ),
        Entry(
            id="entry-002",
            created=datetime.now(),
            version=1,
            title="Repair Café Berlin",
            description="Weekly meetings where people repair their broken items together, promoting sustainability and community spirit.",
            lat=52.5170,
            lng=13.3888,
            street="Unter den Linden 10",
            zip="10117",
            city="Berlin",
            country="Germany",
            email="contact@repaircafe-berlin.de",
            telephone="+49 30 87654321",
            homepage="https://repaircafe-berlin.de",
            license="CC0",
            categories=["repair", "sustainability"],
            tags=["repair", "community", "upcycling", "workshop"],
        ),
    ]

    return mock_subscription, mock_entries


def _test_template_rendering():
    """Test the template rendering functionality with mock data."""
    print("Testing template rendering...")

    mock_subscription, mock_entries = _create_mock_data()

    try:
        rendered_content = render_subscription_template(
            subscription=mock_subscription,
            entries=mock_entries,
            interval="daily",
            domain="kartevonmorgen.org",
            unsubscribe_link="https://kartevonmorgen.org/unsubscribe/sub-123",
        )

        print("Template rendered successfully!")
        print("=" * 50)
        print(rendered_content)
        print("=" * 50)

    except Exception as e:
        logger.error(f"Error rendering template: {e}")
        raise


# Uncomment the function below to test the complete email flow
# WARNING: This will send actual emails if configured
def _test_complete_flow():
    """Test the complete subscription email flow (SENDS ACTUAL EMAILS)."""
    import asyncio

    async def run_test():
        try:
            print("Testing complete subscription email flow...")
            await send_subscription_emails()
            print("Flow completed successfully!")
        except Exception as e:
            logger.error(f"Error in complete flow: {e}")
            raise

    asyncio.run(run_test())


if __name__ == "__main__":
    # Test template rendering with mock data
    # _test_template_rendering()

    # Uncomment the line below to test the complete flow (will send actual emails)
    _test_complete_flow()
