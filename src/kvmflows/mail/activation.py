"""
Email activation utility for sending subscription activation emails.

This module handles rendering and sending activation emails using liquid templates.
"""

from pathlib import Path
from typing import Optional
from loguru import logger
from liquid import Template

from src.kvmflows.config.config import config
from src.kvmflows.mail.mailgun import MailgunSender, EmailMessage


async def send_activation_email(
    subscription_id: str,
    email: str,
    subscription_title: str,
    base_url: Optional[str] = None,
) -> bool:
    """
    Send an activation email to a new subscriber.

    Args:
        subscription_id: The unique ID of the subscription
        email: Email address of the subscriber
        subscription_title: Title of the subscription
        base_url: Base URL for the activation link. If None, defaults to localhost with app port

    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    try:
        # Construct activation link
        if base_url is None:
            base_url = f"http://localhost:{config.app.port}"

        activation_link = f"{base_url}/v1/subscriptions/{subscription_id}/activate"

        # Render the activation email template
        html_content = render_activation_template(
            activation_link=activation_link, subscription_title=subscription_title
        )

        # Create email message
        message = EmailMessage(
            sender=config.email.subscription_activation.sender,
            to=email,
            subject=config.email.subscription_activation.subject,
            html=html_content,
        )

        # Send email using MailgunSender
        sender = MailgunSender(domain=config.email.domain, api_key=config.email.api_key)
        try:
            await sender.send_email_async(message)
            logger.info(f"Activation email sent successfully to {email}")
            return True
        finally:
            await sender.close_async()

    except Exception as e:
        logger.error(f"Failed to send activation email to {email}: {e}")
        return False


def render_activation_template(activation_link: str, subscription_title: str) -> str:
    """
    Render the activation email template with the provided data.

    Args:
        activation_link: The complete activation URL
        subscription_title: Title of the subscription

    Returns:
        The rendered HTML content ready for email sending

    Raises:
        FileNotFoundError: If the template file doesn't exist
    """
    # Construct the absolute path to the template using config
    template_path = Path(config.email.templates.activation_email)

    if not template_path.exists():
        raise FileNotFoundError(f"Activation template not found at {template_path}")

    # Load template content
    with open(template_path, "r", encoding="utf-8") as f:
        template_content = f.read()

    # Initialize liquid template engine
    template = Template(template_content)

    # Prepare template variables
    context = {
        "activation_link": activation_link,
        "subscription_title": subscription_title,
    }

    # Render and return the email content
    return template.render(**context)
