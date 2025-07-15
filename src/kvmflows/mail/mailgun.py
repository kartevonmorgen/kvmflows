import asyncio
import httpx
import time

from pydantic import BaseModel
from loguru import logger
from typing import List, Dict, Any, Optional, Union, cast
from sys import exit

from src.kvmflows.config.config import config


class EmailMessage(BaseModel):
    sender: str
    to: Union[str, List[str]]
    subject: str
    text: Optional[str] = None
    html: Optional[str] = None
    unsubscribe_link: Optional[str] = None


class MailgunSender:
    def __init__(
        self,
        domain: str,
        api_key: str,
        rate_limit: int = config.email.rate_limit,
        max_retries: int = config.email.max_retries,
        retry_delay: int = config.email.retry_delay,
    ):
        self.domain = domain
        self.api_key = api_key
        self.base_url = config.email.url
        self.auth = ("api", api_key)
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._client = None
        self._last_request_time = 0

    def __enter__(self):
        self._client = httpx.Client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            self._client.close()

    async def _get_async_client(self):
        if not hasattr(self, "_async_client") or self._async_client is None:
            self._async_client = httpx.AsyncClient()
        return self._async_client

    def _apply_rate_limit(self):
        """Simple rate limiting mechanism"""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        if time_since_last < (1 / self.rate_limit):
            sleep_time = (1 / self.rate_limit) - time_since_last
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    def send_email(self, message: EmailMessage) -> Dict[str, Any]:
        """Send a single email synchronously with retries"""
        if self._client is None:
            self._client = httpx.Client()

        data = {
            "from": message.sender,
            "to": message.to if isinstance(message.to, str) else ", ".join(message.to),
            "subject": message.subject,
            "text": message.text,
        }

        if message.html:
            data["html"] = message.html

        response: Optional[Dict[str, Any]] = None
        for attempt in range(self.max_retries):
            try:
                self._apply_rate_limit()
                res = self._client.post(self.base_url, auth=self.auth, data=data)
                res.raise_for_status()
                response = res.json()
                break
            except httpx.HTTPStatusError as e:
                logger.error(
                    f"HTTP error: {e}, Attempt {attempt + 1}/{self.max_retries}"
                )
                if attempt + 1 == self.max_retries:
                    raise
            except Exception as e:
                logger.error(
                    f"Error sending email: {e}, Attempt {attempt + 1}/{self.max_retries}"
                )
                if attempt + 1 == self.max_retries:
                    raise
            time.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff

        return cast(dict, response)

    async def send_email_async(self, message: EmailMessage) -> Dict[str, Any]:
        """Send a single email asynchronously with retries"""
        client = await self._get_async_client()

        data = {
            "from": message.sender,
            "to": message.to if isinstance(message.to, str) else ", ".join(message.to),
            "subject": message.subject,
            "text": message.text,
        }

        if message.html:
            data["html"] = message.html
        if message.unsubscribe_link:
            data["h:List-Unsubscribe"] = message.unsubscribe_link  # Add unsubscribe header
            
        response: Optional[Dict[str, Any]] = None
        for attempt in range(self.max_retries):
            try:
                # Apply rate limiting (cooperative in async context)
                current_time = time.time()
                time_since_last = current_time - self._last_request_time
                if time_since_last < (1 / self.rate_limit):
                    await asyncio.sleep((1 / self.rate_limit) - time_since_last)
                self._last_request_time = time.time()

                res = await client.post(self.base_url, auth=self.auth, data=data)
                res.raise_for_status()
                response = res.json()
                break
            except httpx.HTTPStatusError as e:
                logger.error(
                    f"HTTP error: {e}, Attempt {attempt + 1}/{self.max_retries}"
                )
                if attempt + 1 == self.max_retries:
                    raise
            except Exception as e:
                logger.error(
                    f"Error sending email: {e}, Attempt {attempt + 1}/{self.max_retries}"
                )
                if attempt + 1 == self.max_retries:
                    raise
            await asyncio.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
        
        return cast(dict, response)

    async def send_bulk_emails(
        self, messages: List[EmailMessage], concurrency: int = config.email.concurrency
    ) -> List[Union[Dict[str, Any], BaseException]]:
        """Send multiple emails concurrently with rate limiting"""
        semaphore = asyncio.Semaphore(concurrency)

        async def send_with_semaphore(message):
            async with semaphore:
                return await self.send_email_async(message)

        tasks = [send_with_semaphore(message) for message in messages]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def close_async(self):
        """Close the async client"""
        if hasattr(self, "_async_client") and self._async_client:
            await self._async_client.aclose()
            self._async_client = None


def test_send_email(sender: str, to: str, subject: str, text: str):
    """Legacy function for sending a single email"""
    message = EmailMessage(sender=sender, to=to, subject=subject, text=text)
    with MailgunSender(
        domain=config.email.domain, api_key=config.email.api_key
    ) as mail_sender:
        result = mail_sender.send_email(message)
        logger.info(f"Email sent: {result}")
        return result


async def test_send_email_async(sender: str, to: str, subject: str, text: str):
    """Example of sending an email asynchronously"""
    message = EmailMessage(
        sender=sender,
        to=to,
        subject=subject,
        # text=text
        html='<h1>Hello</h1><p>Hello, World!</p><a href="https://google.com">Unsubscribe</a>'
    )
    mail_sender = MailgunSender(domain=config.email.domain, api_key=config.email.api_key)
    try:
        result = await mail_sender.send_email_async(message)
        logger.info(f"Email sent: {result}")
        return result
    finally:
        await mail_sender.close_async()


async def test_send_many_emails(recipients: List[str], subject: str, text: str):
    """Example of sending emails to many recipients efficiently"""
    messages = [
        EmailMessage(
            sender=config.email.area_subscription_creates.sender,
            to=recipient,
            subject=subject,
            text=text,
            unsubscribe_link="https://example.com/unsubscribe",
        )
        for recipient in recipients
    ]

    sender = MailgunSender(domain=config.email.domain, api_key=config.email.api_key)
    try:
        results = await sender.send_bulk_emails(messages, concurrency=10)
        logger.info(
            f"Sent {len([r for r in results if not isinstance(r, Exception)])} emails successfully"
        )
        return results
    finally:
        await sender.close_async()


if __name__ == "__main__":
    recipient = config.email.test_email_recipient
    if recipient is None:
        logger.error("Please set the test_email_recipient in the config")
        exit(1)
    
    # Single email example
    # test_send_email(config.email.area_subscription.sender, recipient, "Hello", "Hello, World!")

    # Asynchronous ingle email example
    asyncio.run(test_send_email_async(
        config.email.area_subscription_creates.sender,
        recipient,
        "Hello",
        "Hello, World!",
        )
    )

    # Bulk email example
    # asyncio.run(test_send_many_emails(
    #     [recipient, recipient],
    #     'Bulk email test',
    #     'This is a test of bulk email sending'
    # ))