import asyncio
import httpx

from typing import List, Dict, Any, Optional, Union
from loguru import logger


class BulkHttpClient:
    def __init__(
        self,
        max_retries: int = 10,
        retry_delay: float = 1.0,
        concurrency: int = 15,
        timeout: float = 10.0,
    ):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.concurrency = concurrency
        self.timeout = timeout
        self._async_client: Optional[httpx.AsyncClient] = None

    async def _get_async_client(self):
        if self._async_client is None:
            self._async_client = httpx.AsyncClient(timeout=self.timeout)
        return self._async_client

    async def get_with_retries(
        self, url: str, **kwargs
    ) -> Union[Dict[str, Any], Exception]:
        client = await self._get_async_client()
        for attempt in range(self.max_retries):
            try:
                res = await client.get(url, **kwargs)
                res.raise_for_status()
                if "application/json" in res.headers.get("content-type", ""):
                    return res.json()
                else:
                    return {"text": res.text}
            except httpx.HTTPStatusError as e:
                logger.error(
                    f"HTTP error: {e}, Attempt {attempt + 1}/{self.max_retries}, URL: {url}"
                )
                if attempt + 1 == self.max_retries:
                    return e
            except Exception as e:
                logger.error(
                    f"Error fetching {url}: {e}, Attempt {attempt + 1}/{self.max_retries}"
                )
                if attempt + 1 == self.max_retries:
                    return e
            await asyncio.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
        return Exception(f"Failed to fetch {url} after {self.max_retries} attempts")

    async def get_with_semaphore(self, url, semaphore: asyncio.Semaphore, **kwargs):
        async with semaphore:
            return await self.get_with_retries(url, **kwargs)

    async def bulk_get(self, urls: List[str], **kwargs) -> List[Union[Any, Exception]]:
        semaphore = asyncio.Semaphore(self.concurrency)
        tasks = [self.get_with_semaphore(url, semaphore, **kwargs) for url in urls]

        return await asyncio.gather(*tasks, return_exceptions=True)

    async def close_async(self):
        if self._async_client:
            await self._async_client.aclose()
            self._async_client = None

    async def __aenter__(self):
        await self._get_async_client()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close_async()


async def main():
    max_retries = 2
    retry_delay = 1.0
    timeout = 5.0
    concurrency = 15
    urls = ["https://httpbin.org/get"] * (concurrency + 5)
    async with BulkHttpClient(max_retries=max_retries, concurrency=concurrency, timeout=timeout, retry_delay=retry_delay) as client:
        results = await client.bulk_get(urls)
        for i, result in enumerate(results):
            print(f"Result {i + 1}:", result)


if __name__ == "__main__":
    asyncio.run(main())