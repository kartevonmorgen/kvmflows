import asyncio
import httpx
import time

from typing import List, Dict, Any, Optional, Union, AsyncGenerator
from loguru import logger


class BulkHttpClient:
    def __init__(
        self,
        max_retries: int = 10,
        retry_delay: float = 1.0,
        concurrency: int = 15,
        timeout: float = 10.0,
    ) -> None:
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.concurrency = concurrency
        self.timeout = timeout
        self._async_client: Optional[httpx.AsyncClient] = None

    async def _get_async_client(self) -> httpx.AsyncClient:
        if self._async_client is None:
            self._async_client = httpx.AsyncClient(timeout=self.timeout)
        return self._async_client

    async def get_with_retries(
        self, url: str, **kwargs
    ) -> Union[Dict[str, Any], Exception]:
        start_time = time.time()
        client = await self._get_async_client()
        for attempt in range(self.max_retries):
            try:
                res = await client.get(url, **kwargs)
                res.raise_for_status()
                if "application/json" in res.headers.get("content-type", ""):
                    result = res.json()
                else:
                    result = {"text": res.text}
                duration = time.time() - start_time
                logger.debug(f"get_with_retries for {url} completed in {duration:.3f}s")
                return result
            except httpx.HTTPStatusError as e:
                logger.error(
                    f"HTTP error: {e}, Attempt {attempt + 1}/{self.max_retries}, URL: {url}"
                )
                if attempt + 1 == self.max_retries:
                    duration = time.time() - start_time
                    logger.warning(
                        f"get_with_retries for {url} failed after {duration:.3f}s"
                    )
                    return e
            except Exception as e:
                logger.error(
                    f"Error fetching {url}: {e}, Attempt {attempt + 1}/{self.max_retries}"
                )
                if attempt + 1 == self.max_retries:
                    duration = time.time() - start_time
                    logger.warning(
                        f"get_with_retries for {url} failed after {duration:.3f}s"
                    )
                    return e
            await asyncio.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
        duration = time.time() - start_time
        logger.warning(f"get_with_retries for {url} failed after {duration:.3f}s")
        return Exception(f"Failed to fetch {url} after {self.max_retries} attempts")

    async def get_with_semaphore(
        self, url: str, semaphore: asyncio.Semaphore, **kwargs
    ) -> Union[Dict[str, Any], Exception]:
        async with semaphore:
            return await self.get_with_retries(url, **kwargs)

    async def bulk_get(self, urls: List[str], **kwargs) -> List[Union[Any, Exception]]:
        start_time = time.time()
        semaphore = asyncio.Semaphore(self.concurrency)
        tasks = [self.get_with_semaphore(url, semaphore, **kwargs) for url in urls]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        duration = time.time() - start_time
        logger.debug(f"bulk_get for {len(urls)} URLs completed in {duration:.3f}s")
        return results

    async def bulk_get_generator(
        self, urls: List[str], **kwargs
    ) -> AsyncGenerator[Union[Dict[str, Any], List[Any], Exception], None]:
        """
        Generator version of bulk_get that yields results as they complete.
        More memory efficient for large numbers of URLs.

        Yields:
            Results as they complete
        """
        start_time = time.time()
        semaphore = asyncio.Semaphore(self.concurrency)
        tasks = []

        # Create tasks
        for url in urls:
            task = asyncio.create_task(
                self.get_with_semaphore(url, semaphore, **kwargs)
            )
            tasks.append(task)

        # Yield results as they complete
        pending = tasks[:]
        results_yielded = 0

        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for completed_task in done:
                result = completed_task.result()
                results_yielded += 1
                yield result

        duration = time.time() - start_time
        logger.debug(
            f"bulk_get_generator for {len(urls)} URLs completed in {duration:.3f}s"
        )

    async def bulk_get_stream(
        self, urls: List[str], batch_size: Optional[int] = None, **kwargs
    ) -> AsyncGenerator[Union[Dict[str, Any], Exception], None]:
        """
        Stream results in batches to balance memory usage and concurrency.

        Args:
            urls: List of URLs to fetch
            batch_size: Number of URLs to process at once (defaults to concurrency * 2)
            **kwargs: Additional arguments for requests

        Yields:
            Results from each batch as they complete
        """
        start_time = time.time()
        if batch_size is None:
            batch_size = self.concurrency * 2

        total_results = 0
        for i in range(0, len(urls), batch_size):
            batch = urls[i : i + batch_size]
            batch_results = await self.bulk_get(batch, **kwargs)
            for result in batch_results:
                total_results += 1
                yield result

        duration = time.time() - start_time
        logger.debug(
            f"bulk_get_stream for {len(urls)} URLs completed in {duration:.3f}s"
        )

    async def close_async(self) -> None:
        if self._async_client:
            await self._async_client.aclose()
            self._async_client = None

    async def __aenter__(self) -> "BulkHttpClient":
        await self._get_async_client()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close_async()


async def main():
    max_retries = 2
    retry_delay = 1.0
    timeout = 5.0
    concurrency = 15
    urls = ["https://httpbin.org/get"] * (concurrency + 5)

    async with BulkHttpClient(
        max_retries=max_retries,
        concurrency=concurrency,
        timeout=timeout,
        retry_delay=retry_delay,
    ) as client:
        logger.debug("Using bulk_get (loads all into memory):")
        results = await client.bulk_get(urls)
        for i, result in enumerate(results):
            pass

        logger.debug(
            "Using bulk_get_generator (memory efficient, results as they complete):"
        )
        async for result in client.bulk_get_generator(urls):
            pass

        logger.debug("Using bulk_get_stream (batch processing):")
        async for result in client.bulk_get_stream(urls, batch_size=5):
            pass


if __name__ == "__main__":
    asyncio.run(main())
