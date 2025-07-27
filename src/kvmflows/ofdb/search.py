from pydantic import StrictStr, BaseModel, StrictInt
from typing import List, Optional, AsyncGenerator
from loguru import logger
from urllib.parse import urlencode

from src.kvmflows.models.search_entry import SearchEntry
from src.kvmflows.config.config import config
from src.kvmflows.clients.http_client import BulkHttpClient


class SearchResult(BaseModel):
    visible: List[SearchEntry] = []
    invisible: List[SearchEntry] = []


class SearchParams(BaseModel):
    bbox: StrictStr
    org_tag: Optional[StrictStr] = None
    categories: Optional[StrictStr] = None
    text: Optional[StrictStr] = None
    ids: Optional[StrictStr] = None
    tags: Optional[StrictStr] = None
    status: Optional[StrictStr] = None
    limit: Optional[StrictInt] = None

    def model_dump(self, **kwargs):
        # Always exclude None values by default
        kwargs.setdefault("exclude_none", True)
        return super().model_dump(**kwargs)


async def search(params: List[SearchParams]) -> AsyncGenerator[SearchResult, None]:
    async with BulkHttpClient(
        max_retries=config.ofdb.max_retries,
        retry_delay=config.ofdb.retry_delay,
        concurrency=config.ofdb.concurrency,
    ) as client:
        urls: List[str] = list(
            map(
                lambda p: f"{config.ofdb.url}/search?{urlencode(p.model_dump())}",
                params,
            )
        )
        async for response in client.bulk_get_generator(urls):
            yield SearchResult.model_validate(response)


async def test_search():
    bbox = "43.9137,-5.8227,55.3666,20.1489"
    async for result in search(params=[SearchParams(bbox=bbox)]):
        logger.info(f"Search result: {len(result.visible) if result else 0}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_search())
