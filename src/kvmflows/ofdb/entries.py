from itertools import islice
from typing import List, AsyncGenerator
from loguru import logger

from src.kvmflows.clients.http_client import BulkHttpClient
from src.kvmflows.config.config import config
from src.kvmflows.models.entries import Entry


async def get_entries(
    entry_ids: List[str],
    chunk_size: int = 100
) -> AsyncGenerator[List[Entry], None]:
    async with BulkHttpClient(
        max_retries=config.ofdb.max_retries,
        retry_delay=config.ofdb.retry_delay,
        concurrency=config.ofdb.concurrency,
    ) as client:
        it = iter(entry_ids)
        id_strs = [list(islice(it, chunk_size)) for _ in range(0, len(entry_ids), chunk_size)]
        urls = [
            f"{config.ofdb.url}/entries/{','.join(ids)}"
            for ids in id_strs
        ]
        async for response in client.bulk_get_generator(urls):
            if isinstance(response, Exception):
                logger.error(f"Error fetching entries: {response}")
                yield []
                continue

            entries = [Entry.model_validate(entry) for entry in response]
            yield entries


async def test_get_entries():
    entry_ids = [
        '6279e9e718654712877de30b411860dc',
        'e3909087cbc04853b5e32067f9a1a3d0',
        'c43c62da8b914803965fed05f27525fc',
        '96809b0390c746aa95348a1a7190b90e',
        'e95e5f113bc248c0b2ecd92fb03ff6d1',
        '972b31a1e86f4184b2aa450e78114e76',
        '918fb998bb5740d7a674ad4b1584f353',
        '3e5df0619c5140f8af1512ff8648cc40',
        'c4b02aef6540497399819865dd5e5f67',
        'a3ed93f5f4df415c8314d34ac0031f62',
        'ee34f7656d6e4cb283d36ae8fb87a311',
        'f6b479c4210c45c1b0282138ebde8f2f',
        '4c20979fe0754e74875afa4308d73ce7',
        'fe987e15372f4b1da99e29cf645a13f3',
        '2fce21634d7c4c2b8308f0c7e76b3fb8',
        '3a39215365d34944a94a09e127fea46f',
        '64f3d2789ee4422b8305916a75e02cfd'
    ]
    async for entries in get_entries(entry_ids, chunk_size=3):
        logger.info(f"Fetched {len(entries)} entries")
        for entry in entries:
            logger.info(f"Entry ID: {entry.id}, Title: {entry.title}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_get_entries())