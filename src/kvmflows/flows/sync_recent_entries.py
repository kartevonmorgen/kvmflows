from loguru import logger

from src.kvmflows.flows.bulk_upsert_entries import bulk_upsert_entries
from src.kvmflows.ofdb.recent_entries import get_recent_entries


async def sync_recent_entries():
    logger.info("Starting sync recent entries flow")
    entries = await get_recent_entries()
    await bulk_upsert_entries(entries)
    logger.success("Completed sync recent entries flow")


async def test_sync_recent_entries():
    await sync_recent_entries()


if __name__ == "__main__":
    import asyncio
    
    asyncio.run(test_sync_recent_entries())
