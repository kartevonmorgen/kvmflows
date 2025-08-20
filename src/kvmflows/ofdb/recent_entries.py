import httpx

from typing import Any, List, Final, Optional
from loguru import logger
from rich import print

from src.kvmflows.config.config import config
from src.kvmflows.models.entries import Entry


async def get_recent_entries(
    since: Optional[int] = None,
    until: Optional[int] = None,
    with_ratings: bool = True,
    limit: int = 1000,
    offset: int = 0,
) -> List[Entry]:
    logger.info(
        f"Fetching recent entries since {since}, until {until}, with_ratings={with_ratings}, limit={limit}, offset={offset}"
    )
    async with httpx.AsyncClient() as client:
        url: Final[str] = f"{config.ofdb.url}/entries/recently-changed"
        params = {
            "with_ratings": with_ratings,
            "limit": limit,
            "offset": offset,
            "since": since,
            "until": until,
        }

        response = await client.get(url, params=params)
        data: List[Any] = list()
        try:
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error(f"Error fetching recent entries: {e}")
            return []

        entries: List[Entry] = []
        try:
            entries = [Entry.model_validate(item) for item in data]
        except Exception as e:
            logger.error(f"Error processing recent entries: {e}")
            return []

        # Remove duplicates based on ID, keeping only the first occurrence
        unique_entries = _deduplicate_entries_by_id(entries)

        logger.success(
            f"Fetched {len(entries)} recent entries, {len(unique_entries)} unique entries"
        )
        return unique_entries


def _deduplicate_entries_by_id(entries: List[Entry]) -> List[Entry]:
    """
    Remove duplicate entries based on ID, keeping only the first occurrence.
    Time complexity: O(n), Space complexity: O(n)
    """
    seen_ids = set()
    unique_entries = []

    for entry in entries:
        if entry.id not in seen_ids:
            seen_ids.add(entry.id)
            unique_entries.append(entry)

    return unique_entries


async def test_get_recent_entries():
    entries = await get_recent_entries()
    assert isinstance(entries, list)
    if entries:
        assert isinstance(entries[0], Entry)

    print(entries[0])


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_get_recent_entries())
