import time
import asyncio
import numpy as np

from loguru import logger
from typing import List

from kvmflows.flows.bulk_upsert_entries import bulk_upsert_entries
from src.kvmflows.ofdb.entries import get_entries
from src.kvmflows.config.config import config
from src.kvmflows.ofdb.search import SearchParams, search
from src.kvmflows.utils.memory_monitor import monitor_memory


@monitor_memory("sync_all_entries")
async def sync_all_entries():
    """
    Sync all entries by fetching data from OFDB and upserting to the database.
    """

    start_time = time.time()

    # Create tasks for each area to process them concurrently
    area_tasks = []
    for area in config.areas:
        task = asyncio.create_task(process_area(area))
        area_tasks.append(task)

    # Wait for all area processing tasks to complete
    area_results = await asyncio.gather(*area_tasks, return_exceptions=True)

    # Aggregate results
    total_upserted = 0
    max_numbers = 0
    successful_areas = 0

    for i, result in enumerate(area_results):
        if isinstance(result, Exception):
            logger.error(f"Error processing area {config.areas[i].name}: {result}")
        elif isinstance(result, tuple) and len(result) == 2:
            area_upserted, area_max_numbers = result
            total_upserted += area_upserted
            max_numbers = max(max_numbers, area_max_numbers)
            successful_areas += 1
        else:
            logger.error(
                f"Unexpected result format for area {config.areas[i].name}: {result}"
            )
            # Treat as failed area

    elapsed = time.time() - start_time
    logger.success(
        f"Update completed. Total entries upserted: {total_upserted} from {successful_areas}/{len(config.areas)} areas. Elapsed time: {elapsed:.2f} seconds"
    )
    logger.info(f"Max visible entries in a chunk: {max_numbers}")

    # Force garbage collection to help with memory cleanup
    import gc

    gc.collect()


@monitor_memory("process_area")
async def process_area(area):
    """
    Process a single area by fetching and upserting its entries.

    Args:
        area: Area configuration object

    Returns:
        tuple: (upserted_count, max_numbers_in_chunk)
    """
    logger.info(f"Fetching entries for area: {area.name}")
    lats = np.linspace(area.lats[0], area.lats[1], num=area.lat_n_chunks)
    lngs = np.linspace(area.lngs[0], area.lngs[1], num=area.lng_n_chunks)

    search_params: List[SearchParams] = []
    for i in range(area.lat_n_chunks - 1):
        for j in range(area.lng_n_chunks - 1):
            bbox = f"{lats[i]},{lngs[j]},{lats[i + 1]},{lngs[j + 1]}"
            search_params.append(SearchParams(bbox=bbox))

    area_upserted = 0
    max_numbers = 0

    async for search_result in search(search_params):
        if not search_result.visible:
            # logger.debug("No visible entries found in this area chunk")
            continue

        max_numbers = max(max_numbers, len(search_result.visible))

        entry_ids = list(map(lambda e: e.id, search_result.visible))

        try:
            async for entry_result in get_entries(entry_ids):  # type: ignore
                if not entry_result:
                    logger.debug("No entries found for the given IDs")
                    continue

                upserted_count = await bulk_upsert_entries(entry_result)
                area_upserted += upserted_count
                logger.debug(f"Bulk upserted {upserted_count} entries in this chunk")
        except Exception as e:
            logger.error(f"Error processing entries for area {area.name}: {e}")
            # Continue with next chunk instead of failing completely
            continue

    logger.success(f"Completed area {area.name}: {area_upserted} entries upserted")
    return area_upserted, max_numbers




if __name__ == "__main__":
    import asyncio

    logger.info("Starting entry update process")
    asyncio.run(sync_all_entries())
    logger.info("Entry update process completed")
