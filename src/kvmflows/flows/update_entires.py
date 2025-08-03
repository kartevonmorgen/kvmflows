import time
import asyncio
import numpy as np

from loguru import logger
from typing import List

from src.kvmflows.ofdb.entries import get_entries
from src.kvmflows.database.db import db
from src.kvmflows.config.config import config
from src.kvmflows.ofdb.search import SearchParams, search
from src.kvmflows.database.entry import Entry
from src.kvmflows.models.entries import Entry as PydanticEntry
from src.kvmflows.utils.memory_monitor import monitor_memory


@monitor_memory("update_entries")
async def update_entries():
    """
    Update entries by fetching data from OFDB and upserting to the database.
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


async def bulk_upsert_entries(pydantic_entries: List[PydanticEntry]) -> int:
    """
    Bulk upsert entries into the database.

    Args:
        pydantic_entries: List of Pydantic Entry models to upsert

    Returns:
        int: Number of entries successfully upserted
    """
    if not pydantic_entries:
        logger.debug("No entries to upsert")
        return 0

    logger.info(f"Starting bulk upsert of {len(pydantic_entries)} entries")

    try:
        # Convert Pydantic entries to database entries
        db_entries = [Entry.from_pydantic(entry) for entry in pydantic_entries]

        # Prepare data for bulk insert with conflict resolution
        entry_data = []
        for db_entry in db_entries:
            entry_dict = db_entry.to_dict()
            # Remove the updated_at field as it will be set by the trigger
            entry_dict.pop("updated_at", None)
            entry_data.append(entry_dict)

        if entry_data:
            # Use bulk insert with ON CONFLICT DO UPDATE
            # This will insert new entries or update existing ones based on the primary key (id)
            with db.atomic():
                # Build the conflict update dictionary for all fields except id and updated_at
                # We'll manually specify the fields to update
                update_fields = {
                    Entry.created: Entry.created,
                    Entry.version: Entry.version,
                    Entry.title: Entry.title,
                    Entry.description: Entry.description,
                    Entry.lat: Entry.lat,
                    Entry.lng: Entry.lng,
                    Entry.street: Entry.street,
                    Entry.zip: Entry.zip,
                    Entry.city: Entry.city,
                    Entry.country: Entry.country,
                    Entry.state: Entry.state,
                    Entry.contact_name: Entry.contact_name,
                    Entry.email: Entry.email,
                    Entry.telephone: Entry.telephone,
                    Entry.homepage: Entry.homepage,
                    Entry.opening_hours: Entry.opening_hours,
                    Entry.founded_on: str(Entry.founded_on),
                    Entry.license: Entry.license,
                    Entry.image_url: Entry.image_url,
                    Entry.image_link_url: Entry.image_link_url,
                    Entry.categories: Entry.categories,
                    Entry.tags: Entry.tags,
                    Entry.ratings: Entry.ratings,
                }

                query = Entry.insert_many(entry_data).on_conflict(
                    conflict_target=[Entry.id], update=update_fields
                )

                query.execute()
                logger.info(f"Successfully bulk upserted {len(entry_data)} entries")
                return len(entry_data)

    except Exception as e:
        logger.error(f"Error during bulk upsert: {e}")
        # Fallback to individual upserts using async operations
        logger.info("Falling back to individual async upserts")
        return await fallback_individual_upserts(pydantic_entries)

    return 0


async def fallback_individual_upserts(pydantic_entries: List[PydanticEntry]) -> int:
    """
    Fallback method to upsert entries individually using sync operations to avoid Windows async issues.

    Args:
        pydantic_entries: List of Pydantic Entry models to upsert

    Returns:
        int: Number of entries successfully upserted
    """
    success_count = 0
    error_count = 0

    # Use synchronous operations to avoid Windows aiopg connection issues

    try:
        # Ensure database connection is available
        if db.is_closed():
            db.connect()

        with db.atomic():
            for pydantic_entry in pydantic_entries:
                try:
                    # Convert to database entry
                    db_entry = Entry.from_pydantic(pydantic_entry)
                    entry_dict = db_entry.to_dict()
                    # Remove updated_at as it's handled by trigger
                    entry_dict.pop("updated_at", None)

                    # Try to update existing entry, insert if not exists
                    # Use raw SQL to handle upsert properly
                    updated_rows = (
                        Entry.update(**entry_dict)
                        .where(Entry.id == pydantic_entry.id)
                        .execute()
                    )

                    if updated_rows == 0:
                        # Entry doesn't exist, create it
                        Entry.create(**entry_dict)
                        logger.debug(f"Created entry: {pydantic_entry.id}")
                    else:
                        logger.debug(f"Updated entry: {pydantic_entry.id}")

                    success_count += 1

                except Exception as e:
                    logger.error(f"Error upserting entry {pydantic_entry.id}: {e}")
                    error_count += 1
                    # Continue with other entries instead of failing the entire batch
                    continue

    except Exception as e:
        logger.error(f"Database connection error during fallback upserts: {e}")
        # If sync operations also fail, try async one more time with better error handling
        return await safe_async_fallback_upserts(pydantic_entries)

    logger.info(
        f"Individual upsert completed: {success_count} successful, {error_count} errors"
    )
    return success_count


async def safe_async_fallback_upserts(pydantic_entries: List[PydanticEntry]) -> int:
    """
    Safe async fallback with proper connection management for Windows.

    Args:
        pydantic_entries: List of Pydantic Entry models to upsert

    Returns:
        int: Number of entries successfully upserted
    """
    success_count = 0
    error_count = 0

    for pydantic_entry in pydantic_entries:
        try:
            # Use a more robust approach with explicit connection handling
            try:
                # Try to get existing entry
                existing_entry = await Entry.aio_get_or_none(
                    Entry.id == pydantic_entry.id
                )

                if existing_entry:
                    # Update existing entry
                    for field_name, value in (
                        Entry.from_pydantic(pydantic_entry).to_dict().items()
                    ):
                        if (
                            field_name != "updated_at"
                        ):  # Skip updated_at as it's handled by trigger
                            setattr(existing_entry, field_name, value)
                    await existing_entry.aio_save()
                    logger.debug(f"Updated entry: {pydantic_entry.id}")
                else:
                    # Create new entry
                    new_entry = Entry.from_pydantic(pydantic_entry)
                    await new_entry.aio_save()
                    logger.debug(f"Created entry: {pydantic_entry.id}")

                success_count += 1

            except Exception as conn_error:
                # Log the specific connection error but continue
                logger.warning(
                    f"Connection error for entry {pydantic_entry.id}: {conn_error}"
                )
                error_count += 1
                # Small delay to allow connection cleanup
                await asyncio.sleep(0.01)

        except Exception as e:
            logger.error(f"Critical error upserting entry {pydantic_entry.id}: {e}")
            error_count += 1

    logger.info(
        f"Safe async upsert completed: {success_count} successful, {error_count} errors"
    )
    return success_count


if __name__ == "__main__":
    import asyncio

    logger.info("Starting entry update process")
    asyncio.run(update_entries())
    logger.info("Entry update process completed")
