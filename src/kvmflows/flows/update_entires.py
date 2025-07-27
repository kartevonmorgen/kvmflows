import numpy as np

from loguru import logger
from typing import List, AsyncGenerator

from src.kvmflows.config.config import config
from src.kvmflows.ofdb.search import SearchParams, search


async def update_entries():
    for area in config.areas:
        logger.info(f"Fetching entries for area: {area.name}")
        lats = np.linspace(area.lats[0], area.lats[1], num=area.lat_n_chunks)
        lngs = np.linspace(area.lngs[0], area.lngs[1], num=area.lng_n_chunks)

        search_params: List[SearchParams] = []
        for i in range(area.lat_n_chunks-1):
            for j in range(area.lng_n_chunks-1):
                bbox = f"{lats[i]},{lngs[j]},{lats[i+1]},{lngs[j+1]}"
                search_params.append(SearchParams(bbox=bbox))
        
        async for search_result in search(search_params):
            if search_result.visible:
                logger.debug(f"Found {len(search_result.visible)} visible entries")
                for entry in search_result.visible:
                    logger.info(f"Entry ID: {entry.id}, Title: {entry.title}")
            else:
                logger.debug("No visible entries found in this area chunk")