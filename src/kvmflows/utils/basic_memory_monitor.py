"""
Basic memory monitoring utilities without external dependencies.
"""

import asyncio
import gc
import tracemalloc
from typing import Dict, Optional
from loguru import logger
from dataclasses import dataclass
from datetime import datetime


@dataclass
class MemorySnapshot:
    """Represents a memory usage snapshot."""

    timestamp: datetime
    tracemalloc_current: int  # Current memory traced by tracemalloc
    tracemalloc_peak: int  # Peak memory traced by tracemalloc
    objects_count: int  # Number of objects in memory


class BasicMemoryMonitor:
    """Basic memory monitor without external dependencies."""

    def __init__(self, threshold_mb: float = 100.0):
        self.threshold_mb = threshold_mb
        self.snapshots: Dict[str, MemorySnapshot] = {}
        self.tracemalloc_started = False

    def start_tracemalloc(self):
        """Start memory tracing."""
        if not self.tracemalloc_started:
            tracemalloc.start()
            self.tracemalloc_started = True
            logger.info("Memory tracing started")

    def stop_tracemalloc(self):
        """Stop memory tracing."""
        if self.tracemalloc_started:
            tracemalloc.stop()
            self.tracemalloc_started = False
            logger.info("Memory tracing stopped")

    def take_snapshot(self, name: str) -> MemorySnapshot:
        """Take a memory snapshot."""
        tracemalloc_current = 0
        tracemalloc_peak = 0

        if self.tracemalloc_started:
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc_current = current
            tracemalloc_peak = peak

        objects_count = len(gc.get_objects())

        snapshot = MemorySnapshot(
            timestamp=datetime.now(),
            tracemalloc_current=tracemalloc_current,
            tracemalloc_peak=tracemalloc_peak,
            objects_count=objects_count,
        )

        self.snapshots[name] = snapshot
        current_mb = tracemalloc_current / 1024 / 1024
        peak_mb = tracemalloc_peak / 1024 / 1024
        logger.debug(
            f"Memory snapshot '{name}': Current={current_mb:.2f}MB, Peak={peak_mb:.2f}MB, Objects={objects_count}"
        )

        return snapshot

    def compare_snapshots(self, name1: str, name2: str) -> Optional[Dict[str, float]]:
        """Compare two memory snapshots."""
        if name1 not in self.snapshots or name2 not in self.snapshots:
            logger.warning(
                f"Cannot compare snapshots: '{name1}' or '{name2}' not found"
            )
            return None

        snap1 = self.snapshots[name1]
        snap2 = self.snapshots[name2]

        diff = {
            "tracemalloc_diff_mb": (
                snap2.tracemalloc_current - snap1.tracemalloc_current
            )
            / 1024
            / 1024,
            "objects_diff": snap2.objects_count - snap1.objects_count,
            "peak_diff_mb": (snap2.tracemalloc_peak - snap1.tracemalloc_peak)
            / 1024
            / 1024,
        }

        return diff

    def check_memory_leak(self, before_name: str, after_name: str) -> bool:
        """Check if there's a potential memory leak between two snapshots."""
        diff = self.compare_snapshots(before_name, after_name)
        if not diff:
            return False

        memory_increase = diff["tracemalloc_diff_mb"]
        objects_increase = diff["objects_diff"]

        if memory_increase > self.threshold_mb:
            logger.warning(
                f"Potential memory leak detected: Memory increased by {memory_increase:.2f}MB "
                f"and {objects_increase} objects between '{before_name}' and '{after_name}'"
            )
            return True

        return False

    def get_top_memory_consumers(self, limit: int = 10):
        """Get top memory consuming objects using tracemalloc."""
        if not self.tracemalloc_started:
            logger.warning("Tracemalloc not started. Call start_tracemalloc() first.")
            return

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")

        logger.info(f"Top {limit} memory consuming lines:")
        for index, stat in enumerate(top_stats[:limit], 1):
            logger.info(f"{index}. {stat}")

    def force_garbage_collection(self) -> int:
        """Force garbage collection and return number of collected objects."""
        before = len(gc.get_objects())
        collected = gc.collect()
        after = len(gc.get_objects())

        logger.debug(
            f"Garbage collection: {collected} cycles collected, {before - after} objects freed"
        )
        return collected

    async def periodic_monitoring(self, interval_seconds: int = 60):
        """Run periodic memory monitoring."""
        logger.info(
            f"Starting periodic memory monitoring every {interval_seconds} seconds"
        )

        while True:
            try:
                snapshot_name = f"periodic_{datetime.now().strftime('%H%M%S')}"
                snapshot = self.take_snapshot(snapshot_name)

                current_mb = snapshot.tracemalloc_current / 1024 / 1024
                if current_mb > self.threshold_mb:
                    logger.warning(f"High memory usage detected: {current_mb:.2f}MB")
                    self.force_garbage_collection()

                await asyncio.sleep(interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic monitoring: {e}")
                await asyncio.sleep(interval_seconds)


# Global memory monitor instance
memory_monitor = BasicMemoryMonitor(threshold_mb=200.0)  # 200MB threshold


def monitor_memory(func_name: Optional[str] = None):
    """Decorator to monitor memory usage of a function."""

    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            name = func_name or f"{func.__name__}"
            memory_monitor.take_snapshot(f"{name}_before")

            try:
                result = await func(*args, **kwargs)
                memory_monitor.take_snapshot(f"{name}_after")
                memory_monitor.check_memory_leak(f"{name}_before", f"{name}_after")
                return result
            except Exception as e:
                memory_monitor.take_snapshot(f"{name}_error")
                raise e

        def sync_wrapper(*args, **kwargs):
            name = func_name or f"{func.__name__}"
            memory_monitor.take_snapshot(f"{name}_before")

            try:
                result = func(*args, **kwargs)
                memory_monitor.take_snapshot(f"{name}_after")
                memory_monitor.check_memory_leak(f"{name}_before", f"{name}_after")
                return result
            except Exception as e:
                memory_monitor.take_snapshot(f"{name}_error")
                raise e

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

    return decorator


async def start_memory_monitoring():
    """Start the memory monitoring system."""
    memory_monitor.start_tracemalloc()
    monitoring_task = asyncio.create_task(memory_monitor.periodic_monitoring())
    logger.info("Basic memory monitoring system started")
    return monitoring_task


async def stop_memory_monitoring(monitoring_task):
    """Stop the memory monitoring system."""
    if monitoring_task and not monitoring_task.done():
        monitoring_task.cancel()
        try:
            await monitoring_task
        except asyncio.CancelledError:
            pass

    memory_monitor.stop_tracemalloc()
    logger.info("Basic memory monitoring system stopped")
