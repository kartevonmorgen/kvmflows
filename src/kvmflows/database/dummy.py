from peewee import CharField, DateTimeField
from playhouse.postgres_ext import ArrayField
from datetime import datetime, timezone
from rich import print
import peewee_async

from src.kvmflows.database.db import async_db
from src.kvmflows.database.mixin.updated_at_trigger import UpdateAtTriggerMixin


class DummyModel(peewee_async.AioModel, UpdateAtTriggerMixin):
    name = CharField(unique=True)
    array_field = ArrayField(CharField)  # type: ignore
    updated_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        database = async_db
        table_name = "dummy"


if __name__ == "__main__":

    async def main():
        # Example of async operations
        try:
            # Create or update using async
            dummy = await DummyModel.aio_create(
                name="Test Dummy Async", array_field=["item1", "item2"]
            )
            print(f"Created: {dummy}")
        except Exception as e:
            print(f"Error: {e}")
            # Handle conflict - get existing and update
            dummy = await DummyModel.aio_get_or_none(
                DummyModel.name == "Test Dummy Async"
            )
            if dummy:
                dummy.array_field = ["item1", "item2", "item3"]  # type: ignore
                await dummy.aio_save()
                print(f"Updated: {dummy}")

    # For sync operations (backward compatibility)
    # Note: You need to import the sync model for these operations
    from src.kvmflows.database.db import db
    from peewee import Model

    class DummyModelSync(Model):
        name = CharField(unique=True)
        array_field = ArrayField(CharField)  # type: ignore
        updated_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

        class Meta:
            database = db
            table_name = "dummy"

    # on_conflict_replace() replaces the ENTIRE record
    DummyModelSync.insert(
        name="Test Dummy", array_field=["item1", "item2"]
    ).on_conflict(
        conflict_target=[DummyModelSync.name],
        update={DummyModelSync.array_field: ["item1", "item2", "item3"]},
    ).execute()

    # If you want to get the created/updated instance
    d = DummyModelSync.get(DummyModelSync.name == "Test Dummy")
    print(d)
