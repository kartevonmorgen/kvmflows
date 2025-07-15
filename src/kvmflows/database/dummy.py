from peewee import CharField, DateTimeField, Model
from playhouse.postgres_ext import ArrayField
from datetime import datetime, timezone
from rich import print

from src.kvmflows.database.db import db
from src.kvmflows.database.mixin.updated_at_trigger import UpdateAtTriggerMixin


class DummyModel(Model, UpdateAtTriggerMixin):
    name = CharField(unique=True)  
    array_field = ArrayField(CharField)  # type: ignore
    updated_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        database = db
        table_name = "dummy"


if __name__ == "__main__":
    # on_conflict_replace() replaces the ENTIRE record
    DummyModel.insert(
        name="Test Dummy", array_field=["item1", "item2"]
    ).on_conflict(
        conflict_target=[DummyModel.name],
        update={DummyModel.array_field: ["item1", "item2", "item3"]}
    ).execute()

    # If you want to get the created/updated instance
    d = DummyModel.get(DummyModel.name == "Test Dummy")
    print(d)
