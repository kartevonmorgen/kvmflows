from peewee import CharField, FloatField, IntegerField, DateTimeField
from peewee_async import AioModel
from playhouse.postgres_ext import ArrayField
from datetime import datetime, timezone
from typing import Dict, Any

from src.kvmflows.database.db import async_db
from src.kvmflows.database.mixin.updated_at_trigger import UpdateAtTriggerMixin
from src.kvmflows.models.entries import Entry as PydanticEntry


def utc_now():
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc)


class Entry(AioModel, UpdateAtTriggerMixin):
    id = CharField(primary_key=True)
    created = DateTimeField()
    version = IntegerField()
    title = CharField()
    description = CharField()
    lat = FloatField(constraints=[FloatField.constraints.between(-90, 90)])
    lng = FloatField(constraints=[FloatField.constraints.between(-180, 180)])
    street = CharField(null=True)
    zip = CharField(null=True)
    city = CharField(null=True)
    country = CharField(null=True)
    state = CharField(null=True)
    contact_name = CharField(null=True)
    email = CharField(null=True)
    telephone = CharField(null=True)
    homepage = CharField(null=True)
    opening_hours = CharField(null=True)
    founded_on = CharField(null=True)
    license = CharField()
    image_url = CharField(null=True)
    image_link_url = CharField(null=True)
    categories = ArrayField(CharField)  # type: ignore
    tags = ArrayField(CharField)  # type: ignore
    ratings = ArrayField(CharField, null=True)  # type: ignore
    updated_at = DateTimeField(default=utc_now)

    class Meta:
        database = async_db
        table_name = "entry"

    @classmethod
    def from_pydantic(cls, entry: PydanticEntry) -> "Entry":
        """Create a database Entry from a Pydantic Entry model."""
        return cls(
            id=entry.id,
            created=entry.created,
            version=entry.version,
            title=entry.title,
            description=entry.description,
            lat=entry.lat,
            lng=entry.lng,
            street=entry.street,
            zip=entry.zip,
            city=entry.city,
            country=entry.country,
            state=entry.state,
            contact_name=entry.contact_name,
            email=entry.email,
            telephone=entry.telephone,
            homepage=entry.homepage,
            opening_hours=entry.opening_hours,
            founded_on=entry.founded_on,
            license=entry.license,
            image_url=entry.image_url,
            image_link_url=entry.image_link_url,
            categories=entry.categories,
            tags=entry.tags,
            ratings=entry.ratings,
        )

    def to_pydantic(self) -> PydanticEntry:
        """Convert this database Entry to a Pydantic Entry model."""
        return PydanticEntry(
            id=getattr(self, "id"),
            created=getattr(self, "created"),
            version=getattr(self, "version"),
            title=getattr(self, "title"),
            description=getattr(self, "description"),
            lat=getattr(self, "lat"),
            lng=getattr(self, "lng"),
            street=getattr(self, "street"),
            zip=getattr(self, "zip"),
            city=getattr(self, "city"),
            country=getattr(self, "country"),
            state=getattr(self, "state"),
            contact_name=getattr(self, "contact_name"),
            email=getattr(self, "email"),
            telephone=getattr(self, "telephone"),
            homepage=getattr(self, "homepage"),
            opening_hours=getattr(self, "opening_hours"),
            founded_on=getattr(self, "founded_on"),
            license=getattr(self, "license"),
            image_url=getattr(self, "image_url"),
            image_link_url=getattr(self, "image_link_url"),
            categories=getattr(self, "categories"),
            tags=getattr(self, "tags"),
            ratings=getattr(self, "ratings"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert this Entry to a dictionary."""
        return {
            "id": self.id,
            "created": self.created,
            "version": self.version,
            "title": self.title,
            "description": self.description,
            "lat": self.lat,
            "lng": self.lng,
            "street": self.street,
            "zip": self.zip,
            "city": self.city,
            "country": self.country,
            "state": self.state,
            "contact_name": self.contact_name,
            "email": self.email,
            "telephone": self.telephone,
            "homepage": self.homepage,
            "opening_hours": self.opening_hours,
            "founded_on": self.founded_on,
            "license": self.license,
            "image_url": self.image_url,
            "image_link_url": self.image_link_url,
            "categories": self.categories,
            "tags": self.tags,
            "ratings": self.ratings,
            "updated_at": self.updated_at,
        }
