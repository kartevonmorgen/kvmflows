import uuid

from peewee import CharField, FloatField, DateTimeField, BooleanField, UUIDField
from peewee_async import AioModel
from datetime import datetime, timezone
from typing import Dict, Any

from src.kvmflows.database.db import async_db
from src.kvmflows.database.mixin.updated_at_trigger import UpdateAtTriggerMixin
from src.kvmflows.models.subscription import Subscription
from src.kvmflows.models.subscription_types import EntrySubscriptionType


def utc_now():
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc)


class SubscriptionModel(AioModel, UpdateAtTriggerMixin):
    id = UUIDField(primary_key=True, default=uuid.uuid4)
    title: CharField = CharField()
    email: CharField = CharField()
    lat_min: FloatField = FloatField()
    lon_min: FloatField = FloatField()
    lat_max: FloatField = FloatField()
    lon_max: FloatField = FloatField()
    interval: CharField = CharField()
    subscription_type: CharField = CharField()
    is_active: BooleanField = BooleanField(default=True)
    language: CharField = CharField(default="en")
    created_at: DateTimeField = DateTimeField(default=utc_now)
    updated_at: DateTimeField = DateTimeField(default=utc_now)

    class Meta:
        database = async_db
        table_name = "subscriptions"

    @classmethod
    def from_pydantic(cls, subscription: Subscription) -> "SubscriptionModel":
        return cls(
            id=subscription.id if subscription.id is not None else str(uuid.uuid4()),
            title=subscription.title,
            email=str(subscription.email),
            lat_min=subscription.lat_min,
            lon_min=subscription.lon_min,
            lat_max=subscription.lat_max,
            lon_max=subscription.lon_max,
            interval=subscription.interval,
            subscription_type=subscription.subscription_type.value,
            created_at=subscription.created_at,
        )

    def to_pydantic(self) -> Subscription:
        return Subscription(
            id=str(getattr(self, "id")),
            title=getattr(self, "title"),
            email=getattr(self, "email"),
            lat_min=getattr(self, "lat_min"),
            lon_min=getattr(self, "lon_min"),
            lat_max=getattr(self, "lat_max"),
            lon_max=getattr(self, "lon_max"),
            interval=getattr(self, "interval"),
            subscription_type=EntrySubscriptionType(getattr(self, "subscription_type")),
            created_at=getattr(self, "created_at"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "email": self.email,
            "lat_min": self.lat_min,
            "lon_min": self.lon_min,
            "lat_max": self.lat_max,
            "lon_max": self.lon_max,
            "interval": self.interval,
            "subscription_type": self.subscription_type,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    def to_subscription_response(self):
        """Convert to SubscriptionResponse model for API responses."""
        from src.kvmflows.models.subscription_interval import SubscriptionInterval
        from src.kvmflows.models.subscription_types import EntrySubscriptionType
        from src.kvmflows.models.supported_languages import SupportedLanguages

        # Import here to avoid circular imports
        from src.kvmflows.apis.router.v1.subscription.router import SubscriptionResponse

        return SubscriptionResponse(
            id=self.id,  # type: ignore
            title=self.title,  # type: ignore
            email=self.email,  # type: ignore
            lat_min=self.lat_min,  # type: ignore
            lon_min=self.lon_min,  # type: ignore
            lat_max=self.lat_max,  # type: ignore
            lon_max=self.lon_max,  # type: ignore
            interval=SubscriptionInterval(self.interval),  # type: ignore
            subscription_type=EntrySubscriptionType(self.subscription_type),  # type: ignore
            language=SupportedLanguages(self.language),  # type: ignore
            is_active=self.is_active,  # type: ignore
        )

    def set_active(self, active: bool):
        """Set the is_active status."""
        setattr(self, "is_active", active)

    def get_is_active(self) -> bool:
        """Get the is_active status."""
        return getattr(self, "is_active")
