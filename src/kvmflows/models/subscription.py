from pydantic import BaseModel, EmailStr
from typing import List, TypeAlias
from datetime import datetime

from src.kvmflows.models.subscription_types import EntrySubscriptionType


class Subscription(BaseModel):
    id: str
    title: str
    email: EmailStr
    lat_min: float
    lon_min: float
    lat_max: float
    lon_max: float
    interval: str
    subscription_type: EntrySubscriptionType
    created_at: datetime


Subscriptions: TypeAlias = List[Subscription]
