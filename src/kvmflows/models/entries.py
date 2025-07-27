from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, field_validator


class Entry(BaseModel):
    id: str
    created: datetime
    version: int
    title: str
    description: str
    lat: float
    lng: float
    street: Optional[str] = None
    zip: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    state: Optional[str] = None
    contact_name: Optional[str] = None
    email: Optional[str] = None
    telephone: Optional[str] = None
    homepage: Optional[str] = None
    opening_hours: Optional[str] = None
    founded_on: Optional[str] = None
    license: str
    image_url: Optional[str] = None
    image_link_url: Optional[str] = None
    categories: List[str]
    tags: List[str]
    ratings: Optional[List[str]] = None

    @field_validator("founded_on", mode="before")
    @classmethod
    def founded_on_to_str(cls, v):
        if v is None:
            return v
        return str(v)
