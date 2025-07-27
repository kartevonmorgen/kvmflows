from pydantic import BaseModel, Field, StringConstraints, StrictStr
from typing import List, Optional, Union, Annotated

from src.kvmflows.models.avg_ratings import AvgRatings
from src.kvmflows.models.review_status import ReviewStatus


class SearchEntry(BaseModel):
    """
    The compact view of an entry as returned in search results.  # noqa: E501
    """
    id: Optional[Annotated[str, StringConstraints(strict=True, max_length=32, min_length=32)]] = Field(default=None, description="Identifier of a resource ")
    status: Optional[ReviewStatus] = None
    lat: Optional[Union[Annotated[float, Field(le=90, ge=-90, strict=True)], Annotated[int, Field(le=90, ge=-90, strict=True)]]] = Field(default=None, description="Geographic latitude (in degrees)")
    lng: Optional[Union[Annotated[float, Field(le=180, ge=-180, strict=True)], Annotated[int, Field(le=180, ge=-180, strict=True)]]] = Field(default=None, description="Geographic longitude (in degrees)")
    title: Optional[StrictStr] = None
    description: Optional[StrictStr] = None
    categories: Optional[Annotated[List[StrictStr], Field()]] = None
    tags: Optional[Annotated[List[Annotated[str, StringConstraints(strict=True, min_length=1)]], Field()]] = None
    ratings: Optional[AvgRatings] = None
