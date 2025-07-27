from pydantic import BaseModel, StrictFloat, StrictInt
from typing import Optional, Union


class AvgRatings(BaseModel):
    """
    All average ratings of an entry.  # noqa: E501
    """
    total: Optional[Union[StrictFloat, StrictInt]] = None
    diversity: Optional[Union[StrictFloat, StrictInt]] = None
    fairness: Optional[Union[StrictFloat, StrictInt]] = None
    humanity: Optional[Union[StrictFloat, StrictInt]] = None
    renewable: Optional[Union[StrictFloat, StrictInt]] = None
    solidarity: Optional[Union[StrictFloat, StrictInt]] = None
    transparency: Optional[Union[StrictFloat, StrictInt]] = None
