from enum import StrEnum


class EntrySubscriptionType(StrEnum):
    CREATES = "creates"
    UPDATES = "updates"
    TAGS = "tags"
