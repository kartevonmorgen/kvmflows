from enum import StrEnum


class ReviewStatus(StrEnum):
    CREATED = 'created'
    CONFIRMED = 'confirmed'
    REJECTED = 'rejected'
    ARCHIVED = 'archived'