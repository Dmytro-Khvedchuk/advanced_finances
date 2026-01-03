"""Event time type enums for the Forex Factory calendar."""

from enum import StrEnum


class ForexEventTimeType(StrEnum):
    """Define the possible time classifications for calendar events."""
    EXACT = "exact"
    ALL_DAY = "all_day"
    TENTATIVE = "tentative"
    MULTI_DAY = "multi_day"
