
"""HTML class names used to parse the Forex Factory calendar."""

from enum import StrEnum


class ForexFactorySectionClasses(StrEnum):
    """Define HTML class names for calendar scraping."""
    TABLE = "calendar__table"
    NEW_DAY_SEPARATOR = "calendar__row--new-day"
    DAY_BREAKER_SEPARATOR = "calendar__row--day-breaker"
    CALENDAR_DATE = "calendar__date"
    DATE = "date"
    TIME = "calendar__time"
    CURRENCY = "calendar__currency"
    IMPACT = "calendar__impact"
    IMPACT_COMMON = "icon--ff-impact-"
    EVENT_TITLE = "calendar__event-title"
    ACTUAL = "calendar__actual"
    FORECAST = "calendar__forecast"
    PREVIOUS = "calendar__previous"
