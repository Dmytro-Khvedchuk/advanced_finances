"""Value unit enums for Forex Factory numeric parsing."""

from enum import StrEnum


class ForexFactoryValueUnits(StrEnum):
    """Define supported numeric units for Forex Factory values."""
    PERCENT = "percent"
    NUMBER = "number"
    THOUSAND = "thousand"
    MILLION = "million"
    BILLION = "billion"
    TRILLION = "trillion"
    BASIS_POINTS = "basis_points"
    INDEX = "index"
    YIELD = "yield"
    UNKNOWN = "unknown"
