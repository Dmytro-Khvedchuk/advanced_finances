"""Impact enums used by Forex Factory calendar parsing."""

from enum import StrEnum


class ForexFactoryImpact(StrEnum):
    """Define impact levels used by Forex Factory events."""
    NON_ECONOMIC = "Non-Economic"
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"


class ForexFactoryImpactIcon(StrEnum):
    """Define CSS icon classes that represent impact levels."""
    NON_ECONOMIC = "icon--ff-impact-gra"
    LOW = "icon--ff-impact-yel"
    MEDIUM = "icon--ff-impact-ora"
    HIGH = "icon--ff-impact-red"
