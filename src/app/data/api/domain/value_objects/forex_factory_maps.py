"""Mapping constants used by Forex Factory scrapers."""

from src.app.data.api.domain.value_objects.forex_factory_impact import ForexFactoryImpact, ForexFactoryImpactIcon


FOREX_FACTORY_IMPACT_MAP: dict[ForexFactoryImpactIcon, ForexFactoryImpact] = {
    ForexFactoryImpactIcon.NON_ECONOMIC: ForexFactoryImpact.NON_ECONOMIC,
    ForexFactoryImpactIcon.LOW: ForexFactoryImpact.LOW,
    ForexFactoryImpactIcon.MEDIUM: ForexFactoryImpact.MEDIUM,
    ForexFactoryImpactIcon.HIGH: ForexFactoryImpact.HIGH,
}

MONTHS: dict[int, str] = {
    1: "jan",
    2: "feb",
    3: "mar",
    4: "apr",
    5: "may",
    6: "jun",
    7: "jul",
    8: "aug",
    9: "sep",
    10: "oct",
    11: "nov",
    12: "dec"
}
