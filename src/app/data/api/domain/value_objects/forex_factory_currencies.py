"""Currency enums used by the Forex Factory calendar."""

from enum import StrEnum


class ForexFactoryCurrencies(StrEnum):
    """Define supported currency codes for Forex Factory events."""
    USD = "USD" 
    EUR = "EUR" 
    GBP = "GBP"
    CAD = "CAD"
    CHF = "CHF"
    CNY = "CNY"
    NZD = "NZD"
    JPY = "JPY"
    AUD = "AUD"
    ALL = "All"
