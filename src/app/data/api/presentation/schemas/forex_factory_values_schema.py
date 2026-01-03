"""Pydantic schema for numeric values in Forex Factory events."""

from decimal import Decimal, InvalidOperation
from typing import Optional

from pydantic import BaseModel, Field

from src.app.data.api.domain.value_objects import ForexFactoryValueUnits


class ForexFactoryNumericValue(BaseModel):
    """Represent a parsed numeric value with units.

    Attributes:
        raw (str): The raw string as seen in the calendar.
        value (Decimal | None): The parsed numeric value.
        unit (ForexFactoryValueUnits): The unit classification for the value.
    """
    raw: str = Field(
        ...,
        description="The raw value.",
        examples=["0.3%", "250K", "1.63"]
    )                    
    value: Decimal | None = Field(
        None,
        description="The parsed value.",
        examples=[0.3, 250, 1.63]
    )
    unit: ForexFactoryValueUnits = Field(
        ...,
        description="The units of the value.",
        examples=[ForexFactoryValueUnits.TRILLION]
    )

    @staticmethod
    def parse_str_into_numerical(raw: str | None) -> Optional["ForexFactoryNumericValue"]:
        """Parse a raw Forex Factory numeric string into a structured value.
        
        Returns None for missing / non-numeric values.

        Args:
            raw (str | None): The raw value of the forex factory data.

        Returns:
            Optional["ForexFactoryNumericValue"]: the optional record with the ForexFactoryNumericValue schema.
        """
        if raw is None:
            return None

        raw = raw.strip()
        if raw in {"", "—", "-", "N/A"}:
            return None

        unit: ForexFactoryValueUnits
        numeric_part: str

        # --- Percent ---
        if raw.endswith("%"):
            unit = ForexFactoryValueUnits.PERCENT
            numeric_part = raw[:-1]

        # --- Thousand / Million / Billion / Trillion ---
        elif raw[-1].upper() in {
            "K": ForexFactoryValueUnits.THOUSAND,
            "M": ForexFactoryValueUnits.MILLION,
            "B": ForexFactoryValueUnits.BILLION,
            "T": ForexFactoryValueUnits.TRILLION,
        }:
            suffix = raw[-1].upper()
            unit = {
                "K": ForexFactoryValueUnits.THOUSAND,
                "M": ForexFactoryValueUnits.MILLION,
                "B": ForexFactoryValueUnits.BILLION,
                "T": ForexFactoryValueUnits.TRILLION,
            }[suffix]
            numeric_part = raw[:-1]

        # --- Plain numeric ---
        else:
            unit = ForexFactoryValueUnits.NUMBER
            numeric_part = raw

        try:
            return ForexFactoryNumericValue(
                raw=raw,
                value=Decimal(numeric_part),
                unit=unit,
            )
        except InvalidOperation:
            return None
