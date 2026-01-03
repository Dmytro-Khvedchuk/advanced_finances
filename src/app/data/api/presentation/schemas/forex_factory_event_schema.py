"""Pydantic schema for a Forex Factory calendar event."""

from datetime import date, datetime, time
from decimal import Decimal
from typing import Self

from pydantic import BaseModel, Field, field_validator, model_validator

from src.app.data.api.domain.value_objects import (
    ForexEventTimeType, ForexFactoryCurrencies, ForexFactoryImpact, ForexFactoryValueUnits
)
from src.app.data.api.presentation.schemas.forex_factory_values_schema import ForexFactoryNumericValue


class ForexFactoryEventSchema(BaseModel):
    """Represent a single Forex Factory calendar event.

    Attributes:
        event_date (date | str): The date of the event.
        event_time (time | None | str): The time of the event when exact.
        event_time_type (ForexEventTimeType): The time classification for the event.
        currency (ForexFactoryCurrencies): The impacted currency code.
        impact (ForexFactoryImpact): The impact level for the event.
        name (str): The event title.
        actual (ForexFactoryNumericValue | None): The actual reported value.
        forecast (ForexFactoryNumericValue | None): The forecasted value.
        previous (ForexFactoryNumericValue | None): The prior reported value.
    """
    
    event_date: date | str = Field(
        ...,
        description="The date of the event.",
        examples=[date(2022, 1, 22)]
    )
    event_time: time | None | str = Field(
        None,
        description="The time of the event.",
        examples=[time(22, 22, 22)]
    )
    event_time_type: ForexEventTimeType = Field(
        ...,
        description="The event time type.",
        examples=[ForexEventTimeType.ALL_DAY]
    )
    currency: ForexFactoryCurrencies = Field(
        ...,
        description="The impacted currency.",
        examples=[ForexFactoryCurrencies.USD]
    )
    impact: ForexFactoryImpact = Field(
        ...,
        description="The impact for the currency.",
        examples=[ForexFactoryImpact.HIGH]
    )
    name: str = Field(
        ...,
        description="The name of the event.",
        examples=["ISM Manufacturing PMI"]
    )
    actual: ForexFactoryNumericValue | None = Field(
        None,
        description="The actual value of the event.",
        examples=[
            ForexFactoryNumericValue(
                raw="0.4K",
                value=Decimal(0.4),
                unit=ForexFactoryValueUnits.THOUSAND
            )
        ]
    )
    forecast: ForexFactoryNumericValue | None = Field(
        None,
        description="The forecasted value of the event.",
        examples=[
            ForexFactoryNumericValue(
                raw="0.4K",
                value=Decimal(0.4),
                unit=ForexFactoryValueUnits.THOUSAND
            )
        ]
    )
    previous: ForexFactoryNumericValue | None = Field(
        None,
        description="The previous value of the event.",
        examples=[
            ForexFactoryNumericValue(
                raw="0.4K",
                value=Decimal(0.4),
                unit=ForexFactoryValueUnits.THOUSAND
            )
        ]
    )

    # === Validators ===

    @field_validator("event_date", mode="before")
    @classmethod
    def parse_date(cls, value: str | date) -> date:
        """Parses date from string to datetime.
        
        Args:
            value (str | date): The date value from forex_factory.
        
        Returns:
            date: Parsed date value.
        """
        if isinstance(value, str):
            return datetime.strptime(value, "%b %d %Y %z").date()
        return value
    
    @field_validator("event_time", mode="before")
    @classmethod
    def parse_time(cls, value: str | time | None) -> time | None:
        """Parses the time into a unified format.

        Args:
            value (str | time | None): The time value from forex_factory.

        Returns:
            time | None: The parsed time value if the value is not None, otherwise None.
        
        Raises:
            ValueError: If time format is invalid.
        """
        if value is None:
            return None
        if not isinstance(value, str):
            return value

        value = value.strip().lower()

        if value in {"all day", "tentative"} or value.startswith("day"):
            return None

        try:
            return datetime.strptime(value, "%I:%M%p %z").time()
        except ValueError:
            try:
                return datetime.strptime(value, "%H:%M %z").time()
            except ValueError as exc:
                raise ValueError(f"Invalid time format: {value}, {exc}") from exc  # TODO: add custom exception.

    @model_validator(mode="after")
    def validate_event_time_consistency(self) -> Self:
        """Validates the `event_time_type` value.
        
        Returns:
            self: Returns model if consistency is held.
        
        Raises:
            ValueError: If the event_time_type and event_time are not consistent.
        """
        if self.event_time_type == ForexEventTimeType.EXACT:
            if self.event_time is None:
                raise ValueError("event_time must be set when event_time_type is EXACT")
        elif self.event_time is not None:
            raise ValueError("event_time must be None unless event_time_type is EXACT")
        return self
