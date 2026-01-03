"""Polars schema definitions for Forex Factory calendar data."""

import polars as pl


NUMERIC_STRUCT: pl.Struct = pl.Struct(
    [
        pl.Field("raw", pl.Utf8),
        pl.Field("value", pl.Float64),
        pl.Field("unit", pl.Utf8),
    ]
)

ForexFactoryDataframeSchema: dict = {
    "event_date": pl.Date,
    "event_time": pl.Time,
    "event_time_type": pl.Utf8,
    "currency": pl.Utf8,
    "impact": pl.Utf8,
    "name": pl.Utf8,
    "actual": NUMERIC_STRUCT,
    "forecast": NUMERIC_STRUCT,
    "previous": NUMERIC_STRUCT,
}
