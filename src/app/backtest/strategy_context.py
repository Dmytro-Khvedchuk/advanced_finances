"""The context of all strategies."""
from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class StrategyContext:
    """Shared context passed to all strategies."""
    price_df: pl.DataFrame
    news_df: pl.DataFrame | None = None
