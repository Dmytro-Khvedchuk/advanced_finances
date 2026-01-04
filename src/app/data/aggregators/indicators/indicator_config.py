"""The config for the indicators."""
from dataclasses import dataclass

import polars as pl

from src.app.data.aggregators.indicators.basic_indicators import (
    atr, clip_expr, ema, log_return, realized_vol, rolling_slope, zscore_rolling
)


@dataclass(frozen=True)
class IndicatorsFeatureConfig:
    """Configuration for indicator feature computation."""

    # EMA
    ema_fast: int = 5
    ema_slow: int = 20

    # Volatility
    atr_period: int = 14
    rv_short: int = 15
    rv_long: int = 60

    # Momentum / trend
    logret_mom: int = 5
    slope_window: int = 15

    # Normalization
    zscore_window: int = 60
    clip_lo: float = -5.0
    clip_hi: float = 5.0


class IndicatorsFeature:
    """Builds indicator feature matrix from OHLC data.

    This class is responsible ONLY for feature computation.
    It does not produce trading signals or predictions.
    """
    def __init__(self, cfg: IndicatorsFeatureConfig | None = None) -> None:
        """Start of the class.
        
        Args:
            cfg (IndicatorsFeatureConfig | None): The indicator config.
        """
        self.cfg = cfg or IndicatorsFeatureConfig()

    def build(self, df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame | pl.DataFrame:
        """Compute indicator features.

        Args:
            df (pl.DataFrame | pl.LazyFrame):
                ['open', 'high', 'low', 'close']

        Returns:
            pl.LazyFrame | pl.DataFrame: LazyFrame with indicator feature columns added.
        """
        close = pl.col("close")

        # --- Core indicators ---
        # EMA example: ema(pl.col("close"), span=20)
        ema_fast = ema(close, self.cfg.ema_fast)
        ema_slow = ema(close, self.cfg.ema_slow)

        # True range example (via ATR): max(high-low, abs(high-prev_close), abs(low-prev_close))
        atr_expr = atr(
            period=self.cfg.atr_period,
        )

        logret_1 = log_return(close, 1)
        logret_mom = log_return(close, self.cfg.logret_mom)

        rv_short = realized_vol(logret_1, self.cfg.rv_short)
        rv_long = realized_vol(logret_1, self.cfg.rv_long)

        slope = rolling_slope(close, self.cfg.slope_window)

        # --- Feature engineering ---
        features = [
            # Momentum / trend
            ((ema_fast - ema_slow) / (atr_expr + 1e-12)).alias("ema_diff_norm"),
            (slope / (atr_expr + 1e-12)).alias("slope_norm"),
            logret_mom.alias("logret_mom"),

            # Volatility regime
            (rv_short / (rv_long + 1e-12)).alias("vol_ratio"),

            # Stationarized versions (optional but recommended)
            clip_expr(
                zscore_rolling(logret_mom, self.cfg.zscore_window),
                self.cfg.clip_lo,
                self.cfg.clip_hi,
            ).alias("logret_mom_z"),
        ]

        return df.with_columns(features)

    @staticmethod
    def feature_names() -> list[str]:
        """Return list of feature column names.
        
        Returns:
            list[str]: List of feature column names.
        """
        return [
            "ema_diff_norm",
            "slope_norm",
            "logret_mom",
            "vol_ratio",
            "logret_mom_z",
        ]
