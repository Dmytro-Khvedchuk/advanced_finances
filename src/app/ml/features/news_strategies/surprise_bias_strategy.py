"""Surprise-based news strategy using ForexFactory events."""
import numpy as np
import polars as pl

from src.app.backtest import StrategyContext


IMPACT_HALF_LIFE = {
    "LOW": 3,
    "MEDIUM": 6,
    "HIGH": 12,
}

IMPACT_WEIGHT = {
    "LOW": 0.3,
    "MEDIUM": 0.6,
    "HIGH": 1.0,
}


def _base_out(price_df: pl.DataFrame) -> pl.DataFrame:
    """Build a zero-signal output aligned to the price bars.

    Returns:
        pl.DataFrame: Output with row_id (and timestamp if available), signal, score.
    """
    # one row per bar
    base = price_df.with_row_index("row_id")
    cols = ["row_id"]
    if "timestamp" in base.columns:
        cols.append("timestamp")
    return base.select(cols).with_columns(
        pl.lit(0).cast(pl.Int8).alias("signal"),
        pl.lit(0.0).cast(pl.Float64).alias("score"),
    )


def _align_to_price(price_df: pl.DataFrame, pred: pl.DataFrame) -> pl.DataFrame:
    """Align predictions to the price index with zero-filled gaps.

    Returns:
        pl.DataFrame: Output aligned to price rows with row_id, signal, score.

    Raises:
        ValueError: If pred has neither row_id nor timestamp columns.
    """
    base = _base_out(price_df)

    if pred.is_empty():
        return base.select(
            ["row_id"]
            + (["timestamp"] if "timestamp" in base.columns else [])
            + ["signal", "score"]
        )

    # IMPORTANT: prevent duplicate-column collision
    base_keys = ["row_id"] + (["timestamp"] if "timestamp" in base.columns else [])
    base_no_sig = base.select(base_keys)  # drop signal/score before join

    if "row_id" in pred.columns:
        out = base_no_sig.join(
            pred.select(["row_id", "signal", "score"]),
            on="row_id",
            how="left",
        )
    elif "timestamp" in pred.columns and "timestamp" in base_no_sig.columns:
        out = base_no_sig.join(
            pred.select(["timestamp", "signal", "score"]),
            on="timestamp",
            how="left",
        )
    else:
        raise ValueError("Prediction output must include 'row_id' or 'timestamp'.")

    return out.with_columns(
        pl.col("signal").fill_null(0).cast(pl.Int8),
        pl.col("score").fill_null(0.0).cast(pl.Float64),
    ).select(
        ["row_id"]
        + (["timestamp"] if "timestamp" in out.columns else [])
        + ["signal", "score"]
    )


def _half_life_decay(k: np.ndarray, half_life: float) -> np.ndarray:
    """Exponential decay curve indexed by k and half-life.

    Returns:
        np.ndarray: Decay weights per index in k.
    """
    return np.exp(-np.log(2.0) * (k / half_life))


def extract_numeric(x: object) -> float | None:
    """Extract numeric value from ForexFactory NUMERIC_STRUCT.

    Expected formats:
        {"value": float, ...}
        {"raw": "3.2%", ...}
        float / int

    Returns:
        float or None if unavailable.
    """
    if x is None:
        return None

    if isinstance(x, (int, float)):
        return float(x)

    if isinstance(x, dict) and isinstance(x.get("value"), (int, float)):
        return float(x["value"])

    return None


class NS1SurpriseBias:
    """ForexFactory surprise-based directional bias strategy."""

    def __init__(self, horizon_bars: int = 12) -> None:
        """Initialize the strategy.

        Args:
            horizon_bars (int): Number of bars affected by each news event.
        """
        self.horizon_bars = horizon_bars

    def fit(self, df: pl.DataFrame) -> None:  # noqa: PLR6301
        """Fit the strategy (stateless).

        Args:
            df (pl.DataFrame): Training price data (unused).
        """
        # stateless
        _ = df

    def predict(self, ctx: StrategyContext) -> pl.DataFrame:  # noqa: PLR0914
        """Generate per-bar signal and score based on news surprises.

        Returns:
            pl.DataFrame: Output aligned to price rows with row_id, signal, score.
        """
        price_df = ctx.price_df
        news_df = ctx.news_df

        if news_df is None or news_df.is_empty():
            return _align_to_price(ctx.price_df, pl.DataFrame())

        price_df = price_df.with_row_index("row_id")

        news = (
            news_df
            .with_columns(
                pl.col("event_date")
                .dt.combine(pl.col("event_time"))
                .alias("timestamp")
            )
            .drop_nulls(subset=["actual", "forecast"])
        )

        outputs = []

        for event in news.iter_rows(named=True):
            impact = (event["impact"] or "").upper()
            if impact not in IMPACT_WEIGHT:
                continue

            actual = extract_numeric(event["actual"])
            forecast = extract_numeric(event["forecast"])
            if actual is None or forecast is None:
                continue

            surprise = actual - forecast
            if surprise == 0:
                continue

            direction = int(np.sign(surprise))
            weight = float(IMPACT_WEIGHT[impact])
            half_life = float(IMPACT_HALF_LIFE[impact])

            ts = event["timestamp"]
            if ts is None:
                continue

            affected = (
                price_df
                .filter(pl.col("timestamp") >= ts)
                .select("row_id")
                .head(self.horizon_bars)
                .with_row_index("k")
            )

            if affected.is_empty():
                continue

            k = affected["k"].to_numpy().astype(float)
            decay = _half_life_decay(k, half_life)

            score = weight * decay
            contrib = direction * score

            outputs.append(
                pl.DataFrame(
                    {
                        "row_id": affected["row_id"],
                        "contrib": contrib,
                    }
                )
            )

        if not outputs:
            return _align_to_price(ctx.price_df, pl.DataFrame())

        pred = (
            pl.concat(outputs)
            .group_by("row_id")
            .agg(pl.sum("contrib").alias("net"))
            .with_columns(
                pl.when(pl.col("net") > 0).then(1)
                    .when(pl.col("net") < 0).then(-1)
                    .otherwise(0)
                    .cast(pl.Int8)
                    .alias("signal"),
                pl.col("net").abs().clip(0.0, 1.0).cast(pl.Float64).alias("score"),
            )
            .select(["row_id", "signal", "score"])
        )

        return _align_to_price(ctx.price_df, pred)
