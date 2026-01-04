"""Walk forward evaluaion of the recommendation system (binary)."""
from typing import Protocol

import numpy as np
import polars as pl

from src.app.backtest.evaluation import classification_metrics, confusion_matrix, make_forward_labels
from src.app.backtest.strategy_context import StrategyContext
from src.app.ml.recommendation import RecommendationEngine, StrategyWeightUpdater
from src.app.system.logs import get_logger


logger = get_logger(__name__)


class Strategy(Protocol):
    """The Strategy class."""
    def fit(self, df: pl.DataFrame) -> None:
        """The fit function.
        
        Args:
            df (pl.Dataframe): Dataframe for fit.
        """
        ...

    def predict(self, ctx: StrategyContext) -> pl.DataFrame:
        """The predict function.
        
        Args:
            ctx (StrategyContext): The strategy context.

        Returns:
            pl.DataFrame: The dataframe with predicted scores.        
        """
        ...


def _ensure_timestamp(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure tmiestamp column is in ms.
    
    Args:
        df (pl.DataFrame): Dataframe.

    Returns:
        pl.DataFrame: Checked dataframe.
    """
    if "timestamp" not in df.columns:
        df = df.with_columns(
            pl.col("open_time").cast(pl.Datetime("ms")).alias("timestamp")
        )
    return df.sort("timestamp")


def _monthly_boundaries(df: pl.DataFrame) -> list[pl.Datetime]:
    """The funciton to determine one month walk-forward.

    Args:
        df (pl.DataFrame): Dataframe with data.

    Returns:
        list[pl.Datetime]: The dataframe boundaries.
    """
    return (
        df.select(pl.col("timestamp").dt.truncate("1mo").alias("month"))
        .unique()
        .sort("month")
        .to_series()
        .to_list()
    )


def run_walk_forward_recommendation(  # noqa: PLR0913, PLR0915, PLR0914
    df: pl.DataFrame,
    *,
    strategies: dict[str, Strategy], 
    recommender: RecommendationEngine,
    news_df: pl.DataFrame | None = None,
    horizon_bars: int = 15,
    min_train_bars: int = 5_000,
    weight_alpha: float = 0.2,
    weight_min_trades: int = 30,
) -> list[dict]:
    """Run monthly walk-forward evaluation for an ensemble recommender.

    Args:
        df (pl.DataFrame): Price DataFrame with at least
            ['open_time' or 'timestamp', 'open', 'high', 'low', 'close'].
        strategies (dict[str, Strategy]): Strategy map that implements fit() and predict().
        recommender (RecommendationEngine): RecommendationEngine to combine strategy outputs.
        news_df (pl.DataFrame | None): Optional news DataFrame for strategies that use news features.
        horizon_bars (int): Forward label horizon in bars.
        min_train_bars (int): Minimum training bars required for each month.
        weight_alpha (float): EMA smoothing for strategy weight updates.
        weight_min_trades (int): Minimum trades before weights are updated.

    Returns:
        list[dict]: List of per-month evaluation dicts.

    Raises:
        ValueError: If a strategy output lacks timestamps and does not match test_df length.
    """
    results: list[dict] = []

    df = _ensure_timestamp(df)
    months = _monthly_boundaries(df)

    weight_updater = StrategyWeightUpdater(
        strategy_names=list(strategies.keys()),
        alpha=weight_alpha,
        min_trades=weight_min_trades,
    )

    for i in range(1, len(months)):
        logger.info("Processing month...")

        train_end = months[i]
        test_start = months[i]
        test_end = months[i + 1] if i + 1 < len(months) else None

        train_df = df.filter(pl.col("timestamp") < pl.lit(train_end))
        test_df = (
            df.filter(
                (pl.col("timestamp") >= pl.lit(test_start))
                & (pl.col("timestamp") < pl.lit(test_end))
            )
            if test_end
            else df.filter(pl.col("timestamp") >= pl.lit(test_start))
        )

        if len(train_df) < min_train_bars or test_df.is_empty():
            continue

        # Base universe
        base_index = (
            test_df
            .with_row_index("row_id")
            .select(
                pl.col("row_id").cast(pl.Int64),
                pl.col("timestamp"),
            )
        )

        # Train
        for strat in strategies.values():
            if hasattr(strat, "fit"):
                strat.fit(train_df)

        # Predict
        ctx = StrategyContext(price_df=test_df, news_df=news_df)

        strategy_outputs: dict[str, pl.DataFrame] = {}
        for name, strat in strategies.items():
            out = strat.predict(ctx)

            if "timestamp" not in out.columns:
                if out.height != base_index.height:
                    raise ValueError(
                        f"{name}.predict() must return 'timestamp' or match test_df length. "
                        f"Got out.height={out.height}, expected {base_index.height}."
                    )
                out = out.with_columns(base_index["timestamp"].alias("timestamp"))

            out = out.select(
                pl.col("timestamp"),
                pl.col("signal").cast(pl.Int64),
                pl.col("score").cast(pl.Float64),
            )

            aligned = (
                base_index
                .join(out, on="timestamp", how="left")
                .with_columns(
                    pl.col("signal").fill_null(0),
                    pl.col("score").fill_null(0.0),
                )
                .select(["row_id", "signal", "score"])  # combine() expects row_id
            )

            strategy_outputs[name] = aligned

        # Ensemble
        final = recommender.combine(strategy_outputs)

        # Labels (label may be -1/0/+1)
        labels = make_forward_labels(test_df, horizon_bars).with_columns(
            pl.col("row_id").cast(pl.Int64)
        )

        # Evaluate ensemble (unchanged)
        eval_df = (
            final.join(labels, on="row_id", how="inner")
            .filter((pl.col("signal") != 0) & (pl.col("label") != 0))
        )
        if eval_df.is_empty():
            continue

        y_pred = eval_df["signal"].to_numpy()
        y_true = eval_df["label"].to_numpy()

        cm = confusion_matrix(y_true, y_pred)
        metrics = classification_metrics(cm)

        # -------------------------------
        # Weight update (per-strategy) - UPDATED
        # Key change: DO NOT drop label==0 here.
        # We want to penalize strategies that trade during label==0 (chop).
        # -------------------------------
        label_universe = (
            labels
            .select(pl.col("row_id").cast(pl.Int64), pl.col("label"))
            .sort("row_id")
        )

        per_strategy_pairs: dict[str, np.ndarray] = {}

        for name, out in strategy_outputs.items():
            eval_s = (
                label_universe
                .join(
                    out.select(pl.col("row_id").cast(pl.Int64), pl.col("signal")),
                    on="row_id",
                    how="left",
                )
                .with_columns(pl.col("signal").fill_null(0))
                .select(["signal", "label"])
            )
            per_strategy_pairs[name] = eval_s.to_numpy()

        # (Optional but recommended) diagnostics BEFORE update
        for n in ["is1", "is2", "is3", "news_ns1"]:
            if n not in per_strategy_pairs:
                continue
            pairs = per_strategy_pairs[n]
            preds = pairs[:, 0]
            labels_ = pairs[:, 1]
            m = (preds != 0) & (labels_ != 0)
            nt = int(m.sum())
            acc = float((preds[m] == labels_[m]).mean()) if nt else 0.0
            logger.info(f"WF {n} n_trades={nt} {acc=}")

        new_weights = weight_updater.update(predictions=per_strategy_pairs)
        recommender.update_weights(new_weights)

        results.append(
            {
                "month": test_start,
                "confusion": cm,
                "metrics": metrics,
                "n_trades": int(eval_df.height),
                "weights": recommender.get_weights(),
            }
        )

    return results
