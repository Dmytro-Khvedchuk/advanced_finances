"""Momentum-volatility regime strategy using indicator clustering."""
from dataclasses import dataclass

import numpy as np
import polars as pl
from sklearn.mixture import GaussianMixture  # type: ignore[reportMissingTypeStubs]
from sklearn.preprocessing import StandardScaler  # type: ignore[reportMissingTypeStubs]

from src.app.backtest import StrategyContext
from src.app.data.aggregators.indicators import IndicatorsFeature


def _to_df(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Ensure eager Polars DataFrame.

    Returns:
        pl.DataFrame: Eager DataFrame.
    """
    return df.collect() if isinstance(df, pl.LazyFrame) else df


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


@dataclass(frozen=True)
class IS1Config:
    """Configuration for IS-1 Momentum–Volatility Regime Strategy."""

    n_clusters: int = 3
    random_state: int = 42
    min_confidence: float = 0.55
    horizon_bars: int = 4


class IS1MomentumVol:
    """IS-1 Momentum–Volatility Regime Indicator Strategy."""

    def __init__(
        self,
        cfg: IS1Config | None = None,
        feature_builder: IndicatorsFeature | None = None,
    ) -> None:
        """Initialize the strategy.

        Args:
            cfg (IS1Config | None): Configuration overrides.
            feature_builder (IndicatorsFeature | None): Feature builder override.
        """
        self.cfg = cfg or IS1Config()
        self.features = feature_builder or IndicatorsFeature()

        self.scaler: StandardScaler | None = None
        self.model: GaussianMixture | None = None
        self.cluster_to_signal: dict[int, int] = {}

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------
    def fit(self, df: pl.DataFrame | pl.LazyFrame) -> None:
        """Fit the GMM using indicator features.

        Args:
            df (pl.DataFrame | pl.LazyFrame): Price data with required columns.

        Raises:
            ValueError: If no data remains after feature computation.
        """
        df = _to_df(df)
        base = df.with_row_index("row_id")

        lf = self.features.build(base)

        feat_df = _to_df(
            lf.select(["row_id"] + self.features.feature_names())
            .drop_nulls()
        )

        if feat_df.is_empty():
            raise ValueError("IS-1 fit(): no data after feature computation.")

        price_df = base.select(["row_id", "close"])
        data = feat_df.join(price_df, on="row_id", how="inner")

        features = data.select(self.features.feature_names()).to_numpy()
        close = data["close"].to_numpy()

        self.scaler = StandardScaler()
        features_scaled = self.scaler.fit_transform(features)

        self.model = GaussianMixture(
            n_components=self.cfg.n_clusters,
            covariance_type="full",
            random_state=self.cfg.random_state,
            n_init=3,
        )
        self.model.fit(features_scaled)

        clusters = self.model.predict(features_scaled)

        fwd_ret = np.full_like(close, np.nan, dtype=float)
        hb = int(self.cfg.horizon_bars)
        fwd_ret[:-hb] = np.log(close[hb:] / close[:-hb])

        cluster_ret = {}
        for c in range(self.cfg.n_clusters):
            mask = (clusters == c) & ~np.isnan(fwd_ret)
            cluster_ret[c] = np.median(fwd_ret[mask]) if mask.any() else 0.0

        ordered = sorted(cluster_ret.items(), key=lambda x: x[1])

        self.cluster_to_signal = dict.fromkeys(range(self.cfg.n_clusters), 0)
        if ordered:
            self.cluster_to_signal[ordered[0][0]] = -1
            self.cluster_to_signal[ordered[-1][0]] = +1

    # ------------------------------------------------------------------
    # Inference
    # ------------------------------------------------------------------
    def predict(self, ctx: StrategyContext) -> pl.DataFrame:
        """Generate per-bar signals from the fitted GMM.

        Args:
            ctx (StrategyContext): Strategy context with price data.

        Returns:
            pl.DataFrame: Output aligned to price rows with row_id, signal, score.

        Raises:
            RuntimeError: If the model has not been fitted.
        """
        if self.scaler is None or self.model is None:
            raise RuntimeError("IS-1 predict(): model not fitted.")

        df = _to_df(ctx.price_df)
        base = df.with_row_index("row_id")

        lf = self.features.build(base)
        feat_df = _to_df(
            lf.select(["row_id"] + self.features.feature_names())
            .drop_nulls()
        )

        if feat_df.is_empty():
            return _align_to_price(df, pl.DataFrame())

        features = feat_df.select(self.features.feature_names()).to_numpy()
        features_scaled = self.scaler.transform(features)

        probs = self.model.predict_proba(features_scaled)
        best_cluster = probs.argmax(axis=1)
        confidence = probs.max(axis=1)

        signal = np.array(
            [
                self.cluster_to_signal.get(int(c), 0) if conf >= self.cfg.min_confidence else 0
                for c, conf in zip(best_cluster, confidence, strict=False)
            ],
            dtype=np.int8,
        )

        pred = pl.DataFrame(
            {
                "row_id": feat_df["row_id"].cast(pl.Int64),
                "signal": pl.Series(signal).cast(pl.Int8),
                "score": pl.Series(confidence).cast(pl.Float64),
            }
        )

        return _align_to_price(df, pred)
