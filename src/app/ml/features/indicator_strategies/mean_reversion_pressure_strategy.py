"""Mean reversion strategy using indicator z-score clustering."""
import numpy as np
import polars as pl
from sklearn.mixture import GaussianMixture  # type: ignore[reportMissingTypeStubs]
from sklearn.preprocessing import StandardScaler  # type: ignore[reportMissingTypeStubs]

from src.app.backtest import StrategyContext
from src.app.data.aggregators.indicators import IndicatorsFeature


def _to_df(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    """Return an eager DataFrame for downstream processing.

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


class IS2MeanReversionML:
    """ML-based mean reversion using unsupervised regime clustering on z-scores."""

    def __init__(
        self,
        n_clusters: int = 3,
        min_confidence: float = 0.55,
    ) -> None:
        """Initialize the strategy.

        Args:
            n_clusters (int): Number of GMM clusters.
            min_confidence (float): Minimum cluster probability to emit a signal.
        """
        self.n_clusters = n_clusters
        self.min_confidence = min_confidence
        self.features = IndicatorsFeature()

        self.scaler: StandardScaler | None = None
        self.model: GaussianMixture | None = None
        self.cluster_direction: dict[int, int] = {}

    # ------------------------
    # Training
    # ------------------------
    def fit(self, df: pl.DataFrame) -> None:
        """Fit the GMM on z-score features.

        Args:
            df (pl.DataFrame): Price DataFrame with required indicator inputs.

        Raises:
            ValueError: If there is no data after feature computation.
        """
        lf = self.features.build(df)

        data = (
            lf
            .select(["logret_mom_z"])
            .drop_nulls()
        )
        data = _to_df(data)

        if data.is_empty():
            raise ValueError("IS2 fit(): no data")

        z = np.asarray(
            data.select("logret_mom_z").to_numpy()
        ).reshape(-1)

        self.scaler = StandardScaler()
        z_s = self.scaler.fit_transform(z.reshape(-1, 1))

        self.model = GaussianMixture(
            n_components=self.n_clusters,
            covariance_type="full",
            random_state=42,
        )
        self.model.fit(z_s)

        # cluster interpretation: extreme z → mean reversion
        means_std = np.asarray(self.model.means_).reshape(-1, 1)
        means = self.scaler.inverse_transform(means_std).reshape(-1)

        self.cluster_direction = {}
        for i, m in enumerate(means):
            if m > 0:
                self.cluster_direction[i] = -1  # fade positive extreme
            elif m < 0:
                self.cluster_direction[i] = +1  # fade negative extreme
            else:
                self.cluster_direction[i] = 0

    # ------------------------
    # Inference
    # ------------------------

    def predict(self, ctx: StrategyContext) -> pl.DataFrame:
        """Generate per-bar signals from the fitted GMM.

        Args:
            ctx (StrategyContext): Strategy context with price data.

        Returns:
            pl.DataFrame: Output aligned to price rows with row_id, signal, score.

        Raises:
            RuntimeError: If the model has not been fitted.
        """
        if self.model is None or self.scaler is None:
            raise RuntimeError("IS2 predict(): model not fitted")

        df = ctx.price_df
        base = df.with_row_index("row_id")

        lf = self.features.build(base)

        data = _to_df(
            lf.select(["row_id"] + (["timestamp"] if "timestamp" in base.columns else []) + ["logret_mom_z"])
            .drop_nulls(subset=["logret_mom_z"])
        )

        if data.is_empty():
            return _align_to_price(df, pl.DataFrame())

        z = data.select("logret_mom_z").to_numpy().reshape(-1, 1)
        z_s = self.scaler.transform(z)

        probs = self.model.predict_proba(z_s)
        clusters = probs.argmax(axis=1)
        confidence = probs.max(axis=1)

        signal = np.array(
            [
                self.cluster_direction.get(int(c), 0) if conf >= self.min_confidence else 0
                for c, conf in zip(clusters, confidence, strict=False)
            ],
            dtype=np.int8,
        )

        pred = pl.DataFrame(
            {
                "row_id": data["row_id"].cast(pl.Int64),
                "signal": pl.Series(signal).cast(pl.Int8),
                "score": pl.Series(confidence).cast(pl.Float64),
            }
        )

        return _align_to_price(df, pred)
