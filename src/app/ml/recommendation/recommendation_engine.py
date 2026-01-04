"""Recommendation engine for combining strategy outputs."""
import numpy as np
import polars as pl


class RecommendationEngine:
    """Combine weighted strategy signals into a final recommendation."""
    def __init__(
        self,
        strategy_weights: dict[str, float],
        decision_threshold: float = 0.2,
        eps: float = 1e-8,
    ) -> None:
        """Initialize the engine.

        Args:
            strategy_weights (dict[str, float]): Initial strategy weights.
            decision_threshold (float): Net vote threshold for signal emission.
            eps (float): Numerical stability epsilon.
        """
        self.weights = strategy_weights.copy()
        self.threshold = float(decision_threshold)
        self.eps = float(eps)

    def update_weights(self, new_weights: dict[str, float]) -> None:
        """Normalize and update strategy weights in-place."""
        if not new_weights:
            return

        total = float(sum(new_weights.values()))
        if total < self.eps:
            n = len(self.weights)
            self.weights = dict.fromkeys(self.weights, 1.0 / n)
        else:
            self.weights = {k: float(v) / total for k, v in new_weights.items()}

    def get_weights(self) -> dict[str, float]:
        """Return a copy of the current weights."""
        return self.weights.copy()

    def combine(self, strategy_outputs: dict[str, pl.DataFrame]) -> pl.DataFrame:
        """Combine strategy outputs into a final recommendation.

        Expected per-strategy schema:
            row_id: int
            signal: int in {-1,0,+1}
            score:  float in [0,1] interpreted as confidence

        Returns:
            row_id, signal, score   where score is ENSEMBLE CONFIDENCE in [0,1]
        """
        if not strategy_outputs:
            return pl.DataFrame({"row_id": [], "signal": [], "score": []})

        # 1) Full row universe
        all_row_ids = (
            pl.concat(
                [df.select(pl.col("row_id").cast(pl.Int64)) for df in strategy_outputs.values()],
                how="vertical",
            )
            .unique()
            .sort("row_id")
        )

        merged = all_row_ids

        signed_cols: list[str] = []
        support_cols: list[str] = []

        # 2) Join each strategy vote + support
        for name, df in strategy_outputs.items():
            w = float(self.weights.get(name, 0.0))
            if w <= 0.0:
                continue

            sig_col = f"{name}__sig"
            conf_col = f"{name}__conf"
            vote_col = f"{name}__vote"
            sup_col = f"{name}__sup"

            contrib = df.select(
                pl.col("row_id").cast(pl.Int64),
                pl.col("signal").cast(pl.Int8).alias(sig_col),
                pl.col("score").cast(pl.Float64).alias(conf_col),
            )

            merged = (
                merged.join(contrib, on="row_id", how="left")
                .with_columns(
                    pl.col(sig_col).fill_null(0),
                    pl.col(conf_col).fill_null(0.0).clip(0.0, 1.0),
                    (pl.col(sig_col) * pl.col(conf_col) * w).alias(vote_col),
                    (pl.col(sig_col).abs() * pl.col(conf_col) * w).alias(sup_col),
                )
            )

            signed_cols.append(vote_col)
            support_cols.append(sup_col)

        if not signed_cols:
            return pl.DataFrame(
                {
                    "row_id": merged["row_id"],
                    "signal": np.zeros(merged.height, dtype=np.int8),
                    "score": np.zeros(merged.height, dtype=float),
                }
            )

        merged = merged.with_columns(
            pl.sum_horizontal(signed_cols).alias("net"),
            pl.sum_horizontal(support_cols).alias("support"),
        )

        net = merged["net"].to_numpy()
        support = merged["support"].to_numpy()

        # 3) Decision (keep your existing semantics: threshold on net)
        final_signal = np.zeros(len(net), dtype=np.int8)
        final_signal[net > self.threshold] = +1
        final_signal[net < -self.threshold] = -1

        # 4) Ensemble confidence in [0,1]
        ensemble_conf = np.where(
            support > self.eps,
            np.clip(np.abs(net) / (support + self.eps), 0.0, 1.0),
            0.0,
        ).astype(float)

        return pl.DataFrame(
            {
                "row_id": merged["row_id"],
                "signal": final_signal,
                "score": ensemble_conf,  # score now = confidence of the final signal
            }
        )
