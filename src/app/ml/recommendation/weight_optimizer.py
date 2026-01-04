"""Adaptive strategy weight updater."""
import numpy as np


_EXPECTED_PAIR_COLS: int = 2


class StrategyWeightUpdater:
    """Minimal adaptive weight updater.

    - Score per strategy = accuracy on its own trades (signal != 0), where label != 0
    - Weight ∝ score * sqrt(n_trades)   (gives more confidence to strategies with more samples)
    - EMA smoothing via alpha
    - Always normalized weights
    """

    def __init__(
        self,
        *,
        strategy_names: list[str],
        alpha: float = 0.2,
        min_trades: int = 30,
        eps: float = 1e-8,
    ) -> None:
        """Initialize the weight updater.

        Args:
            strategy_names (list[str]): Strategy identifiers to track.
            alpha (float): EMA smoothing factor.
            min_trades (int): Minimum trades before updates apply.
            eps (float): Numerical stability epsilon.
        """
        self.alpha = float(alpha)
        self.min_trades = int(min_trades)
        self.eps = float(eps)

        # start uniform
        n = len(strategy_names)
        self.weights: dict[str, float] = dict.fromkeys(strategy_names, 1.0 / n)

    def update(
        self,
        *,
        predictions: dict[str, np.ndarray],  # shape (N,2): [signal, label], aligned
    ) -> dict[str, float]:
        """Update and return strategy weights based on prediction accuracy.

        Returns:
            dict[str, float]: Updated weights.

        Raises:
            ValueError: If any prediction array is not shaped (N, 2).
        """
        scores: dict[str, float] = {}

        for name, pairs in predictions.items():
            if pairs.ndim != 2 or pairs.shape[1] != _EXPECTED_PAIR_COLS:  # noqa: PLR2004
                raise ValueError(f"Invalid shape for {name}: {pairs.shape}")

            preds = pairs[:, 0].astype(np.int8, copy=False)
            labels = pairs[:, 1].astype(np.int8, copy=False)

            trade_mask = (preds != 0) & (labels != 0)
            n = int(trade_mask.sum())
            if n < self.min_trades:
                continue

            acc = float((preds[trade_mask] == labels[trade_mask]).mean())

            # Confidence scaling: more trades => slightly more influence
            scores[name] = acc * float(np.sqrt(n))

        # If nothing qualifies, keep weights (or you can fallback to uniform)
        if not scores:
            return self.weights

        total = float(sum(scores.values()))
        if total <= self.eps:
            return self.weights

        # Normalize scores into a target weight distribution
        target = {k: v / total for k, v in scores.items()}

        # EMA update
        for name in self.weights:
            old = self.weights[name]
            new = target.get(name, 0.0)  # strategies with no score drift toward 0
            self.weights[name] = (1.0 - self.alpha) * old + self.alpha * new

        # Normalize final weights
        total_w = float(sum(self.weights.values()))
        if total_w <= self.eps:
            n = len(self.weights)
            self.weights = dict.fromkeys(self.weights, 1.0 / n)
        else:
            self.weights = {k: v / total_w for k, v in self.weights.items()}

        return self.weights
