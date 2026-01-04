"""Evaluation utilities for backtests."""
import numpy as np
import polars as pl


def make_forward_labels(
    df: pl.DataFrame,
    horizon_bars: int,
) -> pl.DataFrame:
    """Create evaluation labels aligned by row index.

    Returns DataFrame with:
        - row_id
        - label  (+1, -1, 0)

    Returns:
        pl.DataFrame: DataFrame with row_id and label columns.
    """
    close = df["close"].to_numpy()
    n = len(close)

    fwd = np.full(n, np.nan, dtype=float)
    fwd[:-horizon_bars] = np.log(
        close[horizon_bars:] / close[:-horizon_bars]
    )

    label = np.zeros(n, dtype=int)
    valid = ~np.isnan(fwd)
    label[valid] = np.sign(fwd[valid]).astype(int)

    return pl.DataFrame(
        {
            "row_id": np.arange(n),
            "label": label,
        }
    )


def confusion_matrix(
    y_true: np.ndarray,
    y_pred: np.ndarray,
) -> dict:
    """Binary confusion matrix for long/short decisions.

    Returns:
        dict: Confusion matrix with TP, TN, FP, and FN.
    """
    return {
        "TP": int(((y_pred == 1) & (y_true == 1)).sum()),
        "TN": int(((y_pred == -1) & (y_true == -1)).sum()),
        "FP": int(((y_pred == 1) & (y_true == -1)).sum()),
        "FN": int(((y_pred == -1) & (y_true == 1)).sum()),
    }


def classification_metrics(cm: dict) -> dict:
    """Basic directional classification metrics.

    Returns:
        dict: Metrics with accuracy, precision, recall, and n_trades.
    """
    tp, tn, fp, fn = cm["TP"], cm["TN"], cm["FP"], cm["FN"]

    total = tp + tn + fp + fn
    return {
        "accuracy": (tp + tn) / total if total else 0.0,
        "precision": tp / (tp + fp) if (tp + fp) else 0.0,
        "recall": tp / (tp + fn) if (tp + fn) else 0.0,
        "n_trades": total,
    }


def aggregate_confusion_matrices(
    cms: list[dict[str, int]]
) -> dict[str, int]:
    """Aggregate multiple confusion matrices.

    Returns:
        dict[str, int]: Summed confusion matrix values.
    """
    total = {
        "TP": 0,
        "TN": 0,
        "FP": 0,
        "FN": 0,
    }

    for cm in cms:
        for k in total:
            total[k] += int(cm.get(k, 0))

    return total


def cm_array_to_dict(cm: np.ndarray) -> dict[str, int]:
    """Convert a 2x2 confusion matrix array to a dict.

    Expected layout:
        [[tn, fp],
         [fn, tp]]

    Returns:
        dict[str, int]: Confusion matrix with tn, fp, fn, and tp.

    Raises:
        ValueError: If cm is not 2x2.
    """
    if cm.shape != (2, 2):
        raise ValueError(f"Expected 2x2 confusion matrix, got {cm.shape}")

    tn, fp = cm[0]
    fn, tp = cm[1]

    return {
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
        "tp": int(tp),
    }
