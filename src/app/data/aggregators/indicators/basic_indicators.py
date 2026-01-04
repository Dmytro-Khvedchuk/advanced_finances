"""The basic indicators."""
import numpy as np
import polars as pl


_EPS: float = 1e-12


def ema(expr: pl.Expr, span: int) -> pl.Expr:
    """Compute exponential moving average (EMA).

    Uses adjust=False semantics (streaming-friendly).
    EMA_t = alpha * x_t + (1 - alpha) * EMA_{t-1}
    alpha = 2 / (span + 1)

    Example:
        ema(pl.col("close"), span=20)

    Args:
        expr (pl.Expr): Polars expression (e.g., pl.col("close")).
        span (int): EMA span.

    Returns:
        pl.Expr: Polars expression representing the EMA.
    """
    alpha: float = 2.0 / (span + 1.0)
    return expr.ewm_mean(alpha=alpha, adjust=False)


def true_range(high: pl.Expr, low: pl.Expr, close: pl.Expr) -> pl.Expr:
    """Compute True Range (TR).

    TR_t = max(
        high_t - low_t,
        abs(high_t - close_{t-1}),
        abs(low_t - close_{t-1}),
    )

    Example:
        true_range(pl.col("high"), pl.col("low"), pl.col("close"))

    Args:
        high (pl.Expr): High price expression.
        low (pl.Expr): Low price expression.
        close (pl.Expr): Close price expression.

    Returns:
        pl.Expr: Polars expression for True Range.
    """
    prev_close: pl.Expr = close.shift(1)

    tr1: pl.Expr = (high - low).abs()
    tr2: pl.Expr = (high - prev_close).abs()
    tr3: pl.Expr = (low - prev_close).abs()

    return pl.max_horizontal(tr1, tr2, tr3)


def atr(
    period: int = 14,
    wilder: bool = True,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pl.Expr:
    """Compute Average True Range (ATR).

    Uses Wilder's smoothing by default:
        EMA(alpha = 1 / period)

    Args:
        period (int): ATR lookback period.
        wilder (bool): Whether to use Wilder smoothing.
        high_col (str): High price column name.
        low_col (str): Low price column name.
        close_col (str): Close price column name.

    Returns:
        pl.Expr: Polars expression for ATR.
    """
    tr = true_range(
        pl.col(high_col),
        pl.col(low_col),
        pl.col(close_col),
    )

    if wilder:
        alpha = 1.0 / period
        return tr.ewm_mean(alpha=alpha, adjust=False)

    return tr.rolling_mean(
        window_size=period,
        min_samples=period,
    )


def log_return(expr: pl.Expr, periods: int = 1) -> pl.Expr:
    """Compute log returns.

    logret_t = ln(price_t / price_{t-periods})

    Args:
        expr (pl.Expr): Price expression (typically close).
        periods (int): Lag period.

    Returns:
        pl.Expr: Polars expression for log return.
    """
    return (expr / expr.shift(periods)).log()


def realized_vol(
    logret_expr: pl.Expr,
    window: int,
) -> pl.Expr:
    """Compute realized volatility as rolling std of log returns.

    Args:
        logret_expr (pl.Expr): Log return expression (1-period).
        window (int): Rolling window size.

    Returns:
        pl.Expr: Polars expression for realized volatility.
    """
    return logret_expr.rolling_std(
        window_size=window,
        min_samples=window,
    )


def rolling_slope(
    expr: pl.Expr,
    window: int,
) -> pl.Expr:
    """Compute rolling linear regression slope.

    Fits y = a * x + b where x = [0, ..., window-1].

    Note:
    Uses rolling_map because Polars has no native rolling regression.
    Window sizes are small, so performance impact is acceptable.

    Args:
        expr (pl.Expr): Value expression (e.g., close).
        window (int): Rolling window size.

    Returns:
        pl.Expr: Polars expression for slope per bar.
    """
    x = np.arange(window, dtype=float)

    def _slope(values: pl.Series) -> float:
        arr = values.to_numpy()

        if np.isnan(arr).any():
            return float("nan")

        a, _ = np.polyfit(x, arr, 1)
        return float(a)

    return expr.rolling_map(
        function=_slope,
        window_size=window,
        min_samples=window,
    )


def zscore_rolling(
    expr: pl.Expr,
    window: int,
) -> pl.Expr:
    """Compute rolling z-score.

    z_t = (x_t - mean(x)) / std(x)

    Args:
        expr (pl.Expr): Input expression.
        window (int): Rolling window size.

    Returns:
        pl.Expr: Polars expression for rolling z-score.
    """
    mean = expr.rolling_mean(
        window_size=window,
        min_samples=window,
    )
    std = expr.rolling_std(
        window_size=window,
        min_samples=window,
    )

    return (expr - mean) / (std + _EPS)


def clip_expr(
    expr: pl.Expr,
    lo: float = -5.0,
    hi: float = 5.0,
) -> pl.Expr:
    """Clip expression values to a fixed range.

    Args:
        expr (pl.Expr): Input expression.
        lo (float): Lower bound.
        hi (float): Upper bound.

    Returns:
        pl.Expr: Clipped Polars expression.
    """
    return expr.clip(lo, hi)
