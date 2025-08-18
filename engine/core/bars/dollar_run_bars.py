import polars as pl
import numpy as np
from tqdm import tqdm


def build_dollar_run_bars(
    data,
    *,
    alpha: float = 1.0,
    ema_span: int = 50,
    warmup_ticks: int = 200,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Ultra-fast dollar run bars using NumPy vectorization
    """
    # Convert to Polars if needed
    if isinstance(data, pl.DataFrame):
        df = data
    else:
        df = pl.DataFrame(data)

    df = df.select(
        [
            pl.col("price").cast(pl.Float64),
            pl.col("qty").cast(pl.Float64),
            pl.col("quoteQty").cast(pl.Float64),
            pl.col("time").cast(pl.Int64),
            pl.col("id").cast(pl.Int64),
            pl.col("isBuyerMaker").cast(pl.Boolean),
        ]
    ).sort("time")

    # Buyer/taker vector
    buyer_taker = (~df["isBuyerMaker"]).to_numpy()
    signs = np.where(buyer_taker, 1, -1)
    dollar = df["quoteQty"].to_numpy() * signs
    n = len(df)

    # Run-dollar calculation
    run_dollar = np.zeros(n)
    prev_sign = signs[0]
    acc = 0.0

    for i in tqdm(range(n), desc="Calculating run dollars"):
        if signs[i] == prev_sign:
            acc += abs(dollar[i])
        else:
            acc = abs(dollar[i])
        run_dollar[i] = acc
        prev_sign = signs[i]

    # EMA of past run dollars
    ema_dollar = np.zeros(n)
    alpha_ema = 2 / (ema_span + 1)
    for i in tqdm(range(n), desc="Calculating EMA of run dollars"):
        if i < warmup_ticks:
            ema_dollar[i] = np.mean(run_dollar[: i + 1])
        else:
            ema_dollar[i] = (
                alpha_ema * run_dollar[i] + (1 - alpha_ema) * ema_dollar[i - 1]
            )

    thresholds = alpha * ema_dollar

    # Identify bar breaks
    bar_breaks = run_dollar >= thresholds
    bar_indices = np.where(bar_breaks)[0]

    bars_list = []
    start_idx = 0
    bar_id = 0

    for end_idx in tqdm(bar_indices, desc="Building bars"):
        bar_df = df[start_idx : end_idx + 1].with_columns(
            pl.lit(bar_id).alias("bar_id")
        )
        bars_list.append(bar_df)
        start_idx = end_idx + 1
        bar_id += 1

    # Concatenate completed bars
    final = pl.concat(bars_list) if bars_list else pl.DataFrame()

    # Aggregate OHLCV per bar
    if not final.is_empty():
        bars = (
            final.group_by("bar_id", maintain_order=True)
            .agg(
                [
                    pl.col("time").first().alias("start_time"),
                    pl.col("time").last().alias("end_time"),
                    pl.col("price").first().alias("open"),
                    pl.col("price").max().alias("high"),
                    pl.col("price").min().alias("low"),
                    pl.col("price").last().alias("close"),
                    pl.count().alias("n_ticks"),
                    pl.col("qty").sum().alias("base_volume"),
                    (pl.col("price") * pl.col("qty")).sum().alias("quote_volume"),
                    pl.col("id").first().alias("first_trade_id"),
                    pl.col("id").last().alias("last_trade_id"),
                ]
            )
            .sort("bar_id")
        )
    else:
        bars = pl.DataFrame()

    # Return unfinished part
    unfinished_bars = df[start_idx:] if start_idx < n else pl.DataFrame()

    return bars, unfinished_bars
