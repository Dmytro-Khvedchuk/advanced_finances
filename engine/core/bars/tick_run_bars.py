import polars as pl
from typing import Any
from tqdm import tqdm
import numpy as np


def build_tick_run_bars(
    data, *, alpha: float = 1.0, ema_span: int = 50, warmup_ticks: int = 200
) -> tuple[pl.DataFrame | Any, pl.DataFrame]:
    """
    Build tick run bars from raw data
    :param data: raw fetched data from binance, or directly a polars dataframe
    :param alpha: Scaling factor in the stopping rule threshold.
    :param ema_span: Span for EMA updates of expected ticks per bar and expected imbalance.
        (EMA alpha is computed as 2/(span+1)).
    :param warmup_ticks: Use the first `warmup_ticks` trades to seed initial expectations.
        If not enough ticks exist, the function degrades gracefully.
    :return: tick run bars, unfinished part
    """

    if not isinstance(data, pl.DataFrame):
        df = pl.DataFrame(data)
    else:
        df = data

    df = df.select(
        [
            pl.col("price").cast(pl.Float64),
            pl.col("qty").cast(pl.Float64),
            pl.col("time").cast(pl.Int64),
            pl.col("id").cast(pl.Int64),
            pl.col("isBuyerMaker").cast(pl.Boolean),
        ]
    ).sort("time")

    # buyer_taker = True if buyer initiated trade
    df = df.with_columns((~pl.col("isBuyerMaker")).alias("buyer_taker"))

    # Convert to NumPy for vectorized processing
    buyer_taker = df["buyer_taker"].to_numpy()
    n = len(buyer_taker)

    signs = np.where(buyer_taker, 1, -1)

    # Run-length encoding
    run_lengths = np.zeros(n, dtype=int)
    prev_sign = 0
    for i in tqdm(range(n), desc="Calculating run lengths"):
        if i == 0 or signs[i] == prev_sign:
            run_lengths[i] = run_lengths[i - 1] + 1 if i > 0 else 1
        else:
            run_lengths[i] = 1
        prev_sign = signs[i]

    # EMA of run lengths
    ema = np.zeros(n, dtype=float)
    alpha_ema = 2 / (ema_span + 1)
    for i in tqdm(range(n), desc="Calculating EMA"):
        if i < warmup_ticks:
            ema[i] = np.mean(run_lengths[: i + 1])
        else:
            ema[i] = alpha_ema * run_lengths[i] + (1 - alpha_ema) * ema[i - 1]

    threshold = alpha * ema

    # Identify bar breaks
    bar_breaks = run_lengths >= threshold
    bar_indices = np.where(bar_breaks)[0]

    if len(bar_indices) == 0:
        return pl.DataFrame(), pl.DataFrame()

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

    if bars_list:
        final = pl.concat(bars_list)
    else:
        final = pl.DataFrame()

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

    # Any leftover ticks
    unfinished_part = df[start_idx:] if start_idx < n else pl.DataFrame()

    return bars, unfinished_part
