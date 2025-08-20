import polars as pl
from typing import Any
from tqdm import tqdm
import numpy as np


def build_volume_run_bars(
    data, *, alpha: float = 1.0, ema_span: int = 50, warmup_ticks: int = 200
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    build volume run bars from raw data
    :param data: raw fetched data from binance, or directly a polars dataframe
    :param alpha: Scaling factor in the stopping rule threshold.
    :param ema_span: Span for EMA updates of expected ticks per bar and expected imbalance.
        (EMA alpha is computed as 2/(span+1)).
    :param warmup_ticks: Use the first `warmup_ticks` trades to seed initial expectations.
        If not enough ticks exist, the function degrades gracefully.
    :return: volume run bars, unfinished part
    """
    if isinstance(data, pl.DataFrame):
        df = data
    else:
        df = pl.DataFrame(data)

    df = df.select(
        [
            pl.col("price").cast(pl.Float64),
            pl.col("qty").cast(pl.Float64),
            pl.col("time").cast(pl.Int64),
            pl.col("id").cast(pl.Int64),
            pl.col("isBuyerMaker").cast(pl.Boolean),
        ]
    ).sort("time")

    # Buyer/taker vector
    buyer_taker = (~df["isBuyerMaker"]).to_numpy()
    signs = np.where(buyer_taker, 1, -1)
    qtys = df["qty"].to_numpy()
    n = len(df)

    # Run volume calculation
    run_volumes = np.zeros(n)
    prev_sign = signs[0]
    run_vol = 0.0

    for i in tqdm(range(n), desc="Calculating run volumes"):
        if signs[i] == prev_sign:
            run_vol += abs(qtys[i])
        else:
            run_vol = abs(qtys[i])
        run_volumes[i] = run_vol
        prev_sign = signs[i]

    # EMA of past run volumes
    ema_vol = np.zeros(n)
    alpha_ema = 2 / (ema_span + 1)
    for i in tqdm(range(n), desc="Calculating EMA of run volumes"):
        if i < warmup_ticks:
            ema_vol[i] = np.mean(run_volumes[: i + 1])
        else:
            ema_vol[i] = alpha_ema * run_volumes[i] + (1 - alpha_ema) * ema_vol[i - 1]

    # Thresholds
    thresholds = alpha * ema_vol

    # Identify bar breaks
    bar_breaks = run_volumes >= thresholds
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

    # Concatenate all completed bars
    final = pl.concat(bars_list) if bars_list else pl.DataFrame()

    # Build OHLCV per bar
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

    # Unfinished bars
    unfinished_bars = df[start_idx:] if start_idx < n else pl.DataFrame()

    return bars, unfinished_bars
