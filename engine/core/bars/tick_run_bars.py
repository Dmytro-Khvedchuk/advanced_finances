from numpy import concat, lit, mean, where, zeros
from polars import Boolean, col, count, DataFrame, Float64, Int64
from tqdm import tqdm
from typing import Any


def build_tick_run_bars(
    data: DataFrame, *, alpha: float = 1.0, ema_span: int = 50, warmup_ticks: int = 200
) -> tuple[DataFrame | Any, DataFrame]:
    """
    Build tick run bars from raw data

    :param data: Polars DataFrame of a trades data
    :type data: pl.DataFrame
    :param alpha: Scaling factor in the stopping rule threshold.
    :type alpha: float
    :param ema_span: Span for EMA updates of expected ticks per bar and expected imbalance.
        (EMA alpha is computed as 2/(span+1)).
    :type ema_span: int
    :param warmup_ticks: Use the first `warmup_ticks` trades to seed initial expectations.
        If not enough ticks exist, the function degrades gracefully.
    :type warmup_ticks: int
    :returns: tick run bars, unfinished part
    """

    if not isinstance(data, DataFrame):
        df = DataFrame(data)
    else:
        df = data

    df = df.select(
        [
            col("price").cast(Float64),
            col("qty").cast(Float64),
            col("time").cast(Int64),
            col("id").cast(Int64),
            col("isBuyerMaker").cast(Boolean),
        ]
    ).sort("time")

    df = df.with_columns((~col("isBuyerMaker")).alias("buyer_taker"))

    buyer_taker = df["buyer_taker"].to_numpy()
    n = len(buyer_taker)

    signs = where(buyer_taker, 1, -1)

    run_lengths = zeros(n, dtype=int)
    prev_sign = 0
    for i in tqdm(range(n), desc="Calculating run lengths"):
        if i == 0 or signs[i] == prev_sign:
            run_lengths[i] = run_lengths[i - 1] + 1 if i > 0 else 1
        else:
            run_lengths[i] = 1
        prev_sign = signs[i]

    ema = zeros(n, dtype=float)
    alpha_ema = 2 / (ema_span + 1)
    for i in tqdm(range(n), desc="Calculating EMA"):
        if i < warmup_ticks:
            ema[i] = mean(run_lengths[: i + 1])
        else:
            ema[i] = alpha_ema * run_lengths[i] + (1 - alpha_ema) * ema[i - 1]

    threshold = alpha * ema

    bar_breaks = run_lengths >= threshold
    bar_indices = where(bar_breaks)[0]

    if len(bar_indices) == 0:
        return DataFrame(), DataFrame()

    bars_list = []
    start_idx = 0
    bar_id = 0
    for end_idx in tqdm(bar_indices, desc="Building bars"):
        bar_df = df[start_idx : end_idx + 1].with_columns(lit(bar_id).alias("bar_id"))
        bars_list.append(bar_df)
        start_idx = end_idx + 1
        bar_id += 1

    if bars_list:
        final = concat(bars_list)
    else:
        final = DataFrame()

    if not final.is_empty():
        bars = (
            final.group_by("bar_id", maintain_order=True)
            .agg(
                [
                    col("time").first().alias("start_time"),
                    col("time").last().alias("end_time"),
                    col("price").first().alias("open"),
                    col("price").max().alias("high"),
                    col("price").min().alias("low"),
                    col("price").last().alias("close"),
                    count().alias("n_ticks"),
                    col("qty").sum().alias("base_volume"),
                    (col("price") * col("qty")).sum().alias("quote_volume"),
                    col("id").first().alias("first_trade_id"),
                    col("id").last().alias("last_trade_id"),
                ]
            )
            .sort("bar_id")
        )
    else:
        bars = DataFrame()

    unfinished_part = df[start_idx:] if start_idx < n else DataFrame()

    return bars, unfinished_part
