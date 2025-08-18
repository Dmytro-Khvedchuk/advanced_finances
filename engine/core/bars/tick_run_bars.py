import polars as pl


def build_tick_run_bars(
        data,
        *,
        alpha: float = 1.0,
        ema_span: int = 50,
        warmup_ticks: int = 200,
        drop_last_incomplete: bool = True,
) -> pl.DataFrame:
    """
    Build Tick Run Bars (TRBs) as described by López de Prado (2018).

    Parameters
    ----------
    data : list[dict] or pl.DataFrame
        Trades with columns: price, qty, time, id, isBuyerMaker
    alpha : float
        Sensitivity multiplier for threshold.
    ema_span : int
        Span for EMA of expected run length.
    warmup_ticks : int
        Number of ticks to collect before activating run rule.
    drop_last_incomplete : bool
        Whether to drop last unfinished bar.
    """

    df = pl.DataFrame(data).select(
        pl.col("price").cast(pl.Float64),
        pl.col("qty").cast(pl.Float64),
        pl.col("time").cast(pl.Int64),
        pl.col("id").cast(pl.Int64),
        pl.col("isBuyerMaker").cast(pl.Boolean),
    ).sort("time")

    # buyer_taker = True if buyer initiated trade
    df = df.with_columns((~pl.col("isBuyerMaker")).alias("buyer_taker"))

    bars = []
    bar_ticks = []
    bar_id = 0

    run_length = 0
    prev_sign = None
    run_history = []

    for i, row in enumerate(df.iter_rows(named=True)):
        sign = 1 if row["buyer_taker"] else -1
        bar_ticks.append(row)

        # update run length
        if prev_sign is None or sign == prev_sign:
            run_length += 1
        else:
            run_history.append(run_length)
            run_length = 1
        prev_sign = sign

        if i < warmup_ticks:
            continue

        # compute EMA of past run lengths
        if len(run_history) > ema_span:
            weights = [(1 - 2 / (ema_span + 1)) ** k for k in range(len(run_history))]
            denom = sum(weights)
            ema = sum(v * w for v, w in zip(reversed(run_history), weights)) / denom
        else:
            ema = sum(run_history) / len(run_history) if run_history else 1.0

        threshold = alpha * ema

        # close bar if run length >= threshold
        if run_length >= threshold:
            bar_df = pl.DataFrame(bar_ticks)
            bars.append(bar_df.with_columns(pl.lit(bar_id).alias("bar_id")))
            bar_id += 1

            # reset state
            bar_ticks = []
            run_length = 0
            prev_sign = None
            run_history = []

    if not bar_ticks or drop_last_incomplete:
        final = pl.concat(bars)
    else:
        final = pl.concat(bars + [pl.DataFrame(bar_ticks).with_columns(pl.lit(bar_id).alias("bar_id"))])

    # aggregate bars to OHLCV
    result = (
        final.group_by("bar_id", maintain_order=True)
        .agg([
            pl.col("time").first().alias("start_time"),
            pl.col("time").last().alias("end_time"),

            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),

            pl.len().alias("n_ticks"),
            pl.col("qty").sum().alias("base_volume"),
            (pl.col("price") * pl.col("qty")).sum().alias("quote_volume"),

            pl.col("id").first().alias("first_trade_id"),
            pl.col("id").last().alias("last_trade_id"),
        ])
        .sort("bar_id")
    )

    return result
