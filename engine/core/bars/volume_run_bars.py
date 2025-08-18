import polars as pl


def build_volume_run_bars(
        data,
        *,
        alpha: float = 1.0,
        ema_span: int = 50,
        warmup_ticks: int = 200,
        drop_last_incomplete: bool = True,
) -> pl.DataFrame:
    df = pl.DataFrame(data).select(
        pl.col("price").cast(pl.Float64),
        pl.col("qty").cast(pl.Float64),
        pl.col("time").cast(pl.Int64),
        pl.col("id").cast(pl.Int64),
        pl.col("isBuyerMaker").cast(pl.Boolean),
    ).sort("time")

    df = df.with_columns((~pl.col("isBuyerMaker")).alias("buyer_taker"))

    bars = []
    bar_ticks = []
    bar_id = 0

    run_volume = 0.0
    prev_sign = None
    run_history = []

    for i, row in enumerate(df.iter_rows(named=True)):
        sign = 1 if row["buyer_taker"] else -1
        vol = row["qty"] * sign
        bar_ticks.append(row)

        if prev_sign is None or sign == prev_sign:
            run_volume += abs(row["qty"])
        else:
            run_history.append(run_volume)
            run_volume = abs(row["qty"])
        prev_sign = sign

        if i < warmup_ticks:
            continue

        ema = sum(run_history[-ema_span:]) / len(run_history[-ema_span:]) if run_history else 1.0
        threshold = alpha * ema

        if run_volume >= threshold:
            bar_df = pl.DataFrame(bar_ticks)
            bars.append(bar_df.with_columns(pl.lit(bar_id).alias("bar_id")))
            bar_id += 1

            bar_ticks = []
            run_volume = 0.0
            prev_sign = None
            run_history = []

    if not bar_ticks or drop_last_incomplete:
        final = pl.concat(bars) if bars else pl.DataFrame()
    else:
        final = pl.concat(bars + [pl.DataFrame(bar_ticks).with_columns(pl.lit(bar_id).alias("bar_id"))])

    return final.group_by("bar_id", maintain_order=True).agg([
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
    ]).sort("bar_id")
