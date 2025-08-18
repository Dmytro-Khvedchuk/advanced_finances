import polars as pl


def build_volume_imbalance_bars(
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

    df = df.with_columns(
        pl.when(~pl.col("isBuyerMaker")).then(pl.col("qty")).otherwise(-pl.col("qty")).alias("signed_qty"),
        (~pl.col("isBuyerMaker")).alias("buyer_taker")
    )

    bars = []
    bar_id = 0

    cum_vol = 0.0
    signed_cum_vol = 0.0
    ema = None
    ema_alpha = 2 / (ema_span + 1)

    bar_start_idx = 0

    for i, row in enumerate(df.iter_rows(named=True)):
        qty = row["qty"]
        signed = row["signed_qty"]

        cum_vol += qty
        signed_cum_vol += signed

        if ema is None:
            ema = abs(signed)
        else:
            ema = ema_alpha * abs(signed) + (1 - ema_alpha) * ema

        threshold = alpha * ema

        if abs(signed_cum_vol) >= threshold and i >= warmup_ticks:
            bar = df.slice(bar_start_idx, i - bar_start_idx + 1)

            bars.append(pl.DataFrame({
                "bar_id": [bar_id],
                "start_time": [bar["time"][0]],
                "end_time": [bar["time"][-1]],
                "open": [bar["price"][0]],
                "high": [bar["price"].max()],
                "low": [bar["price"].min()],
                "close": [bar["price"][-1]],
                "n_ticks": [bar.height],
                "base_volume": [bar["qty"].sum()],
                "quote_volume": [(bar["price"] * bar["qty"]).sum()],
                "buy_volume": [bar.filter(bar["buyer_taker"])["qty"].sum()],
                "sell_volume": [bar.filter(~bar["buyer_taker"])["qty"].sum()],
                "signed_volume_sum": [bar["signed_qty"].sum()],
                "first_trade_id": [bar["id"][0]],
                "last_trade_id": [bar["id"][-1]],
            }))

            bar_id += 1
            cum_vol = 0.0
            signed_cum_vol = 0.0
            bar_start_idx = i + 1

    bars_df = pl.concat(bars) if bars else pl.DataFrame()

    if drop_last_incomplete and bars_df.height > 0:
        bars_df = bars_df.head(-1)

    return bars_df
