from polars import Boolean, col, concat, DataFrame, Float64, Int64, when
from typing import Any


def build_volume_imbalance_bars(
    data: DataFrame,
    *,
    alpha: float = 1.0,
    ema_span: int = 50,
    warmup_ticks: int = 200,
) -> tuple[DataFrame | Any, DataFrame]:
    """
    Build volume imbalance bars from raw data

    :param data: Polars DataFrame
    :type data: pl.DataFrame
    :param alpha: Scaling factor in the stopping rule threshold.
    :type alpha: float
    :param ema_span: Span for EMA updates of expected volume per bar and expected imbalance.
        (EMA alpha is computed as 2/(span+1)).
    :type ema_span: int
    :param warmup_ticks: Use the first `warmup_ticks` trades to seed initial expectations.
        If not enough ticks exist, the function degrades gracefully.
    :type warmup_ticks: int
    :returns: volume imbalance bars, unfinished part
    """
    if isinstance(data, DataFrame):
        df = data
    else:
        df = DataFrame(data)

    df = df.select(
        col("price").cast(Float64),
        col("qty").cast(Float64),
        col("time").cast(Int64),
        col("id").cast(Int64),
        col("isBuyerMaker").cast(Boolean),
    ).sort("time")

    df = df.with_columns(
        when(~col("isBuyerMaker"))
        .then(col("qty"))
        .otherwise(-col("qty"))
        .alias("signed_qty"),
        (~col("isBuyerMaker")).alias("buyer_taker"),
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

            bars.append(
                DataFrame(
                    {
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
                    }
                )
            )

            bar_id += 1
            cum_vol = 0.0
            signed_cum_vol = 0.0
            bar_start_idx = i + 1

    bars = concat(bars) if bars else DataFrame()

    unfinished_part = DataFrame()

    return bars, unfinished_part
