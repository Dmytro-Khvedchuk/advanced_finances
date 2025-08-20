import polars as pl
from typing import Any
import numpy as np
from tqdm import tqdm


def build_dollar_imbalance_bars(
    data, *, alpha: float = 1.0, ema_span: int = 50, warmup_ticks: int = 200
) -> tuple[pl.DataFrame | Any, pl.DataFrame]:
    """
    Build dollar imbalance bars from raw data
    :param data: raw fetched data from binance, or directly a polars dataframe
    :param alpha: Scaling factor in the stopping rule threshold.
    :param ema_span: Span for EMA updates of expected ticks per bar and expected imbalance.
        (EMA alpha is computed as 2/(span+1)).
    :param warmup_ticks: Use the first `warmup_ticks` trades to seed initial expectations.
        If not enough ticks exist, the function degrades gracefully.
    :return: dollar imbalance bars, unfinished part
    """

    if not isinstance(data, pl.DataFrame):
        df = pl.DataFrame(data)
    else:
        df = data

    df = df.select(
        pl.col("price").cast(pl.Float64),
        pl.col("qty").cast(pl.Float64),
        pl.col("time").cast(pl.Int64),
        pl.col("id").cast(pl.Int64),
        pl.col("isBuyerMaker").cast(pl.Boolean),
    ).sort("time")

    # buyer_taker = True if buyer initiated trade
    df = df.with_columns((~pl.col("isBuyerMaker")).alias("buyer_taker"))

    # convert to numpy for speed
    prices = df["price"].to_numpy()
    qtys = df["qty"].to_numpy()
    buyer_taker = df["buyer_taker"].to_numpy()
    ids = df["id"].to_numpy()
    times = df["time"].to_numpy()

    # signed dollar flow
    signs = np.where(buyer_taker, 1.0, -1.0)
    dollar_flows = signs * prices * qtys

    # EMA params
    ema_alpha = 2 / (ema_span + 1)
    ema = np.mean(np.abs(dollar_flows[:warmup_ticks]))  # initial EMA

    bars = []
    bar_id = 0
    signed_dollar_sum = 0.0
    bar_start_idx = 0

    for i in tqdm(range(len(dollar_flows)), desc="Processing rows"):
        signed_dollar_sum += dollar_flows[i]

        # update EMA recursively after warmup
        if i >= warmup_ticks:
            ema = ema_alpha * abs(dollar_flows[i]) + (1 - ema_alpha) * ema
            threshold = alpha * ema

            if abs(signed_dollar_sum) >= threshold:
                # slice the ticks for this bar
                sl = slice(bar_start_idx, i + 1)
                bar_df = pl.DataFrame(
                    {
                        "time": times[sl],
                        "price": prices[sl],
                        "qty": qtys[sl],
                        "id": ids[sl],
                    }
                ).with_columns(pl.lit(bar_id).alias("bar_id"))

                bars.append(bar_df)

                bar_id += 1
                bar_start_idx = i + 1
                signed_dollar_sum = 0.0

    if not bars:
        return pl.DataFrame(), pl.DataFrame()

    final = pl.concat(bars)

    # aggregate bars like OHLCV
    result = (
        final.group_by("bar_id", maintain_order=True)
        .agg(
            [
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
            ]
        )
        .sort("bar_id")
    )

    unfinished_part = pl.DataFrame()

    return result, unfinished_part
