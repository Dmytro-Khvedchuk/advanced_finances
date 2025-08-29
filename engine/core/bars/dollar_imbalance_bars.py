from typing import Any
from polars import Boolean, col, concat, DataFrame, Float64, Int64, lit
from tqdm import tqdm
from numpy import where, mean, abs


def build_dollar_imbalance_bars(
    data: DataFrame, *, alpha: float = 1.0, ema_span: int = 50, warmup_ticks: int = 200
) -> tuple[DataFrame | Any, DataFrame]:
    """
    Build dollar imbalance bars from raw data

    :param data: Polars dataframe that contains trades data
    :type data: pl.DataFrame
    :param alpha: Scaling factor in the stopping rule threshold.
    :type alpha: float
    :param ema_span: Span for EMA updates of expected ticks per bar and expected imbalance.
        (EMA alpha is computed as 2/(span+1)).
    :type ema_span: int
    :param warmup_ticks: Use the first `warmup_ticks` trades to seed initial expectations.
        If not enough ticks exist, the function degrades gracefully.
    :type warmup_ticks: int
    :returns: dollar imbalance bars, unfinished part
    """

    if not isinstance(data, DataFrame):
        df = DataFrame(data)
    else:
        df = data

    df = df.select(
        col("price").cast(Float64),
        col("qty").cast(Float64),
        col("time").cast(Int64),
        col("id").cast(Int64),
        col("isBuyerMaker").cast(Boolean),
    ).sort("time")

    df = df.with_columns((~col("isBuyerMaker")).alias("buyer_taker"))

    prices = df["price"].to_numpy()
    qtys = df["qty"].to_numpy()
    buyer_taker = df["buyer_taker"].to_numpy()
    ids = df["id"].to_numpy()
    times = df["time"].to_numpy()

    signs = where(buyer_taker, 1.0, -1.0)
    dollar_flows = signs * prices * qtys

    ema_alpha = 2 / (ema_span + 1)
    ema = mean(abs(dollar_flows[:warmup_ticks]))

    bars = []
    bar_id = 0
    signed_dollar_sum = 0.0
    bar_start_idx = 0

    for i in tqdm(range(len(dollar_flows)), desc="Processing rows"):
        signed_dollar_sum += dollar_flows[i]

        if i >= warmup_ticks:
            ema = ema_alpha * abs(dollar_flows[i]) + (1 - ema_alpha) * ema
            threshold = alpha * ema

            if abs(signed_dollar_sum) >= threshold:
                sl = slice(bar_start_idx, i + 1)
                bar_df = DataFrame(
                    {
                        "time": times[sl],
                        "price": prices[sl],
                        "qty": qtys[sl],
                        "id": ids[sl],
                    }
                ).with_columns(lit(bar_id).alias("bar_id"))

                bars.append(bar_df)

                bar_id += 1
                bar_start_idx = i + 1
                signed_dollar_sum = 0.0

    if not bars:
        return DataFrame(), DataFrame()

    final = concat(bars)

    result = (
        final.group_by("bar_id", maintain_order=True)
        .agg(
            [
                col("time").first().alias("start_time"),
                col("time").last().alias("end_time"),
                col("price").first().alias("open"),
                col("price").max().alias("high"),
                col("price").min().alias("low"),
                col("price").last().alias("close"),
                len().alias("n_ticks"),
                col("qty").sum().alias("base_volume"),
                (col("price") * col("qty")).sum().alias("quote_volume"),
                col("id").first().alias("first_trade_id"),
                col("id").last().alias("last_trade_id"),
            ]
        )
        .sort("bar_id")
    )

    unfinished_part = DataFrame()

    return result, unfinished_part
