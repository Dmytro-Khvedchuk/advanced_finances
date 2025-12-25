from polars import Boolean, col, DataFrame, Float64, Int64, len, when
from typing import Any


def build_tick_bars(data, bar_size: int = 10) -> tuple[DataFrame | Any, DataFrame]:
    """
    Build a tick bars from raw data

    :param data: Polars DataFrame with trades data
    :type data: pl.DataFrame
    :param bar_size: Amount of ticks for a bar formation
    :type bar_size: int
    :return: tick bars, unfinished part
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

    df = (
        df.with_row_index(name="row_idx")
        .with_columns((col("row_idx") // bar_size).alias("bar_id"))
        .with_columns((~col("isBuyerMaker")).alias("buyer_taker"))
    )

    bars = (
        df.group_by("bar_id", maintain_order=True)
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
                col("buyer_taker").cast(Int64).sum().alias("buy_ticks"),
                when(col("buyer_taker"))
                .then(col("qty"))
                .otherwise(0.0)
                .sum()
                .alias("buy_volume"),
                (~col("buyer_taker")).cast(Int64).sum().alias("sell_ticks"),
                when(~col("buyer_taker"))
                .then(col("qty"))
                .otherwise(0.0)
                .sum()
                .alias("sell_volume"),
                when(col("buyer_taker"))
                .then(1)
                .otherwise(-1)
                .sum()
                .alias("signed_tick_sum"),
                when(col("buyer_taker"))
                .then(col("qty"))
                .otherwise(-col("qty"))
                .sum()
                .alias("signed_volume_sum"),
                col("id").first().alias("first_trade_id"),
                col("id").last().alias("last_trade_id"),
            ]
        )
        .sort("bar_id")
    )

    unfinished_part = DataFrame()

    return bars, unfinished_part
