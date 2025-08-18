import polars as pl
from typing import Any


def build_tick_bars(
    data, bar_size: int = 10
) -> tuple[pl.DataFrame | Any, pl.DataFrame]:
    """
    Build a tick bars from raw data
    :param data: raw fetched data from binance, or directly a polars dataframe
    :param bar_size: amount of ticks for a bar formation
    :return: tick bars, unfinished part
    """
    if isinstance(data, pl.DataFrame):
        df = data
    else:
        df = pl.DataFrame(data)

    df = df.select(
        pl.col("price").cast(pl.Float64),
        pl.col("qty").cast(pl.Float64),
        pl.col("time").cast(pl.Int64),
        pl.col("id").cast(pl.Int64),
        pl.col("isBuyerMaker").cast(pl.Boolean),
    ).sort("time")

    df = (
        df.with_row_index(name="row_idx")
        .with_columns((pl.col("row_idx") // bar_size).alias("bar_id"))
        .with_columns((~pl.col("isBuyerMaker")).alias("buyer_taker"))  # precompute once
    )

    bars = (
        df.group_by("bar_id", maintain_order=True)
        .agg(
            [
                # times
                pl.col("time").first().alias("start_time"),
                pl.col("time").last().alias("end_time"),
                # OHLC
                pl.col("price").first().alias("open"),
                pl.col("price").max().alias("high"),
                pl.col("price").min().alias("low"),
                pl.col("price").last().alias("close"),
                pl.len().alias("n_ticks"),
                pl.col("qty").sum().alias("base_volume"),
                (pl.col("price") * pl.col("qty")).sum().alias("quote_volume"),
                pl.col("buyer_taker").cast(pl.Int64).sum().alias("buy_ticks"),
                pl.when(pl.col("buyer_taker"))
                .then(pl.col("qty"))
                .otherwise(0.0)
                .sum()
                .alias("buy_volume"),
                (~pl.col("buyer_taker")).cast(pl.Int64).sum().alias("sell_ticks"),
                pl.when(~pl.col("buyer_taker"))
                .then(pl.col("qty"))
                .otherwise(0.0)
                .sum()
                .alias("sell_volume"),
                pl.when(pl.col("buyer_taker"))
                .then(1)
                .otherwise(-1)
                .sum()
                .alias("signed_tick_sum"),
                pl.when(pl.col("buyer_taker"))
                .then(pl.col("qty"))
                .otherwise(-pl.col("qty"))
                .sum()
                .alias("signed_volume_sum"),
                pl.col("id").first().alias("first_trade_id"),
                pl.col("id").last().alias("last_trade_id"),
            ]
        )
        .sort("bar_id")
    )

    unfinished_part = pl.DataFrame()

    return bars, unfinished_part
