from polars import col, concat, DataFrame, Float64, Int64, len, lit
from typing import Any


def build_volume_bars(
    data: DataFrame, bar_size: float
) -> tuple[DataFrame | Any, DataFrame]:
    """
    Build a volume bars from raw data

    :param data: Polars DataFrame of trades data
    :type data: pl.DataFrame
    :param bar_size: Amount of dollars for a bar formation
    :type bar_size: float
    :returns: volume bars, unfinished part
    """
    if isinstance(data, DataFrame):
        df = data
    else:
        df = DataFrame(data)

    df = df.select(
        col("price").cast(Float64),
        col("qty").cast(Float64),
        col("id").cast(Int64),
    ).sort("id")

    bar_id = 1
    bars = DataFrame()

    while not df.is_empty():
        df = df.with_columns(col("qty").cum_sum().alias("cumulative_volume"))

        cross_mask = (col("cumulative_volume") >= bar_size) & (
            col("cumulative_volume").shift(1) < bar_size
        )

        df_until_cross = df.filter((col("cumulative_volume") < bar_size) | cross_mask)

        if df_until_cross.is_empty():
            break

        bar_row = df_until_cross.select(
            [
                lit(bar_id).alias("bar_id"),
                col("id").first().alias("open_id"),
                col("id").last().alias("close_id"),
                col("price").first().alias("open"),
                col("price").max().alias("high"),
                col("price").min().alias("low"),
                col("price").last().alias("close"),
                col("qty").sum().alias("volume"),
                len().alias("trades"),
            ]
        )

        bars = bar_row if bars.is_empty() else concat([bars, bar_row], how="vertical")

        df = df.join(df_until_cross.select("id"), on="id", how="anti").drop(
            "cumulative_volume"
        )

        bar_id += 1

    unfinished_part = DataFrame()

    return bars, unfinished_part
