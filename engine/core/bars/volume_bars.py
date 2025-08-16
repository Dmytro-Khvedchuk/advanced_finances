import polars as pl

def build_volume_bars(data, bar_size: float, drop_last_incomplete: bool = False) -> pl.DataFrame:
    df = pl.DataFrame(data).select(
        pl.col('price').cast(pl.Float64),
        pl.col('qty').cast(pl.Float64),
        pl.col('id').cast(pl.Int64),
        pl.col('isBuyerMaker').cast(pl.Boolean),
    ).sort('id')

    df = df.with_columns(pl.col("qty").cum_sum().alias("cum_vol"))

    df = df.with_columns(
        ((pl.col("cum_vol") / bar_size).floor()).alias("bar_index")
    )

    bars = (
        df.group_by("bar_index")
        .agg([
            pl.col("id").first().alias("open_id"),
            pl.col("id").last().alias("close_id"),
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("qty").sum().alias("volume"),
        ])
        .sort("bar_index")
    )

    if drop_last_incomplete:
        # Drop last bar if total volume < bar_size
        bars = bars.filter(pl.col("volume") >= bar_size)

    return bars