import polars as pl

def build_dollar_bars(data, bar_size: float) -> pl.DataFrame:
    df = pl.DataFrame(data).select(
        pl.col('price').cast(pl.Float64),
        pl.col('quoteQty').cast(pl.Float64),
        pl.col('id').cast(pl.Int64),
        pl.col('isBuyerMaker').cast(pl.Boolean),
    ).sort('id')

    bar_id = 1
    bars = pl.DataFrame()

    while not df.is_empty():
        df = df.with_columns(pl.col("quoteQty").cum_sum().alias("cum_dol"))

        cross_mask = (pl.col("cum_dol") >= bar_size) & (pl.col("cum_dol").shift(1) < bar_size)

        df_until_cross = df.filter((pl.col("cum_dol") < bar_size) | cross_mask)

        if df_until_cross.is_empty():
            break

        bar_row = df_until_cross.select([
            pl.lit(bar_id).alias("bar_id"),
            pl.col("id").first().alias("open_id"),
            pl.col("id").last().alias("close_id"),
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("quoteQty").sum().alias("dollars"),
            pl.len().alias("trades"),
        ])

        bars = bar_row if bars.is_empty() else pl.concat([bars, bar_row], how="vertical")

        df = df.join(df_until_cross.select("id"), on="id", how="anti").drop("cum_dol")

        bar_id += 1

    unfinished_part = 0

    return bars, unfinished_part