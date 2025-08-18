import polars as pl

COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "trades",
    "taker_base",
    "taker_quote",
    "ignore",
]

NUMERIC_COLS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_asset_volume",
    "taker_base",
    "taker_quote",
]


def build_time_bars(data) -> pl.DataFrame:
    df = pl.DataFrame(data, schema=COLUMNS, orient="row").drop("ignore")

    df = df.with_columns(
        [
            pl.col("open_time").cast(pl.Int64).cast(pl.Datetime("ms")),
            pl.col("close_time").cast(pl.Int64).cast(pl.Datetime("ms")),
        ]
    )

    df = df.with_columns([pl.col(c).cast(pl.Float64) for c in NUMERIC_COLS])
    df = df.with_columns(pl.col("trades").cast(pl.Int64))

    return df
