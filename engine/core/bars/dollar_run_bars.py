from numpy import concat, mean, where, zeros
from polars import Boolean, col, count, DataFrame, Float64, Int64, lit
from tqdm import tqdm


def build_dollar_run_bars(
    data: DataFrame,
    *,
    alpha: float = 1.0,
    ema_span: int = 50,
    warmup_ticks: int = 200,
) -> tuple[DataFrame, DataFrame]:
    """
    Build dollar run bars from raw data

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
    :returns: dollar run bars, unfinished part
    """
    if isinstance(data, DataFrame):
        df = data
    else:
        df = DataFrame(data)

    df = df.select(
        [
            col("price").cast(Float64),
            col("qty").cast(Float64),
            col("quoteQty").cast(Float64),
            col("time").cast(Int64),
            col("id").cast(Int64),
            col("isBuyerMaker").cast(Boolean),
        ]
    ).sort("time")

    buyer_taker = (~df["isBuyerMaker"]).to_numpy()
    signs = where(buyer_taker, 1, -1)
    dollar = df["quoteQty"].to_numpy() * signs
    n = len(df)

    run_dollar = zeros(n)
    prev_sign = signs[0]
    acc = 0.0

    for i in tqdm(range(n), desc="Calculating run dollars"):
        if signs[i] == prev_sign:
            acc += abs(dollar[i])
        else:
            acc = abs(dollar[i])
        run_dollar[i] = acc
        prev_sign = signs[i]

    ema_dollar = zeros(n)
    alpha_ema = 2 / (ema_span + 1)
    for i in tqdm(range(n), desc="Calculating EMA of run dollars"):
        if i < warmup_ticks:
            ema_dollar[i] = mean(run_dollar[: i + 1])
        else:
            ema_dollar[i] = (
                alpha_ema * run_dollar[i] + (1 - alpha_ema) * ema_dollar[i - 1]
            )

    thresholds = alpha * ema_dollar

    bar_breaks = run_dollar >= thresholds
    bar_indices = where(bar_breaks)[0]

    bars_list = []
    start_idx = 0
    bar_id = 0

    for end_idx in tqdm(bar_indices, desc="Building bars"):
        bar_df = df[start_idx : end_idx + 1].with_columns(lit(bar_id).alias("bar_id"))
        bars_list.append(bar_df)
        start_idx = end_idx + 1
        bar_id += 1

    final = concat(bars_list) if bars_list else DataFrame()

    if not final.is_empty():
        bars = (
            final.group_by("bar_id", maintain_order=True)
            .agg(
                [
                    col("time").first().alias("start_time"),
                    col("time").last().alias("end_time"),
                    col("price").first().alias("open"),
                    col("price").max().alias("high"),
                    col("price").min().alias("low"),
                    col("price").last().alias("close"),
                    count().alias("n_ticks"),
                    col("qty").sum().alias("base_volume"),
                    (col("price") * col("qty")).sum().alias("quote_volume"),
                    col("id").first().alias("first_trade_id"),
                    col("id").last().alias("last_trade_id"),
                ]
            )
            .sort("bar_id")
        )
    else:
        bars = DataFrame()

    unfinished_bars = df[start_idx:] if start_idx < n else DataFrame()

    return bars, unfinished_bars
