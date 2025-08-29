from numpy import max, mean, min, sum, where
from polars import Boolean, col, DataFrame, Float64, Int64
from tqdm import tqdm
from typing import Any, Dict, List
from utils.global_variables.SCHEMAS import TIBS_SCHEMA


def build_tick_imbalance_bars(
    data: DataFrame,
    *,
    alpha: float = 1.0,
    ema_span: int = 50,
    warmup_ticks: int = 200,
) -> tuple[DataFrame | Any, DataFrame]:
    """
    Build tick imbalance bars from raw data

    :param data: Polars DataFrame with trades data
    :type data: pl.DataFrame
    :param alpha: Scaling factor in the stopping rule threshold.
    :type alpha: float
    :param ema_span: Span for EMA updates of expected ticks per bar and expected imbalance.
        (EMA alpha is computed as 2/(span+1)).
    :type ema_span: int
    :param warmup_ticks: Use the first `warmup_ticks` trades to seed initial expectations.
        If not enough ticks exist, the function degrades gracefully.
    :type warmup_ticks: int
    :returns: tick imbalance bars, unfinished part
    """

    if isinstance(data, DataFrame):
        df = data
    else:
        df = DataFrame(data)

    if df.height == 0:
        return DataFrame(schema=TIBS_SCHEMA), DataFrame()

    df = (
        df.select(
            col("price").cast(Float64),
            col("qty").cast(Float64),
            col("time").cast(Int64),
            col("id").cast(Int64),
            col("isBuyerMaker").cast(Boolean),
        )
        .sort("time")
        .with_columns((~col("isBuyerMaker")).alias("buyer_taker"))
    )

    price = df["price"].to_numpy()
    qty = df["qty"].to_numpy()
    ts = df["time"].to_numpy()
    tid = df["id"].to_numpy()
    sign = where(df["buyer_taker"].to_numpy(), 1, -1)

    n = len(price)
    if n == 0:
        return DataFrame(schema=TIBS_SCHEMA), DataFrame()

    # --- Initialize EMA expectations ---
    w_end = min(warmup_ticks, n)
    theta0 = max(1e-6, abs(mean(sign[:w_end])))
    e_T = max(10.0, w_end / 5.0)
    e_theta = theta0
    ema_alpha = 2.0 / (ema_span + 1.0)

    out_rows: List[Dict[str, Any]] = []

    idx = 0
    pbar = tqdm(total=n, desc="Building tick imbalance bars")

    while idx < n:
        bar_start_idx = idx
        cum_signed_ticks = 0
        cum_signed_volume = 0.0
        threshold = max(alpha * e_T * e_theta, 1.0)

        while idx < n:
            cum_signed_ticks += sign[idx]
            cum_signed_volume += sign[idx] * qty[idx]
            idx += 1
            if abs(cum_signed_ticks) >= threshold:
                break

        i0, i1 = bar_start_idx, idx - 1
        p_slice = price[i0 : i1 + 1]
        q_slice = qty[i0 : i1 + 1]
        t_slice = ts[i0 : i1 + 1]
        id_slice = tid[i0 : i1 + 1]
        s_slice = sign[i0 : i1 + 1]

        n_ticks = i1 - i0 + 1
        base_volume = float(sum(q_slice))
        quote_volume = float(sum(p_slice * q_slice))
        signed_tick_sum = int(sum(s_slice))
        signed_volume_sum = float(sum(q_slice * s_slice))

        buy_mask = s_slice > 0
        sell_mask = ~buy_mask

        out_rows.append(
            {
                "start_time": int(t_slice[0]),
                "end_time": int(t_slice[-1]),
                "open": float(p_slice[0]),
                "high": float(max(p_slice)),
                "low": float(min(p_slice)),
                "close": float(p_slice[-1]),
                "n_ticks": int(n_ticks),
                "base_volume": base_volume,
                "quote_volume": quote_volume,
                "buy_ticks": int(sum(buy_mask)),
                "buy_volume": float(sum(q_slice[buy_mask])),
                "sell_ticks": int(sum(sell_mask)),
                "sell_volume": float(sum(q_slice[sell_mask])),
                "signed_tick_sum": signed_tick_sum,
                "signed_volume_sum": signed_volume_sum,
                "first_trade_id": int(id_slice[0]),
                "last_trade_id": int(id_slice[-1]),
            }
        )

        e_T = (1 - ema_alpha) * e_T + ema_alpha * n_ticks
        bar_theta = max(1e-12, abs(signed_tick_sum) / n_ticks)
        e_theta = (1 - ema_alpha) * e_theta + ema_alpha * bar_theta

        pbar.update(n_ticks)

    pbar.close()

    bars = DataFrame(out_rows)
    if bars.height > 0:
        bars = bars.select(
            col("start_time").cast(Int64),
            col("end_time").cast(Int64),
            col("open").cast(Float64),
            col("high").cast(Float64),
            col("low").cast(Float64),
            col("close").cast(Float64),
            col("n_ticks").cast(Int64),
            col("base_volume").cast(Float64),
            col("quote_volume").cast(Float64),
            col("buy_ticks").cast(Int64),
            col("buy_volume").cast(Float64),
            col("sell_ticks").cast(Int64),
            col("sell_volume").cast(Float64),
            col("signed_tick_sum").cast(Int64),
            col("signed_volume_sum").cast(Float64),
            col("first_trade_id").cast(Int64),
            col("last_trade_id").cast(Int64),
        )

    return bars, DataFrame()
