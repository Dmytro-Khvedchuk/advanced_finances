import polars as pl
import numpy as np
from tqdm import tqdm
from typing import List, Dict, Any
from utils.global_variables.SCHEMAS import TIBS_SCHEMA


def build_tick_imbalance_bars(
    data,
    *,
    alpha: float = 1.0,
    ema_span: int = 50,
    warmup_ticks: int = 200,
) -> tuple[pl.DataFrame | Any, pl.DataFrame]:
    """
    Build tick imbalance bars from raw data
    :param data: raw fetched data from binance, or directly a polars dataframe
    :param alpha: Scaling factor in the stopping rule threshold.
    :param ema_span: Span for EMA updates of expected ticks per bar and expected imbalance.
        (EMA alpha is computed as 2/(span+1)).
    :param warmup_ticks: Use the first `warmup_ticks` trades to seed initial expectations.
        If not enough ticks exist, the function degrades gracefully.
    :return: tick imbalance bars, unfinished part
    """

    if isinstance(data, pl.DataFrame):
        df = data
    else:
        df = pl.DataFrame(data)

    if df.height == 0:
        return pl.DataFrame(schema=TIBS_SCHEMA), pl.DataFrame()

    # Select and prepare columns
    df = (
        df.select(
            pl.col("price").cast(pl.Float64),
            pl.col("qty").cast(pl.Float64),
            pl.col("time").cast(pl.Int64),
            pl.col("id").cast(pl.Int64),
            pl.col("isBuyerMaker").cast(pl.Boolean),
        )
        .sort("time")
        .with_columns((~pl.col("isBuyerMaker")).alias("buyer_taker"))
    )

    price = df["price"].to_numpy()
    qty = df["qty"].to_numpy()
    ts = df["time"].to_numpy()
    tid = df["id"].to_numpy()
    sign = np.where(df["buyer_taker"].to_numpy(), 1, -1)

    n = len(price)
    if n == 0:
        return pl.DataFrame(schema=TIBS_SCHEMA), pl.DataFrame()

    # --- Initialize EMA expectations ---
    w_end = min(warmup_ticks, n)
    theta0 = max(1e-6, abs(np.mean(sign[:w_end])))
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

        # --- Grow the bar ---
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
        base_volume = float(np.sum(q_slice))
        quote_volume = float(np.sum(p_slice * q_slice))
        signed_tick_sum = int(np.sum(s_slice))
        signed_volume_sum = float(np.sum(q_slice * s_slice))

        buy_mask = s_slice > 0
        sell_mask = ~buy_mask

        out_rows.append(
            {
                "start_time": int(t_slice[0]),
                "end_time": int(t_slice[-1]),
                "open": float(p_slice[0]),
                "high": float(np.max(p_slice)),
                "low": float(np.min(p_slice)),
                "close": float(p_slice[-1]),
                "n_ticks": int(n_ticks),
                "base_volume": base_volume,
                "quote_volume": quote_volume,
                "buy_ticks": int(np.sum(buy_mask)),
                "buy_volume": float(np.sum(q_slice[buy_mask])),
                "sell_ticks": int(np.sum(sell_mask)),
                "sell_volume": float(np.sum(q_slice[sell_mask])),
                "signed_tick_sum": signed_tick_sum,
                "signed_volume_sum": signed_volume_sum,
                "first_trade_id": int(id_slice[0]),
                "last_trade_id": int(id_slice[-1]),
            }
        )

        # --- Update EMA ---
        e_T = (1 - ema_alpha) * e_T + ema_alpha * n_ticks
        bar_theta = max(1e-12, abs(signed_tick_sum) / n_ticks)
        e_theta = (1 - ema_alpha) * e_theta + ema_alpha * bar_theta

        pbar.update(n_ticks)

    pbar.close()

    bars = pl.DataFrame(out_rows)
    if bars.height > 0:
        bars = bars.select(
            pl.col("start_time").cast(pl.Int64),
            pl.col("end_time").cast(pl.Int64),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("n_ticks").cast(pl.Int64),
            pl.col("base_volume").cast(pl.Float64),
            pl.col("quote_volume").cast(pl.Float64),
            pl.col("buy_ticks").cast(pl.Int64),
            pl.col("buy_volume").cast(pl.Float64),
            pl.col("sell_ticks").cast(pl.Int64),
            pl.col("sell_volume").cast(pl.Float64),
            pl.col("signed_tick_sum").cast(pl.Int64),
            pl.col("signed_volume_sum").cast(pl.Float64),
            pl.col("first_trade_id").cast(pl.Int64),
            pl.col("last_trade_id").cast(pl.Int64),
        )

    return bars, pl.DataFrame()
