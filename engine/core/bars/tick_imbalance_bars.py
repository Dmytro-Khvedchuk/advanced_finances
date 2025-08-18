import polars as pl
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

    # Nothing to do?
    if df.height == 0:
        return pl.DataFrame(schema=TIBS_SCHEMA), pl.DataFrame()

    # Convert to Python lists for fast iterative bar construction
    price = df["price"].to_list()
    qty = df["qty"].to_list()
    ts = df["time"].to_list()
    tid = df["id"].to_list()
    # Trade sign: +1 for buy-taker, -1 for sell-taker
    sign = [1 if v else -1 for v in df["buyer_taker"].to_list()]

    # --- Initialize expectations (EMA) ---
    # Use a short warmup slice (or all if fewer than warmup_ticks)
    w_end = min(warmup_ticks, len(sign))
    if w_end == 0:
        # Fallback: return a single bar with everything, if caller insists
        # (Or just return empty; here we choose empty since imbalance has no meaning without data)
        return pl.DataFrame(), pl.DataFrame()

    warmup_n = w_end  # ticks used for warmup
    # Absolute mean of sign in warmup as theta_0; clamp away from zero to avoid division by zero thresholds.
    theta0 = max(1e-6, abs(sum(sign[:w_end]) / float(warmup_n)))
    # Seed expected ticks per bar with a sensible guess: use warmup_n / 5 (arbitrary) but >= 10,
    # or use warmup chunks by splitting into ~5 bars: this is heuristic.
    e_T = max(10.0, warmup_n / 5.0)
    e_theta = theta0

    # EMA smoothing factor
    ema_alpha = 2.0 / (ema_span + 1.0)

    # --- Build bars iteratively according to imbalance stopping rule ---
    out_rows: List[Dict[str, Any]] = []

    idx = 0
    n = len(price)

    while idx < n:
        # Start a new bar
        bar_start_idx = idx
        bar_end_idx = idx  # inclusive index as we grow
        cum_signed_ticks = 0
        cum_signed_volume = 0.0

        # Precompute dynamic threshold for this bar using current expectations
        threshold = alpha * e_T * e_theta
        if threshold < 1e-6:
            threshold = 1.0  # minimal sensible threshold

        # Walk forward until we hit the stopping condition or run out of data
        while bar_end_idx < n:
            s = sign[bar_end_idx]
            q = qty[bar_end_idx]
            cum_signed_ticks += s
            cum_signed_volume += s * q

            if abs(cum_signed_ticks) >= threshold:
                # Bar is complete at bar_end_idx
                break

            bar_end_idx += 1

        # Now compute bar aggregates over [bar_start_idx, bar_end_idx]
        i0, i1 = bar_start_idx, min(bar_end_idx, n - 1)
        # Slice views
        p_slice = price[i0 : i1 + 1]
        q_slice = qty[i0 : i1 + 1]
        t_slice = ts[i0 : i1 + 1]
        id_slice = tid[i0 : i1 + 1]
        s_slice = sign[i0 : i1 + 1]

        n_ticks = len(p_slice)
        base_volume = float(sum(q_slice))
        quote_volume = float(sum(pi * qi for pi, qi in zip(p_slice, q_slice)))

        buy_mask = [1 if s_ > 0 else 0 for s_ in s_slice]
        sell_mask = [1 - b for b in buy_mask]

        buy_ticks = sum(buy_mask)
        sell_ticks = sum(sell_mask)
        buy_volume = float(sum(qi for qi, b in zip(q_slice, buy_mask) if b))
        sell_volume = float(sum(qi for qi, sm in zip(q_slice, sell_mask) if sm))

        signed_tick_sum = int(sum(s_slice))
        signed_volume_sum = float(
            sum(qi if s_ > 0 else -qi for qi, s_ in zip(q_slice, s_slice))
        )

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
                "buy_ticks": int(buy_ticks),
                "buy_volume": buy_volume,
                "sell_ticks": int(sell_ticks),
                "sell_volume": sell_volume,
                "signed_tick_sum": int(signed_tick_sum),
                "signed_volume_sum": signed_volume_sum,
                "first_trade_id": int(id_slice[0]),
                "last_trade_id": int(id_slice[-1]),
            }
        )

        # --- Update expectations via EMA using the just-completed (or partial) bar ---
        # E[T] <- EMA of bar sizes (ticks per bar)
        e_T = (1 - ema_alpha) * e_T + ema_alpha * float(n_ticks)
        # theta_hat <- EMA of absolute mean sign per bar
        bar_theta = max(1e-12, abs(signed_tick_sum) / float(n_ticks))
        e_theta = (1 - ema_alpha) * e_theta + ema_alpha * bar_theta

        # Advance index to the next tick after this bar
        idx = i1 + 1

    # Return as Polars DataFrame
    bars = pl.DataFrame(out_rows)
    # Ensure consistent dtypes
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

    unfinished_part = pl.DataFrame()

    return bars, unfinished_part
