import polars as pl

TIBS_SCHEMA = {
    "start_time": pl.Int64,
    "end_time": pl.Int64,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "n_ticks": pl.Int64,
    "base_volume": pl.Float64,
    "quote_volume": pl.Float64,
    "buy_ticks": pl.Int64,
    "buy_volume": pl.Float64,
    "sell_ticks": pl.Int64,
    "sell_volume": pl.Float64,
    "signed_tick_sum": pl.Int64,
    "signed_volume_sum": pl.Float64,
    "first_trade_id": pl.Int64,
    "last_trade_id": pl.Int64,
}

KLINES_SCHEMA = {
    "open_time": pl.Int64,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "close_time": pl.Int64,
    "quote_asset_volume": pl.Float64,
    "num_trades": pl.Int64,
    "taker_buy_base_asset_volume": pl.Float64,
    "taker_buy_quote_asset_volume": pl.Float64,
    "ignore": pl.Utf8,  # always "0", but kept as string
}

TRADES_SCHEMA = {
    "trade_id": pl.Int64,
    "price": pl.Float64,
    "qty": pl.Float64,
    "quote_qty": pl.Float64,
    "time": pl.Int64,
    "is_buyer_maker": pl.Boolean,
    "is_best_match": pl.Boolean,
}
