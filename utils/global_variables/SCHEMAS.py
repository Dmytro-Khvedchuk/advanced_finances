from polars import Boolean, Float64, Int64, Utf8, String

TIBS_SCHEMA = {
    "start_time": Int64,
    "end_time": Int64,
    "open": Float64,
    "high": Float64,
    "low": Float64,
    "close": Float64,
    "n_ticks": Int64,
    "base_volume": Float64,
    "quote_volume": Float64,
    "buy_ticks": Int64,
    "buy_volume": Float64,
    "sell_ticks": Int64,
    "sell_volume": Float64,
    "signed_tick_sum": Int64,
    "signed_volume_sum": Float64,
    "first_trade_id": Int64,
    "last_trade_id": Int64,
}

KLINES_SCHEMA = {
    "open_time": Int64,
    "open": Float64,
    "high": Float64,
    "low": Float64,
    "close": Float64,
    "volume": Float64,
    "close_time": Int64,
    "quote_asset_volume": Float64,
    "num_trades": Int64,
    "taker_buy_base_asset_volume": Float64,
    "taker_buy_quote_asset_volume": Float64,
    "ignore": Utf8,
}

TRADES_SCHEMA = {
    "id": Int64,
    "price": Float64,
    "qty": Float64,
    "quoteQty": Float64,
    "time": Int64,
    "isBuyerMaker": Boolean,
    "isBestMatch": Boolean,
}

TRADE_HISTORY_SCHEMA = {
    "order_id": Int64,
    "symbol": String,
    "pnl": Float64,
    "volume": Float64,
    "direction": String,
    "entry_time": Int64,
    "exit_time": Int64,
    "strategy": String,
    "stop_loss": Float64,
    "break_even": Float64,
    "take_profit": Float64,
    "closed_by": String,
    "commissions": Float64,
}

ORDER_HISTORY_SCHEMA = {
    "order_id": Int64,
    "symbol": String,
    "volume": Float64,
    "direction": String,
    "order_type": String,
    "order_time": Int64,
    "strategy": String,
    "status": String,
    "entry_price": Float64,
    "take_profit": Float64,
    "stop_loss": Float64,
}

POSITIONS_SCHEMA = {
    "order_id": Int64,
    "symbol": String,
    "volume": Float64,
    "direction": String,
    "entry_time": Int64,
    "entry_price": Float64,
    "leverage": Int64,
    "strategy": String,
    "unrealized_pnl": Float64,
    "realized_pnl": Float64,
    "take_profit": Float64,
    "stop_loss": Float64,
}
