"""Polars schema definitions for Binance Klines data."""

import polars as pl


BinanceKlineDataframeSchema: dict = {
    "open_time": pl.Int64,                 # Kline open time (ms)
    "open": pl.Float64,                    # Open price
    "high": pl.Float64,                    # High price
    "low": pl.Float64,                     # Low price
    "close": pl.Float64,                   # Close price
    "volume": pl.Float64,                  # Base asset volume
    "close_time": pl.Int64,                # Kline close time (ms)
    "quote_asset_volume": pl.Float64,      # Quote asset volume
    "number_of_trades": pl.Int64,           # Number of trades
    "taker_buy_base_volume": pl.Float64,   # Taker buy base volume
    "taker_buy_quote_volume": pl.Float64,  # Taker buy quote volume
    "ignore": pl.Utf8,                     # Unused field from Binance API
}