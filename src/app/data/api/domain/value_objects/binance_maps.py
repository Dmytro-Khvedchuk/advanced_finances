"""Timeframe map for the sequential fetching."""
from src.app.data.api.domain.value_objects.binance_kline_timeframes import BinanceKlineTimeframes


TIMEFRAME_TIMESTAMP_MAP = {
    BinanceKlineTimeframes.ONE_MINUTE: 60_000,
    BinanceKlineTimeframes.THREE_MINUTES: 3 * 60_000,
    BinanceKlineTimeframes.FIVE_MINUTES: 5 * 60_000,
    BinanceKlineTimeframes.FIFTEEN_MINUTES: 15 * 60_000,
    BinanceKlineTimeframes.THIRTY_MINUTES: 30 * 60_000,
    BinanceKlineTimeframes.ONE_HOUR: 60 * 60_000,
    BinanceKlineTimeframes.TWO_HOURS: 2 * 60 * 60_000,
    BinanceKlineTimeframes.FOUR_HOURS: 4 * 60 * 60_000,
    BinanceKlineTimeframes.SIX_HOURS: 6 * 60 * 60_000,
    BinanceKlineTimeframes.EIGHT_HOURS: 8 * 60 * 60_000,
    BinanceKlineTimeframes.TWELVE_HOURS: 12 * 60 * 60_000,
    BinanceKlineTimeframes.ONE_DAY: 24 * 60 * 60_000,
    BinanceKlineTimeframes.THREE_DAYS: 3 * 24 * 60 * 60_000,
    BinanceKlineTimeframes.ONE_WEEK: 7 * 24 * 60 * 60_000,
    BinanceKlineTimeframes.ONE_MONTH: 30 * 24 * 60 * 60_000, 
}