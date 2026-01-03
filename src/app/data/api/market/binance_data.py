"""Market data access layer for retrieving historical kline (candlestick) data from Binance.

This module provides a `MarketData` service responsible for fetching, aggregating,
and returning historical OHLCV data from the Binance API as Polars DataFrames.
"""
from datetime import datetime

import polars as pl
from binance.client import Client  # type: ignore[reportMissingTypeStubs]
from binance.exceptions import BinanceAPIException, BinanceRequestException  # type: ignore[reportMissingTypeStubs]
from requests.exceptions import ConnectionError, HTTPError, Timeout
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.app.data.api.domain.value_objects import BinanceKlineTimeframes, TIMEFRAME_TIMESTAMP_MAP
from src.app.data.api.presentation.schemas import BinanceKlineDataframeSchema
from src.app.system.api import get_binance_client
from src.app.system.logs import get_logger


logger = get_logger(__name__)

LIMIT = 1000
RETRYABLE_EXCEPTIONS = (
    BinanceAPIException,
    BinanceRequestException,
    Timeout,
    ConnectionError,
    HTTPError,
)


class MarketData:
    """The class responsible for Binance market data."""
    def __init__(self) -> None:
        """Initialize the MarketData service.

        This constructor instantiates a Binance API client using the application's
        configured client factory. The client is reused across all data-fetching
        operations performed by this service.
        """
        self.client: Client = get_binance_client()  # add client initialization.

    def get_klines(
            self, start_date: datetime, end_date: datetime, symbol: str, timeframe: BinanceKlineTimeframes
    ) -> pl.DataFrame:
        """Fetch historical kline (candlestick) data for a symbol and time range.

        Args:
            start_date (datetime): Inclusive start datetime for the kline data.
            end_date (datetime): Exclusive end datetime for the kline data.
            symbol (str): Trading pair symbol (e.g., "BTCUSDT").
            timeframe (BinanceKlineTimeframes): Kline interval as a `BinanceKlineTimeframes` enum value.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the aggregated kline data. If the input
            range is invalid or no data is available, an empty DataFrame is returned.
        """
        symbol = symbol.upper()
        
        # 1. Form timestamps from start and end_date.
        # 2. Form range.
        # 3. Start to fetch data.
        # 4. Form final dataset.

        # add check for the symbol

        if start_date > end_date:
            logger.warning(f"The start date is later than end date. "
                           f"Start date: {start_date}, End date: {end_date}, Symbol: {symbol}")
            return pl.DataFrame()  # TODO: add custom exception.

        start_timestamp: int = int(start_date.timestamp() * 1000)
        end_timestamp: int = int(end_date.timestamp() * 1000)
        
        # TODO: add check for the earliest record.
        # TODO: add check for the latest record (current date).

        final_df: pl.DataFrame = self._fetch_interval(start_timestamp, end_timestamp, symbol, timeframe)
        
        logger.info(f"Successfully fetched klines, final shape {final_df.shape}")
        return final_df
        
    def _fetch_interval(
            self, start_timestamp: int, end_timestamp: int, symbol: str, timeframe: BinanceKlineTimeframes
    ) -> pl.DataFrame:
        """Fetch kline data over a timestamp interval using pagination.

        Args:
            start_timestamp (int): Start timestamp in milliseconds since epoch.
            end_timestamp (int): End timestamp in milliseconds since epoch.
            symbol (str): Trading pair symbol (e.g., "BTCUSDT").
            timeframe (BinanceKlineTimeframes): Kline interval as a `BinanceKlineTimeframes` enum value.

        Returns:
            pl.DataFrame: A Polars DataFrame constructed from all fetched klines using the
            predefined `BinanceKlineDataframeSchema`. If no rows are collected,
            an empty DataFrame is returned.
        """
        logger.info(
            f"Starting to fetch klines for {symbol=} {timeframe=} "
            f"between {start_timestamp=} and {end_timestamp=}"
        )

        interval_ms: int = TIMEFRAME_TIMESTAMP_MAP[timeframe]
        rows: list[list] = []

        current_start: int = start_timestamp

        while current_start < end_timestamp:
            data: dict | None = self._fetch_klines(symbol, timeframe, current_start)

            if data is None:
                logger.warning(
                    f"No kline data returned for {symbol=} {timeframe=} at {current_start=}"
                )
                break

            rows.extend(data)

            last_open_time: int = data[-1][0]
            next_start: int = last_open_time + interval_ms

            # Safety guard against infinite loops or non-advancing cursors
            if next_start <= current_start:
                logger.error(
                    f"Non-advancing kline cursor detected for {symbol=} {timeframe=}: "
                    f"{current_start=} {next_start=}"
                )
                break

            current_start = next_start

        if not rows:
            logger.info(
                f"No klines collected for {symbol=} {timeframe=} "
                f"between {start_timestamp=} and {end_timestamp=}"
            )
            return pl.DataFrame()

        return pl.DataFrame(rows, schema=BinanceKlineDataframeSchema, orient="row")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        reraise=False
    )
    def _fetch_klines(self, symbol: str, timeframe: BinanceKlineTimeframes, start_timestamp: int) -> dict | None:
        """Fetch a single batch of klines from the Binance API.

        Args:
            symbol (str): Trading pair symbol (e.g., "BTCUSDT").
            timeframe (BinanceKlineTimeframes): Kline interval as a `BinanceKlineTimeframes` enum value.
            start_timestamp (int): Start timestamp in milliseconds since epoch.

        Returns:
            dict | None: A list-like structure containing raw kline records as returned by
            the Binance API, or `None` if the request ultimately fails after
            retries or encounters a non-retryable error.
        """
        try:
            data: dict = self.client.get_klines(
                symbol=symbol,
                interval=timeframe,
                startTime=start_timestamp,
                limit=LIMIT
            )

        except BinanceAPIException as e:
            logger.warning(
                f"Binance API error while fetching klines | symbol={symbol} timeframe={timeframe} | {e}"
            )

        except BinanceRequestException as e:
            logger.warning(
                f"Binance request error while fetching klines | symbol={symbol} timeframe={timeframe} | {e}"
            )

        except (Timeout, ConnectionError, HTTPError) as e:
            logger.warning(
                f"Network error while fetching klines | symbol={symbol} timeframe={timeframe} | {e}"
            )

        except Exception:
            logger.exception(
                f"Unexpected error while fetching klines | symbol={symbol} timeframe={timeframe}", exc_info=True
            )

        else:
            return data

        return None