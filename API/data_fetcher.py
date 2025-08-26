from __future__ import annotations
from binance.client import Client
from typing import List, Dict, Any
from utils.global_variables.GLOBAL_VARIABLES import (
    SYMBOL,
    TIMEFRAME,
    MAX_RETRIES,
    RETRY_DELAY,
)
from utils.logger.logger import LoggerWrapper, log_execution
from time import sleep
from requests.exceptions import ReadTimeout


class FetchData:
    def __init__(self, client: Client, symbol: str = SYMBOL, log_level: int = 10):
        self.logger = LoggerWrapper(name="Fetch Data Module", level=log_level)
        self.symbol = symbol.upper()
        self.client = client

    @log_execution
    def fetch_order_book(self, limit: int = 100) -> Dict[str, Any]:
        return self._fetch_with_retry(
            self.client.get_order_book, symbol=self.symbol, limit=limit
        )

    @log_execution
    def fetch_recent_trades(self, limit: int = 500):
        return self._fetch_with_retry(
            self.client.get_recent_trades, symbol=self.symbol, limit=limit
        )

    @log_execution
    def fetch_historical_trades(
        self, limit: int = 1000, from_id: int = None
    ) -> List[Dict[str, Any]]:
        """
        Historical trades (older than the recent trades window).
        Requires API key with 'Enable Reading' permission.
        `from_id` is the tradeId to start from (inclusive).
        """
        return self._fetch_with_retry(
            self.client.get_historical_trades,
            symbol=self.symbol,
            limit=limit,
            fromId=from_id,
        )

    @log_execution
    def fetch_klines(self, timeframe: str = TIMEFRAME, limit: int = 1000):
        return self._fetch_with_retry(
            self.client.get_klines, symbol=self.symbol, interval=timeframe, limit=limit
        )

    @log_execution
    def fetch_historical_klines(
        self,
        timeframe: str = TIMEFRAME,
        start_str: str | None = None,
        end_str: str | None = None,
    ):
        return self._fetch_with_retry(
            self.client.get_klines,
            symbol=self.symbol,
            interval=timeframe,
            startTime=start_str,
            endTime=end_str,
        )

    @log_execution
    def _fetch_with_retry(self, func, *args, **kwargs):
        """Fetch a batch of trades with retry on ReadTimeout."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                data = func(*args, **kwargs)
                break
            except ReadTimeout:
                if attempt < MAX_RETRIES:
                    self.logger.warning(
                        f"ReadTimeout, retrying {attempt}/{MAX_RETRIES} after {RETRY_DELAY}s..."
                    )
                    sleep(RETRY_DELAY)
                else:
                    self.logger.error(
                        f"Failed to fetch trades after {MAX_RETRIES} attempts."
                    )
                    raise
        return data
