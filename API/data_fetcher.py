from binance.client import Client
from requests.exceptions import ReadTimeout
from time import sleep
from typing import List, Dict, Any
from utils.global_variables.GLOBAL_VARIABLES import (
    SYMBOL,
    TIMEFRAME,
    MAX_RETRIES,
    RETRY_DELAY,
)
from utils.logger.logger import LoggerWrapper, log_execution


class FetchData:
    def __init__(self, client: Client, symbol: str = SYMBOL, log_level: int = 10):
        self.logger = LoggerWrapper(name="Fetch Data Module", level=log_level)
        self.symbol = symbol.upper()
        self.client = client

    @log_execution
    def fetch_order_book(self, limit: int = 100) -> Dict[str, Any]:
        """
        Fetches order book from API with certain depth

        :param limit: depth of the oreder book
        :type limit: int
        :returns: The dictionary of prices and orders
        """
        return self._fetch_with_retry(
            self.client.get_order_book, symbol=self.symbol, limit=limit
        )

    @log_execution
    def fetch_recent_trades(self, limit: int = 500):
        """
        Fetches most recent "limit" amount of trades

        :param limit: Amount of the most recent trades
        :type limit: int
        :returns: "limit" amount of most recent trades
        """
        return self._fetch_with_retry(
            self.client.get_recent_trades, symbol=self.symbol, limit=limit
        )

    @log_execution
    def fetch_historical_trades(
        self, limit: int = 1000, from_id: int | None = None
    ) -> List[Dict[str, Any]]:
        """
        Fetches "limit" amount of trades starting from id "from_id"

        :param limit: Amount of trades
        :type limit: int
        :param from_id: Starting point of fetching "limit" amount of trades
        :type from_id: int | None
        :returns: "limit" amount of trades starting from "from_id"
        """
        return self._fetch_with_retry(
            self.client.get_historical_trades,
            symbol=self.symbol,
            limit=limit,
            fromId=from_id,
        )

    @log_execution
    def fetch_klines(self, timeframe: str = TIMEFRAME, limit: int = 1000):
        """
        Fetches a "limit" amount of klines data with custom "timeframe"

        :param timeframe: Binance klines timeframe
        :type timeframe: str
        :param limit: amount of klines to be fetched
        :type limit: int
        :returns: Klines data in a raw List[str]
        """
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
        """
        Fetches certain amount of klines from "start_str" to "end_str" with custom "timeframe"

        :param timeframe: Binance klines timeframe
        :type timeframe: str
        :param start_str: Starting date of fetching
        :type start_str: str | None
        :param end_str: Ending date of fetching
        :type end_str: str | None
        :returns: Klines data in a raw List[str]
        """
        return self._fetch_with_retry(
            self.client.get_klines,
            symbol=self.symbol,
            interval=timeframe,
            startTime=start_str,
            endTime=end_str,
        )

    @log_execution
    def _fetch_with_retry(self, func, *args, **kwargs):
        """
        Fetch a batch of trades with retry on ReadTimeout.

        :param func: function that will be executed
        :type func: function
        :param *args: arguments of a function
        :type *args: tuple
        :param **kwargs: keywords arguments of a function
        :type **kwargs: dict[str, Any]
        :returns: Data from function "func"
        """
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
