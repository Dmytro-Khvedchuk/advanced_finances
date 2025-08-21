from __future__ import annotations
from typing import Dict, Any, List, Optional
from binance.client import Client
from utils.global_variables.GLOBAL_VARIABLES import SYMBOL, TIMEFRAME
from utils.logger.logger import LoggerWrapper, log_execution

# here should be added try catch blocks with retry function

class FetchData:
    def __init__(self, client: Client, symbol: str = SYMBOL, log_level: int = 10):
        self.logger = LoggerWrapper(name="Fetch Data Module", log_level=log_level)
        self.symbol = symbol.upper()
        self.client = client

    @log_execution
    def fetch_order_book(self, limit: int = 100) -> Dict[str, Any]:
        return self.client.get_order_book(symbol=self.symbol, limit=limit)

    @log_execution
    def fetch_recent_trades(self, limit: int = 500) -> List[Dict[str, Any]]:
        """
        Recent trades (most-recent-first window). Max limit typically 1000.
        Does NOT require API key permission beyond public access.
        """
        return self.client.get_recent_trades(symbol=self.symbol, limit=limit)  # type: ignore[no-any-return]

    @log_execution
    def fetch_historical_trades(
        self, limit: int = 1000, from_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Historical trades (older than the recent trades window).
        Requires API key with 'Enable Reading' permission.
        `from_id` is the tradeId to start from (inclusive).
        """
        return self.client.get_historical_trades(  # type: ignore[no-any-return]
            symbol=self.symbol, limit=limit, fromId=from_id
        )

    @log_execution
    def fetch_klines(self, timeframe: str = TIMEFRAME, limit: int = 1000):
        return self.client.get_klines(
            symbol=self.symbol, interval=timeframe, limit=limit
        )

    @log_execution
    def fetch_historical_klines(
        self,
        timeframe: str = TIMEFRAME,
        start_str: str = None,
        end_str: str = None,
    ):
        return self.client.get_klines(
            symbol=self.symbol, interval=timeframe, startTime=start_str, endTime=end_str
        )
