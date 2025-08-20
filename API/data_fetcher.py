from __future__ import annotations
from typing import Dict, Any, List, Optional
from binance.client import Client


class FetchData:
    def __init__(self, client: Client, symbol: str):
        self.symbol = symbol.upper()
        self.client = client

    def fetch_order_book(self, limit: int = 100) -> Dict[str, Any]:
        return self.client.get_order_book(symbol=self.symbol, limit=limit)

    def fetch_recent_trades(self, limit: int = 500) -> List[Dict[str, Any]]:
        """
        Recent trades (most-recent-first window). Max limit typically 1000.
        Does NOT require API key permission beyond public access.
        """
        return self.client.get_recent_trades(symbol=self.symbol, limit=limit)  # type: ignore[no-any-return]

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

    def fetch_klines(
        self, interval: str = Client.KLINE_INTERVAL_1MINUTE, limit: int = 1000
    ):
        return self.client.get_klines(
            symbol=self.symbol, interval=interval, limit=limit
        )

    def fetch_historical_klines(
        self,
        interval: str = Client.KLINE_INTERVAL_1MINUTE,
        start_str: str = None,
        end_str: str = None,
    ):
        return self.client.get_klines(
            symbol=self.symbol, interval=interval, startTime=start_str, endTime=end_str
        )
