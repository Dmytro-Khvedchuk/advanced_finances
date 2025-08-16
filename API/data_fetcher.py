from __future__ import annotations
from typing import Dict, Any, List, Optional
from binance.client import Client
from datetime import datetime


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

    def _to_milliseconds(self, date_str: str | None) -> int | None:
        if date_str is None:
            return None
        # Try ISO format first (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
        try:
            dt = datetime.fromisoformat(date_str)
        except ValueError:
            # Try Binance-style string like "1 Jan, 2025"
            try:
                dt = datetime.strptime(date_str, "%d %b, %Y")
            except ValueError:
                raise ValueError(f"Unsupported date format: {date_str}")
        return int(dt.timestamp() * 1000)

    def fetch_klines(
        self,
        interval: str,
        start_time: str = None,
        end_time: str = None,
        limit: int = 1000,
    ) -> List[List[Any]]:
        start_ms = self._to_milliseconds(start_time)
        end_ms = self._to_milliseconds(end_time)

        return self.client.get_klines(
            symbol=self.symbol,
            interval=interval,
            startTime=start_ms,
            endTime=end_ms,
            limit=limit
        )
